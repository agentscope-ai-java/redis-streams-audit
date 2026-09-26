"""The audit itself: read PEL state, turn it into findings.

Everything here is read-only. This module never calls ``XCLAIM``,
``XAUTOCLAIM``, ``XACK`` or ``XTRIM`` -- it only reads ``XINFO``/``XPENDING``
and tells you, per finding, exactly which of those commands to run.

## What "dead consumer" actually means here

Redis has no concept of a consumer being alive or dead. A consumer is just
a name that showed up in ``XREADGROUP``; it exists, as far as Redis is
concerned, for as long as ``XGROUP DELCONSUMER`` hasn't been called, even
if the process behind it has been gone for a week. ``XINFO CONSUMERS``
reports ``idle`` (time since that consumer's oldest pending entry was last
delivered) and, since Redis 6.2, ``inactive`` (time since that consumer's
last *any* interaction -- read, ack, or claim). This package's
"dead-consumer candidate" finding is a heuristic: a consumer with pending
entries whose ``inactive`` time exceeds ``dead_consumer_after_ms``. That
consumer might genuinely be dead, or it might just be a slow worker on a
long GC pause -- there is no way to tell from the PEL alone. Size the
threshold for your workload.

## What "orphaned PEL entry" means, and why it needs its own category

``XTRIM``/``MAXLEN`` deletes entries from the stream body. It does **not**
touch the PEL. Confirmed against a real ``redis:7-alpine`` container: after
trimming away every entry in a stream, ``XPENDING`` still reports the full
pending count, for entries whose data no longer exists anywhere. Those
entries can never be reprocessed -- there is nothing left to read. They
also don't just sit there forever: ``XCLAIM``/``XAUTOCLAIM``, on
encountering a pending entry whose stream entry is gone, silently drops it
from the PEL as a side effect (``XAUTOCLAIM``'s reply even lists which ids
it deleted this way). But nothing does that automatically -- until a claim
sweep runs across them, they inflate every PEL-size and PEL-age number this
tool reports, for data you have already lost. That is why this package
flags them as their own kind, separate from ordinary stale entries: the
remedy is not "wait" or "investigate," it is "run a claim sweep to clear
the bookkeeping," and conflating them with recoverable backlog would hide
that the recovery path is different (bookkeeping cleanup, not
reprocessing).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterator, Mapping, Sequence

from .findings import Finding, Remedy, Severity
from .protocol import RedisStreamsClient
from .thresholds import Thresholds

_AGE_BUCKETS: tuple[tuple[str, float], ...] = (
    ("<1s", 1_000),
    ("1s-10s", 10_000),
    ("10s-1m", 60_000),
    ("1m-10m", 600_000),
    ("10m-1h", 3_600_000),
    (">1h", float("inf")),
)

_DISPLAY_CAP = 20
"""How many concrete message ids/consumer names go into a single finding's
``details`` before it switches to "and N more"."""


class StreamNotFoundError(RuntimeError):
    """Raised when a named stream does not exist (``XINFO STREAM`` failed)."""


class GroupNotFoundError(RuntimeError):
    """Raised when a named group does not exist on an existing stream."""


@dataclass(frozen=True, slots=True)
class PendingEntry:
    message_id: str
    consumer: str
    age_ms: int
    times_delivered: int
    orphaned: bool


@dataclass(frozen=True, slots=True)
class AgeStats:
    count: int
    min_ms: int | None
    max_ms: int | None
    mean_ms: float | None
    buckets: dict[str, int]

    def to_dict(self) -> dict[str, Any]:
        return {
            "count": self.count,
            "min_ms": self.min_ms,
            "max_ms": self.max_ms,
            "mean_ms": self.mean_ms,
            "buckets": self.buckets,
        }


@dataclass(frozen=True, slots=True)
class ConsumerStats:
    name: str
    pending: int
    idle_ms: int
    inactive_ms: int | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "pending": self.pending,
            "idle_ms": self.idle_ms,
            "inactive_ms": self.inactive_ms,
        }


@dataclass(slots=True)
class GroupReport:
    stream: str
    group: str
    pending_total: int
    lag: int | None
    consumers: list[ConsumerStats]
    age_stats: AgeStats
    findings: list[Finding] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "stream": self.stream,
            "group": self.group,
            "pending_total": self.pending_total,
            "lag": self.lag,
            "consumers": [c.to_dict() for c in self.consumers],
            "age_stats": self.age_stats.to_dict(),
            "findings": [f.to_dict() for f in self.findings],
        }


@dataclass(slots=True)
class AuditReport:
    groups: list[GroupReport] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    @property
    def findings(self) -> list[Finding]:
        return [f for g in self.groups for f in g.findings]

    @property
    def max_severity(self) -> Severity | None:
        sevs = [f.severity for f in self.findings]
        return max(sevs) if sevs else None

    @property
    def exit_code(self) -> int:
        """0 clean, 1 warnings, 2 critical findings, 3 audit incomplete.

        3 outranks 2: a stream or group that couldn't be read at all means
        the rest of the report is incomplete, which is worse than a report
        that is complete and merely alarming.
        """
        if self.errors:
            return 3
        sev = self.max_severity
        if sev is None:
            return 0
        if sev is Severity.CRITICAL:
            return 2
        if sev is Severity.WARNING:
            return 1
        return 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "groups": [g.to_dict() for g in self.groups],
            "errors": self.errors,
            "exit_code": self.exit_code,
        }


def _parse_id(message_id: str) -> tuple[int, int]:
    ms_str, _, seq_str = message_id.partition("-")
    return int(ms_str), int(seq_str or 0)


def _first_entry_id(stream_info: Mapping[str, Any]) -> tuple[int, int] | None:
    if not stream_info.get("length"):
        return None
    first_entry = stream_info.get("first-entry")
    if not first_entry:
        return None
    return _parse_id(first_entry[0])


def _bucket_for(age_ms: int) -> str:
    for label, ceiling in _AGE_BUCKETS:
        if age_ms < ceiling:
            return label
    return _AGE_BUCKETS[-1][0]  # pragma: no cover - unreachable, last is inf


def _iter_pending(
    client: RedisStreamsClient,
    stream: str,
    group: str,
    *,
    page_size: int = 1000,
) -> Iterator[Mapping[str, Any]]:
    """Page through the full extended PEL for one group.

    ``XPENDING key group start end count`` has no native cursor, but a
    Redis stream id range accepts an exclusive lower bound written as
    ``(id``, so re-issuing the call with ``(<last id seen>`` as ``min``
    walks the whole PEL in id order without ever re-reading an entry.
    """

    start = "-"
    while True:
        page = client.xpending_range(stream, group, min=start, max="+", count=page_size)
        if not page:
            return
        for entry in page:
            yield entry
        if len(page) < page_size:
            return
        start = f"({page[-1]['message_id']}"


def _capped(items: Sequence[str]) -> dict[str, Any]:
    shown = list(items[:_DISPLAY_CAP])
    out: dict[str, Any] = {"total": len(items), "shown": shown}
    if len(items) > _DISPLAY_CAP:
        out["more"] = len(items) - _DISPLAY_CAP
    return out


def audit_group(
    client: RedisStreamsClient,
    stream: str,
    group: str,
    *,
    thresholds: Thresholds | None = None,
) -> GroupReport:
    """Audit one consumer group on one stream. Read-only."""

    thresholds = thresholds or Thresholds()

    try:
        stream_info = client.xinfo_stream(stream)
    except Exception as exc:  # noqa: BLE001 - client-specific error types vary
        raise StreamNotFoundError(f"stream {stream!r} not found: {exc}") from exc

    groups_by_name = {g["name"]: g for g in client.xinfo_groups(stream)}
    if group not in groups_by_name:
        raise GroupNotFoundError(f"group {group!r} not found on stream {stream!r}")
    ginfo = groups_by_name[group]

    consumers = [
        ConsumerStats(
            name=c["name"],
            pending=c["pending"],
            idle_ms=c["idle"],
            inactive_ms=c.get("inactive"),
        )
        for c in client.xinfo_consumers(stream, group)
    ]
    consumers_by_name = {c.name: c for c in consumers}

    first_id = _first_entry_id(stream_info)

    entries: list[PendingEntry] = []
    for raw in _iter_pending(client, stream, group):
        message_id = raw["message_id"]
        orphaned = first_id is None or _parse_id(message_id) < first_id
        entries.append(
            PendingEntry(
                message_id=message_id,
                consumer=raw["consumer"],
                age_ms=raw["time_since_delivered"],
                times_delivered=raw["times_delivered"],
                orphaned=orphaned,
            )
        )

    pending_total = ginfo.get("pending", len(entries))
    findings: list[Finding] = []

    # -- age distribution (always reported, not itself a finding) --------
    buckets = {label: 0 for label, _ in _AGE_BUCKETS}
    for e in entries:
        buckets[_bucket_for(e.age_ms)] += 1
    ages = [e.age_ms for e in entries]
    age_stats = AgeStats(
        count=len(entries),
        min_ms=min(ages) if ages else None,
        max_ms=max(ages) if ages else None,
        mean_ms=(sum(ages) / len(ages)) if ages else None,
        buckets=buckets,
    )

    # -- PEL size -----------------------------------------------------------
    if pending_total >= thresholds.pel_size_critical:
        findings.append(
            Finding(
                stream=stream,
                group=group,
                kind="pel-growth",
                severity=Severity.CRITICAL,
                message=(
                    f"{pending_total} entries pending in group {group!r} -- at or "
                    f"above the critical threshold ({thresholds.pel_size_critical})."
                ),
                remedy=Remedy(
                    command=f"XAUTOCLAIM {stream} {group} <recovery-consumer> 0 0 COUNT 100",
                    explanation=(
                        "A PEL this large rarely drains on its own. Sweep it with "
                        "XAUTOCLAIM (min-idle-time 0 reclaims everything regardless "
                        "of age) and reprocess or dead-letter what comes back, in "
                        "batches, until XPENDING's summary count returns to normal."
                    ),
                ),
                details={"pending_total": pending_total},
            )
        )
    elif pending_total >= thresholds.pel_size_warn:
        findings.append(
            Finding(
                stream=stream,
                group=group,
                kind="pel-growth",
                severity=Severity.WARNING,
                message=(
                    f"{pending_total} entries pending in group {group!r} -- at or "
                    f"above the warning threshold ({thresholds.pel_size_warn})."
                ),
                remedy=Remedy(
                    command=f"XPENDING {stream} {group}",
                    explanation=(
                        "Not yet critical, but growing PELs are cheap to catch "
                        "early and expensive once memory or lag alerts fire. Check "
                        "whether consumers are keeping up before this compounds."
                    ),
                ),
                details={"pending_total": pending_total},
            )
        )

    # -- orphaned (trimmed-away) entries -------------------------------------
    orphaned_ids = [e.message_id for e in entries if e.orphaned]
    if orphaned_ids:
        findings.append(
            Finding(
                stream=stream,
                group=group,
                kind="orphaned-pel-entry",
                severity=Severity.CRITICAL,
                message=(
                    f"{len(orphaned_ids)} pending entries in group {group!r} point "
                    "to stream ids that no longer exist -- they were removed by "
                    "XTRIM/MAXLEN (or XDEL) while still unacknowledged. XTRIM does "
                    "not clean the PEL; these will stay pending, with their data "
                    "unrecoverable, until something claims or acks them."
                ),
                remedy=Remedy(
                    command=f"XAUTOCLAIM {stream} {group} <recovery-consumer> 0 0 COUNT 100",
                    explanation=(
                        "XAUTOCLAIM (and XCLAIM) silently drop a pending entry from "
                        "the PEL when its underlying stream entry is gone -- this is "
                        "the only way to clear these, since the data cannot be "
                        "reprocessed. Redis 7+ reports which ids it deleted this way "
                        "in XAUTOCLAIM's third reply element."
                    ),
                ),
                details=_capped(orphaned_ids),
            )
        )

    # -- poison-message candidates -------------------------------------------
    poison = [
        e for e in entries if not e.orphaned and e.times_delivered > thresholds.poison_delivery_count
    ]
    if poison:
        poison_ids = [e.message_id for e in poison]
        max_deliveries = max(e.times_delivered for e in poison)
        findings.append(
            Finding(
                stream=stream,
                group=group,
                kind="poison-candidate",
                severity=Severity.CRITICAL,
                message=(
                    f"{len(poison)} pending entries in group {group!r} have been "
                    f"delivered more than {thresholds.poison_delivery_count} times "
                    f"(max seen: {max_deliveries}) without being acknowledged -- "
                    "they fail every time they're retried."
                ),
                remedy=Remedy(
                    command=(
                        f"XCLAIM {stream} {group} <dlq-mover> 0 <message-id> ; "
                        f"XADD {stream}:dlq * ... ; XACK {stream} {group} <message-id>"
                    ),
                    explanation=(
                        "Without a dead-letter step these are retried forever by "
                        "XAUTOCLAIM/XCLAIM, since delivery count never caps anything "
                        "on its own. Claim each one, copy it to a dead-letter stream "
                        "for inspection, then ack the original so it leaves this PEL."
                    ),
                ),
                details=_capped(poison_ids),
            )
        )

    # -- stale (old, not-yet-poison, not-orphaned) entries -------------------
    poison_ids_set = {e.message_id for e in poison}
    stale = [
        e
        for e in entries
        if not e.orphaned
        and e.message_id not in poison_ids_set
        and e.age_ms >= thresholds.stale_after_ms
    ]
    if stale:
        stale_ids = [e.message_id for e in stale]
        oldest = max(e.age_ms for e in stale)
        findings.append(
            Finding(
                stream=stream,
                group=group,
                kind="stale-pending-entry",
                severity=Severity.WARNING,
                message=(
                    f"{len(stale)} pending entries in group {group!r} have been "
                    f"idle at least {thresholds.stale_after_ms} ms (oldest: "
                    f"{oldest} ms) without being acknowledged."
                ),
                remedy=Remedy(
                    command=f"XPENDING {stream} {group} - + 10",
                    explanation=(
                        "Confirm whether the owning consumer is still alive and "
                        "working through a slow backlog, or has crashed and needs "
                        "XAUTOCLAIM. Age alone doesn't distinguish the two."
                    ),
                ),
                details=_capped(stale_ids),
            )
        )

    # -- dead-consumer candidates --------------------------------------------
    for c in consumers:
        if c.pending == 0:
            continue
        if c.inactive_ms is None or c.inactive_ms < thresholds.dead_consumer_after_ms:
            continue
        findings.append(
            Finding(
                stream=stream,
                group=group,
                consumer=c.name,
                kind="dead-consumer-candidate",
                severity=Severity.WARNING,
                message=(
                    f"Consumer {c.name!r} owns {c.pending} pending entries and has "
                    f"had no read/ack/claim activity for {c.inactive_ms} ms (at or "
                    f"above {thresholds.dead_consumer_after_ms} ms) -- a likely-dead "
                    "consumer, not just a slow one."
                ),
                remedy=Remedy(
                    command=(
                        f"XAUTOCLAIM {stream} {group} <recovery-consumer> "
                        f"{thresholds.dead_consumer_after_ms} 0 COUNT 100"
                    ),
                    explanation=(
                        "Redis never expires a consumer or notices it died -- "
                        "XGROUP DELCONSUMER and reassignment are the only ways its "
                        "entries move. XAUTOCLAIM with a matching min-idle-time "
                        "reassigns everything this idle in one sweep, not just this "
                        "consumer's share."
                    ),
                ),
                details={"pending": c.pending, "inactive_ms": c.inactive_ms},
            )
        )

    # -- group lag ------------------------------------------------------------
    lag = ginfo.get("lag")
    if lag is None:
        findings.append(
            Finding(
                stream=stream,
                group=group,
                kind="lag-unavailable",
                severity=Severity.INFO,
                message=(
                    f"Group {group!r} reports lag=None -- Redis can't compute exact "
                    "lag, typically after XGROUP SETID or XSETID moved the group's "
                    "last-delivered-id somewhere entries-read can't track, or on a "
                    "Redis version older than 7.0."
                ),
                remedy=Remedy(
                    command=f"XINFO STREAM {stream} ; XINFO GROUPS {stream}",
                    explanation=(
                        "Compare last-delivered-id against the stream's "
                        "last-generated-id by timestamp to estimate how far behind "
                        "the group is, since an exact entry count isn't available."
                    ),
                ),
            )
        )
    elif lag >= thresholds.lag_critical:
        findings.append(
            Finding(
                stream=stream,
                group=group,
                kind="group-lag",
                severity=Severity.CRITICAL,
                message=(
                    f"Group {group!r} lag is {lag} entries -- at or above the "
                    f"critical threshold ({thresholds.lag_critical}). Consumers are "
                    "far behind the stream tail."
                ),
                remedy=Remedy(
                    command=f"XINFO CONSUMERS {stream} {group}",
                    explanation=(
                        "Check whether consumers are running at all and whether "
                        "throughput matches ingestion rate; lag this large usually "
                        "means a stalled or under-scaled consumer group, not a "
                        "momentary blip."
                    ),
                ),
                details={"lag": lag},
            )
        )
    elif lag >= thresholds.lag_warn:
        findings.append(
            Finding(
                stream=stream,
                group=group,
                kind="group-lag",
                severity=Severity.WARNING,
                message=(
                    f"Group {group!r} lag is {lag} entries -- at or above the "
                    f"warning threshold ({thresholds.lag_warn})."
                ),
                details={"lag": lag},
            )
        )

    return GroupReport(
        stream=stream,
        group=group,
        pending_total=pending_total,
        lag=lag,
        consumers=consumers,
        age_stats=age_stats,
        findings=findings,
    )


def audit(
    client: RedisStreamsClient,
    streams: Sequence[str],
    *,
    groups: Mapping[str, Sequence[str]] | None = None,
    thresholds: Thresholds | None = None,
) -> AuditReport:
    """Audit every named stream.

    ``streams`` is explicit and required -- this package never scans the
    keyspace to "discover" streams for you (the same reasoning as ``KEYS``
    being a footgun applies to any full-keyspace walk; naming your streams
    is one line and avoids it entirely). For each stream, ``groups`` says
    which consumer groups to check; when a stream is omitted from
    ``groups`` (or ``groups`` is ``None``), every group ``XINFO GROUPS``
    reports for that stream is audited.
    """

    thresholds = thresholds or Thresholds()
    report = AuditReport()

    for stream in streams:
        try:
            group_names = list((groups or {}).get(stream) or [])
            if not group_names:
                group_infos = client.xinfo_groups(stream)
                group_names = [g["name"] for g in group_infos]
        except Exception as exc:  # noqa: BLE001
            report.errors.append(f"stream {stream!r}: could not list groups: {exc}")
            continue

        if not group_names:
            report.errors.append(f"stream {stream!r}: no consumer groups found")
            continue

        for group in group_names:
            try:
                report.groups.append(
                    audit_group(client, stream, group, thresholds=thresholds)
                )
            except (StreamNotFoundError, GroupNotFoundError) as exc:
                report.errors.append(str(exc))

    return report
