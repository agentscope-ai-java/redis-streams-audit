"""Fast, deterministic unit tests for every branch in audit.py, using
FakeStreamsClient. Real-server confirmation lives in test_integration.py."""

from __future__ import annotations

from redis_streams_audit import GroupNotFoundError, Severity, StreamNotFoundError, Thresholds, audit, audit_group
from redis_streams_audit.audit import AgeStats, GroupReport

from .fake_client import FakeStreamsClient


def _kinds(report: GroupReport) -> set[str]:
    return {f.kind for f in report.findings}


def test_clean_group_has_no_findings() -> None:
    c = FakeStreamsClient()
    c.add_stream("s", length=5, first_entry_id="100-0")
    c.add_group("s", "g", lag=0)
    c.add_consumer("s", "g", "c1", pending=1, idle=10, inactive=10)
    c.add_pending("s", "g", message_id="100-0", consumer="c1", age_ms=10, times_delivered=1)

    report = audit_group(c, "s", "g")
    assert report.findings == []
    assert report.pending_total == 1
    assert report.age_stats.count == 1
    assert report.age_stats.buckets["<1s"] == 1


def test_orphaned_entry_detected_when_id_precedes_first_entry() -> None:
    c = FakeStreamsClient()
    # first entry in the stream is now 500-0 -- anything pending below that
    # id was trimmed away.
    c.add_stream("s", length=1, first_entry_id="500-0")
    c.add_group("s", "g")
    c.add_consumer("s", "g", "c1", pending=2, idle=10, inactive=10)
    c.add_pending("s", "g", message_id="100-0", consumer="c1", age_ms=999_999, times_delivered=1)
    c.add_pending("s", "g", message_id="500-0", consumer="c1", age_ms=10, times_delivered=1)

    report = audit_group(c, "s", "g")
    assert "orphaned-pel-entry" in _kinds(report)
    finding = next(f for f in report.findings if f.kind == "orphaned-pel-entry")
    assert finding.severity is Severity.CRITICAL
    assert finding.details["shown"] == ["100-0"]
    assert finding.details["total"] == 1


def test_all_pending_orphaned_when_stream_is_empty() -> None:
    # This is the trim-to-zero case: XLEN 0, but PEL still non-empty.
    c = FakeStreamsClient()
    c.add_stream("s", length=0, first_entry_id=None)
    c.add_group("s", "g")
    c.add_consumer("s", "g", "c1", pending=1, idle=10, inactive=10)
    c.add_pending("s", "g", message_id="100-0", consumer="c1", age_ms=10, times_delivered=1)

    report = audit_group(c, "s", "g")
    finding = next(f for f in report.findings if f.kind == "orphaned-pel-entry")
    assert finding.details["total"] == 1


def test_poison_candidate_over_threshold() -> None:
    c = FakeStreamsClient()
    c.add_stream("s", length=1, first_entry_id="100-0")
    c.add_group("s", "g")
    c.add_consumer("s", "g", "c1", pending=1, idle=10, inactive=10)
    c.add_pending("s", "g", message_id="100-0", consumer="c1", age_ms=10, times_delivered=6)

    report = audit_group(c, "s", "g", thresholds=Thresholds(poison_delivery_count=5))
    finding = next(f for f in report.findings if f.kind == "poison-candidate")
    assert finding.severity is Severity.CRITICAL
    assert "100-0" in finding.details["shown"]


def test_poison_takes_precedence_over_stale_for_the_same_entry() -> None:
    c = FakeStreamsClient()
    c.add_stream("s", length=1, first_entry_id="100-0")
    c.add_group("s", "g")
    c.add_consumer("s", "g", "c1", pending=1, idle=10, inactive=10)
    c.add_pending("s", "g", message_id="100-0", consumer="c1", age_ms=999_999, times_delivered=6)

    report = audit_group(
        c, "s", "g", thresholds=Thresholds(poison_delivery_count=5, stale_after_ms=1000)
    )
    kinds = _kinds(report)
    assert "poison-candidate" in kinds
    assert "stale-pending-entry" not in kinds


def test_stale_entry_below_poison_threshold() -> None:
    c = FakeStreamsClient()
    c.add_stream("s", length=1, first_entry_id="100-0")
    c.add_group("s", "g")
    c.add_consumer("s", "g", "c1", pending=1, idle=10, inactive=10)
    c.add_pending("s", "g", message_id="100-0", consumer="c1", age_ms=120_000, times_delivered=1)

    report = audit_group(c, "s", "g", thresholds=Thresholds(stale_after_ms=60_000))
    finding = next(f for f in report.findings if f.kind == "stale-pending-entry")
    assert finding.severity is Severity.WARNING


def test_dead_consumer_candidate() -> None:
    c = FakeStreamsClient()
    c.add_stream("s", length=1, first_entry_id="100-0")
    c.add_group("s", "g")
    c.add_consumer("s", "g", "ghost", pending=3, idle=500_000, inactive=500_000)
    c.add_pending("s", "g", message_id="100-0", consumer="ghost", age_ms=500_000, times_delivered=1)

    report = audit_group(c, "s", "g", thresholds=Thresholds(dead_consumer_after_ms=300_000))
    finding = next(f for f in report.findings if f.kind == "dead-consumer-candidate")
    assert finding.consumer == "ghost"
    assert finding.severity is Severity.WARNING


def test_consumer_with_no_pending_is_never_flagged_dead() -> None:
    c = FakeStreamsClient()
    c.add_stream("s", length=1, first_entry_id="100-0")
    c.add_group("s", "g")
    c.add_consumer("s", "g", "idle-but-caught-up", pending=0, idle=999_999, inactive=999_999)

    report = audit_group(c, "s", "g", thresholds=Thresholds(dead_consumer_after_ms=1000))
    assert "dead-consumer-candidate" not in _kinds(report)


def test_lag_none_is_info_not_warning() -> None:
    c = FakeStreamsClient()
    c.add_stream("s", length=1, first_entry_id="100-0")
    c.add_group("s", "g", lag=None)

    report = audit_group(c, "s", "g")
    finding = next(f for f in report.findings if f.kind == "lag-unavailable")
    assert finding.severity is Severity.INFO


def test_lag_warning_and_critical_thresholds() -> None:
    c = FakeStreamsClient()
    c.add_stream("s", length=1, first_entry_id="100-0")
    c.add_group("s", "g", lag=5_000)
    warn_report = audit_group(c, "s", "g", thresholds=Thresholds(lag_warn=1000, lag_critical=10_000))
    assert any(f.kind == "group-lag" and f.severity is Severity.WARNING for f in warn_report.findings)

    c.streams["s"]["groups"]["g"]["lag"] = 50_000
    crit_report = audit_group(c, "s", "g", thresholds=Thresholds(lag_warn=1000, lag_critical=10_000))
    assert any(f.kind == "group-lag" and f.severity is Severity.CRITICAL for f in crit_report.findings)


def test_pel_size_thresholds() -> None:
    # Kept well under _iter_pending's page size (1000) -- this is a
    # threshold-logic test, not a pagination test; FakeStreamsClient's
    # xpending_range doesn't implement real id ordering, only enough
    # min-exclusion to avoid looping when a page is genuinely full.
    c = FakeStreamsClient()
    c.add_stream("s", length=100, first_entry_id="1-0")
    c.add_group("s", "g")
    c.add_consumer("s", "g", "c1", pending=50, idle=10, inactive=10)
    for i in range(50):
        c.add_pending("s", "g", message_id=f"{i + 1}-0", consumer="c1", age_ms=10, times_delivered=1)

    report = audit_group(c, "s", "g", thresholds=Thresholds(pel_size_warn=10, pel_size_critical=1000))
    assert any(f.kind == "pel-growth" and f.severity is Severity.WARNING for f in report.findings)


def test_audit_report_exit_codes() -> None:
    from redis_streams_audit.audit import AuditReport
    from redis_streams_audit.findings import Finding, Remedy

    clean = AuditReport()
    assert clean.exit_code == 0

    warn = AuditReport(
        groups=[
            GroupReport(
                stream="s",
                group="g",
                pending_total=0,
                lag=0,
                consumers=[],
                age_stats=AgeStats(count=0, min_ms=None, max_ms=None, mean_ms=None, buckets={}),
                findings=[
                    Finding(
                        stream="s",
                        group="g",
                        kind="stale-pending-entry",
                        severity=Severity.WARNING,
                        message="x",
                    )
                ],
            )
        ]
    )
    assert warn.exit_code == 1

    warn.groups[0].findings.append(
        Finding(stream="s", group="g", kind="poison-candidate", severity=Severity.CRITICAL, message="y")
    )
    assert warn.exit_code == 2

    warn.errors.append("stream 'missing': not found")
    assert warn.exit_code == 3


def test_audit_top_level_discovers_groups_and_records_errors() -> None:
    c = FakeStreamsClient()
    c.add_stream("s", length=1, first_entry_id="100-0")
    c.add_group("s", "g1")
    c.add_group("s", "g2")
    c.add_consumer("s", "g1", "c1", pending=0, idle=0, inactive=0)
    c.add_consumer("s", "g2", "c1", pending=0, idle=0, inactive=0)

    report = audit(c, ["s", "missing-stream"])
    audited = {(g.stream, g.group) for g in report.groups}
    assert audited == {("s", "g1"), ("s", "g2")}
    assert any("missing-stream" in e for e in report.errors)


def test_audit_group_raises_for_missing_group() -> None:
    c = FakeStreamsClient()
    c.add_stream("s", length=1, first_entry_id="100-0")
    c.add_group("s", "g")

    try:
        audit_group(c, "s", "does-not-exist")
    except GroupNotFoundError:
        pass
    else:
        raise AssertionError("expected GroupNotFoundError")


def test_audit_group_raises_for_missing_stream() -> None:
    c = FakeStreamsClient()

    try:
        audit_group(c, "does-not-exist", "g")
    except StreamNotFoundError:
        pass
    else:
        raise AssertionError("expected StreamNotFoundError")
