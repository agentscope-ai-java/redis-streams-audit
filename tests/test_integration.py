"""Integration tests against a real ``redis:7-alpine`` container.

These reproduce, and assert on, the exact behaviours the README's "What we
found" section describes -- most importantly that ``XTRIM`` does **not**
remove PEL entries for the data it deletes. Nothing here is inferred from
documentation; every assertion was checked interactively against a live
container before being written down as a test. See ``conftest.py`` for how
the server is found (``REDIS_STREAMS_AUDIT_TEST_HOST``/``_PORT``, default
``localhost:16379``) and how tests skip loudly, not silently, without one.
"""

from __future__ import annotations

import time

from redis_streams_audit import Severity, Thresholds, audit_group
from redis_streams_audit.audit import _iter_pending
from redis_streams_audit.resp import MinimalRedisClient


def test_crashed_consumer_leaves_entries_pending(real_redis) -> None:
    real_redis.xadd("orders", {"id": "1"})
    real_redis.xadd("orders", {"id": "2"})
    real_redis.xgroup_create("orders", "workers", id="0")

    # A consumer reads but crashes before XACK -- exactly what a container
    # restart mid-processing looks like from Redis's side.
    real_redis.xreadgroup("workers", "doomed-worker", {"orders": ">"}, count=2)

    report = audit_group(real_redis, "orders", "workers")
    assert report.pending_total == 2
    assert report.age_stats.count == 2
    # Not yet stale or dead by default thresholds -- just present.
    assert report.consumers[0].pending == 2


def test_xtrim_does_not_clear_the_pel(real_redis) -> None:
    """The core, genuinely surprising finding: XTRIM deletes stream data,
    not PEL bookkeeping. Confirmed live: after trimming a stream to zero
    entries, XPENDING still reports every entry that was pending before
    the trim, XRANGE returns nothing for their ids, and only XCLAIM/
    XAUTOCLAIM will actually clear them (dropping them silently, since
    there is nothing left to claim)."""

    real_redis.xadd("events", {"k": "v1"})
    real_redis.xadd("events", {"k": "v2"})
    real_redis.xadd("events", {"k": "v3"})
    real_redis.xgroup_create("events", "g", id="0")
    real_redis.xreadgroup("g", "c1", {"events": ">"}, count=3)

    assert real_redis.xlen("events") == 3
    real_redis.xtrim("events", maxlen=0, approximate=False)
    assert real_redis.xlen("events") == 0

    # The stream data is genuinely gone.
    assert real_redis.xrange("events", "-", "+") == []
    # But the PEL still lists all three -- this is the surprising part.
    pending = real_redis.xpending("events", "g")
    assert pending["pending"] == 3

    report = audit_group(real_redis, "events", "g")
    orphaned = next(f for f in report.findings if f.kind == "orphaned-pel-entry")
    assert orphaned.severity is Severity.CRITICAL
    assert orphaned.details["total"] == 3

    # The remedy this package recommends actually works: XAUTOCLAIM purges
    # dangling PEL entries pointing at trimmed-away data as a side effect.
    cursor, claimed, deleted = real_redis.xautoclaim("events", "g", "janitor", 0, "0-0")
    assert claimed == []  # nothing to hand back -- there's no data left
    assert len(deleted) == 3  # but Redis 7+ reports purging all three
    assert real_redis.xpending("events", "g")["pending"] == 0


def test_partial_trim_orphans_only_the_removed_entries(real_redis) -> None:
    """A partial XTRIM (not to zero) orphans only the entries whose ids
    fall before the new first entry -- the rest of the PEL is unaffected
    and must not be flagged."""

    for i in range(5):
        real_redis.xadd("partial", {"i": str(i)})
    real_redis.xgroup_create("partial", "g", id="0")
    real_redis.xreadgroup("g", "c1", {"partial": ">"}, count=5)

    real_redis.xtrim("partial", maxlen=2, approximate=False)  # keeps exactly the newest 2

    report = audit_group(real_redis, "partial", "g")
    orphaned = next(f for f in report.findings if f.kind == "orphaned-pel-entry")
    assert orphaned.details["total"] == 3
    assert report.pending_total == 5  # the PEL itself is still full size


def test_poison_message_flagged_after_repeated_redelivery(real_redis) -> None:
    real_redis.xadd("jobs", {"payload": "bad"})
    real_redis.xgroup_create("jobs", "g", id="0")
    result = real_redis.xreadgroup("g", "worker-1", {"jobs": ">"}, count=1)
    message_id = result[0][1][0][0]

    # Simulate repeated failed processing: claim it back to a consumer
    # each time, which is what a retry loop around XCLAIM looks like.
    for _ in range(4):
        real_redis.xclaim("jobs", "g", "worker-1", 0, [message_id])

    report = audit_group(real_redis, "jobs", "g", thresholds=Thresholds(poison_delivery_count=3))
    poison = next(f for f in report.findings if f.kind == "poison-candidate")
    assert message_id in poison.details["shown"]


def test_dead_consumer_candidate_after_real_inactivity(real_redis) -> None:
    real_redis.xadd("tasks", {"k": "v"})
    real_redis.xgroup_create("tasks", "g", id="0")
    real_redis.xreadgroup("g", "vanished-worker", {"tasks": ">"}, count=1)

    time.sleep(0.3)

    report = audit_group(real_redis, "tasks", "g", thresholds=Thresholds(dead_consumer_after_ms=200))
    finding = next(f for f in report.findings if f.kind == "dead-consumer-candidate")
    assert finding.consumer == "vanished-worker"


def test_group_lag_reflects_undelivered_backlog(real_redis) -> None:
    for i in range(10):
        real_redis.xadd("backlog", {"i": str(i)})
    real_redis.xgroup_create("backlog", "g", id="0")
    # Only deliver 3 of the 10 -- the rest is real, measurable lag.
    real_redis.xreadgroup("g", "c1", {"backlog": ">"}, count=3)

    ginfo = real_redis.xinfo_groups("backlog")[0]
    assert ginfo["lag"] == 7

    report = audit_group(real_redis, "backlog", "g", thresholds=Thresholds(lag_warn=5, lag_critical=100))
    lag_finding = next(f for f in report.findings if f.kind == "group-lag")
    assert lag_finding.severity is Severity.WARNING
    assert report.lag == 7


def test_iter_pending_pages_without_loss_or_duplication(real_redis) -> None:
    """_iter_pending's exclusive-id cursor ("(<id>") must walk the real PEL
    exactly once per entry, in real Redis id order, even when the PEL is
    bigger than one page."""

    for i in range(23):
        real_redis.xadd("paged", {"i": str(i)})
    real_redis.xgroup_create("paged", "g", id="0")
    real_redis.xreadgroup("g", "c1", {"paged": ">"}, count=23)

    ids = [e["message_id"] for e in _iter_pending(real_redis, "paged", "g", page_size=5)]
    assert len(ids) == 23
    assert len(set(ids)) == 23  # no duplicates
    assert ids == sorted(ids, key=lambda i: tuple(int(p) for p in i.split("-")))


def test_resp_client_matches_redis_py_shapes(real_redis, resp_client: MinimalRedisClient) -> None:
    """Our zero-dependency RESP client, run against the same live server
    real_redis just populated, must parse XINFO/XPENDING into the same
    shapes redis-py does -- structurally, not by re-running the clock."""

    real_redis.xadd("parity", {"k": "v1"})
    real_redis.xadd("parity", {"k": "v2"})
    real_redis.xgroup_create("parity", "g", id="0")
    real_redis.xreadgroup("g", "c1", {"parity": ">"}, count=2)

    py_stream = real_redis.xinfo_stream("parity")
    resp_stream = resp_client.xinfo_stream("parity")
    assert resp_stream["length"] == py_stream["length"]
    assert resp_stream["first-entry"][0] == py_stream["first-entry"][0]
    assert resp_stream["first-entry"][1] == py_stream["first-entry"][1]

    py_groups = real_redis.xinfo_groups("parity")
    resp_groups = resp_client.xinfo_groups("parity")
    assert [g["name"] for g in resp_groups] == [g["name"] for g in py_groups]
    assert [g["pending"] for g in resp_groups] == [g["pending"] for g in py_groups]

    py_consumers = real_redis.xinfo_consumers("parity", "g")
    resp_consumers = resp_client.xinfo_consumers("parity", "g")
    assert [c["name"] for c in resp_consumers] == [c["name"] for c in py_consumers]
    assert [c["pending"] for c in resp_consumers] == [c["pending"] for c in py_consumers]

    py_pending = real_redis.xpending("parity", "g")
    resp_pending = resp_client.xpending("parity", "g")
    assert resp_pending["pending"] == py_pending["pending"]

    py_range = real_redis.xpending_range("parity", "g", min="-", max="+", count=10)
    resp_range = resp_client.xpending_range("parity", "g", min="-", max="+", count=10)
    assert [r["message_id"] for r in resp_range] == [r["message_id"] for r in py_range]
    assert [r["consumer"] for r in resp_range] == [r["consumer"] for r in py_range]
    assert [r["times_delivered"] for r in resp_range] == [r["times_delivered"] for r in py_range]

    # And the actual audit produces the same findings through either client.
    report_py = audit_group(real_redis, "parity", "g")
    report_resp = audit_group(resp_client, "parity", "g")
    assert report_py.pending_total == report_resp.pending_total
    assert [f.kind for f in report_py.findings] == [f.kind for f in report_resp.findings]
