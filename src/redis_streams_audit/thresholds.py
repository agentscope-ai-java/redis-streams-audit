"""Thresholds that turn raw PEL numbers into findings.

Every number here has a production-sane default, and every one is
overridable per audit call or per CLI flag -- what counts as "a consumer
has probably died" is a judgment call that depends on your workload, not a
constant this package can know for you.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Thresholds:
    """Tunable limits applied while auditing a stream/group pair.

    All time thresholds are in milliseconds, to match the units Redis
    itself reports (``XPENDING``'s ``time_since_delivered`` and
    ``XINFO CONSUMERS``'s ``idle``/``inactive`` are both milliseconds).
    """

    stale_after_ms: int = 60_000
    """A pending entry idle at least this long is "stale" (WARNING)."""

    dead_consumer_after_ms: int = 300_000
    """A consumer with pending entries and ``inactive`` at least this long
    (no read, ack, claim or ping since) is reported as a dead-consumer
    candidate (WARNING) -- see :mod:`redis_streams_audit.audit` for why
    this can never be a certainty."""

    poison_delivery_count: int = 5
    """A pending entry with ``times_delivered`` strictly greater than this
    is reported as a poison-message candidate (CRITICAL)."""

    lag_warn: int = 1_000
    """Group lag (entries added but not yet delivered) at or above this is
    a WARNING."""

    lag_critical: int = 10_000
    """Group lag at or above this is CRITICAL."""

    pel_size_warn: int = 1_000
    """Total pending count for one group at or above this is a WARNING --
    signals unbounded PEL growth even before anything is provably stuck."""

    pel_size_critical: int = 10_000
    """Total pending count for one group at or above this is CRITICAL."""
