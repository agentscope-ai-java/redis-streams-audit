"""Auditor for Redis Streams consumer groups, focused on the Pending
Entries List (PEL): the part of Streams where production incidents
actually live.

See :mod:`redis_streams_audit.audit` for the checks and the reasoning
behind each one, and :mod:`redis_streams_audit.protocol` for the
duck-typed client shape this package expects.
"""

from __future__ import annotations

from .audit import (
    AgeStats,
    AuditReport,
    ConsumerStats,
    GroupNotFoundError,
    GroupReport,
    PendingEntry,
    StreamNotFoundError,
    audit,
    audit_group,
)
from .findings import Finding, Remedy, Severity
from .protocol import RedisStreamsClient
from .thresholds import Thresholds

__version__ = "0.1.0"

__all__ = [
    "AgeStats",
    "AuditReport",
    "ConsumerStats",
    "Finding",
    "GroupNotFoundError",
    "GroupReport",
    "PendingEntry",
    "RedisStreamsClient",
    "Remedy",
    "Severity",
    "StreamNotFoundError",
    "Thresholds",
    "audit",
    "audit_group",
]
