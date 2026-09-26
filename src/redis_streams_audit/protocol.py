"""The duck-typed client shape this package needs.

``redis-streams-audit`` never imports ``redis``. It calls seven read-only
Streams commands, spelled exactly the way `redis-py <https://github.com/redis/redis-py>`_
spells them (method name, argument order, return shape), because that is the
client almost everyone already has connected to production. Hand this
package a real ``redis.Redis`` / ``redis.asyncio.Redis`` instance (with
``decode_responses=True``) and it works with no adapter layer. Hand it
anything else with the same seven methods -- a hand-rolled RESP client, a
test double, a cluster-aware wrapper -- and it works identically, because
:class:`RedisStreamsClient` is checked structurally, not by import.

The audit never calls ``XCLAIM``, ``XAUTOCLAIM``, ``XACK`` or ``XTRIM``
itself. It only reads state and tells you which of those commands to run
and with what arguments -- see :mod:`redis_streams_audit.audit`.
"""

from __future__ import annotations

from typing import Any, Mapping, Protocol, Sequence, runtime_checkable


@runtime_checkable
class RedisStreamsClient(Protocol):
    """Structural shape of a redis-py-compatible client's Streams surface.

    Every method here maps to one Redis command and matches redis-py's
    signature and return shape (dicts with ``str`` keys, i.e.
    ``decode_responses=True``). Only the methods this package actually
    calls are declared -- a real client will have many more, and that is
    fine; ``Protocol`` matching only requires these to be present.
    """

    def xlen(self, name: str) -> int:
        """``XLEN key`` -- number of entries currently in the stream."""
        ...

    def xinfo_stream(self, name: str) -> Mapping[str, Any]:
        """``XINFO STREAM key`` -- length, first/last entry, entries-added."""
        ...

    def xinfo_groups(self, name: str) -> Sequence[Mapping[str, Any]]:
        """``XINFO GROUPS key`` -- one dict per consumer group on the stream."""
        ...

    def xinfo_consumers(
        self, name: str, groupname: str
    ) -> Sequence[Mapping[str, Any]]:
        """``XINFO CONSUMERS key group`` -- one dict per known consumer."""
        ...

    def xpending(self, name: str, groupname: str) -> Mapping[str, Any] | None:
        """``XPENDING key group`` (summary form) -- count, id range, per-consumer counts."""
        ...

    def xpending_range(
        self,
        name: str,
        groupname: str,
        min: str,
        max: str,
        count: int,
        consumername: str | None = None,
    ) -> Sequence[Mapping[str, Any]]:
        """``XPENDING key group start end count [consumer]`` (extended form) --
        one dict per pending entry: ``message_id``, ``consumer``,
        ``time_since_delivered`` (ms), ``times_delivered``."""
        ...
