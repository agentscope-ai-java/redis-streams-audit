"""An in-memory stand-in for :class:`~redis_streams_audit.protocol.RedisStreamsClient`.

Deliberately reimplements nothing from Redis's own PEL semantics -- it is
a hand-built fixture for exercising :mod:`redis_streams_audit.audit`'s
branches quickly and deterministically, not a Streams simulator. The
behaviours that matter (what XTRIM does to a PEL, what XAUTOCLAIM reports)
are established against a real ``redis:7-alpine`` container in
``test_integration.py`` instead -- see that file's docstring.
"""

from __future__ import annotations

from typing import Any, Mapping, Sequence


class FakeStreamsClient:
    def __init__(self) -> None:
        self.streams: dict[str, dict[str, Any]] = {}

    def add_stream(
        self,
        name: str,
        *,
        length: int,
        first_entry_id: str | None,
    ) -> None:
        self.streams[name] = {
            "length": length,
            "first-entry": [first_entry_id, {}] if first_entry_id else None,
            "last-entry": None,
            "groups": {},
        }

    def add_group(self, stream: str, group: str, *, lag: int | None = 0) -> None:
        self.streams[stream]["groups"][group] = {
            "name": group,
            "consumers": {},
            "pending_entries": [],
            "lag": lag,
        }

    def add_consumer(
        self, stream: str, group: str, name: str, *, pending: int, idle: int, inactive: int | None
    ) -> None:
        self.streams[stream]["groups"][group]["consumers"][name] = {
            "name": name,
            "pending": pending,
            "idle": idle,
            "inactive": inactive,
        }

    def add_pending(
        self,
        stream: str,
        group: str,
        *,
        message_id: str,
        consumer: str,
        age_ms: int,
        times_delivered: int,
    ) -> None:
        self.streams[stream]["groups"][group]["pending_entries"].append(
            {
                "message_id": message_id,
                "consumer": consumer,
                "time_since_delivered": age_ms,
                "times_delivered": times_delivered,
            }
        )

    # -- RedisStreamsClient protocol -----------------------------------------

    def xlen(self, name: str) -> int:
        return int(self.streams[name]["length"])

    def xinfo_stream(self, name: str) -> Mapping[str, Any]:
        s = self.streams[name]
        return {"length": s["length"], "first-entry": s["first-entry"], "last-entry": s["last-entry"]}

    def xinfo_groups(self, name: str) -> Sequence[Mapping[str, Any]]:
        groups = self.streams[name]["groups"]
        return [
            {
                "name": g["name"],
                "consumers": len(g["consumers"]),
                "pending": len(g["pending_entries"]),
                "lag": g["lag"],
            }
            for g in groups.values()
        ]

    def xinfo_consumers(self, name: str, groupname: str) -> Sequence[Mapping[str, Any]]:
        return list(self.streams[name]["groups"][groupname]["consumers"].values())

    def xpending(self, name: str, groupname: str) -> Mapping[str, Any] | None:
        entries = self.streams[name]["groups"][groupname]["pending_entries"]
        return {"pending": len(entries), "min": None, "max": None, "consumers": []}

    def xpending_range(
        self,
        name: str,
        groupname: str,
        min: str,
        max: str,
        count: int,
        consumername: str | None = None,
    ) -> Sequence[Mapping[str, Any]]:
        # Mirrors real XPENDING's exclusive-start-id paging (a "(<id>" min),
        # since audit._iter_pending relies on it to page without repeating
        # entries -- a fake that ignored min here would loop forever on any
        # PEL bigger than one page.
        entries = self.streams[name]["groups"][groupname]["pending_entries"]
        if consumername is not None:
            entries = [e for e in entries if e["consumer"] == consumername]
        if min not in ("-", "("):
            if min.startswith("("):
                lower = min[1:]
                entries = [e for e in entries if e["message_id"] > lower]
            else:
                entries = [e for e in entries if e["message_id"] >= min]
        return entries[:count]
