"""A minimal, real RESP2 client over a raw socket -- no ``redis`` import.

This exists so the CLI works with zero runtime dependencies: it does not
have a ``redis`` package to connect with, so it speaks just enough of the
wire protocol to run the six read-only commands this package needs, and
shapes the replies exactly like redis-py does (dicts with the same key
names) so :mod:`redis_streams_audit.audit` cannot tell the difference.

If you already have a ``redis.Redis`` instance in your own code, use that
instead -- pass it straight to :func:`redis_streams_audit.audit.audit`, it
satisfies :class:`~redis_streams_audit.protocol.RedisStreamsClient` as-is.
This client is for the CLI, and for anyone who wants a Streams client with
no dependency at all.

Only what this package's protocol needs is implemented: ``XLEN``,
``XINFO STREAM/GROUPS/CONSUMERS``, ``XPENDING`` (summary and extended).
There is no pipelining, no RESP3, no TLS, no Cluster redirection, and no
``AUTH`` with a username (``AUTH password`` only, i.e. Redis's legacy
single-password auth or an ACL user's ``requirepass``-equivalent).
"""

from __future__ import annotations

import socket
from typing import Any, Mapping, Sequence


class RedisError(RuntimeError):
    """Raised for a RESP error reply (Redis returned ``-ERR ...``)."""


def _encode(parts: Sequence[object]) -> bytes:
    out = [f"*{len(parts)}\r\n".encode()]
    for part in parts:
        raw = part if isinstance(part, (bytes, bytearray)) else str(part).encode()
        out.append(f"${len(raw)}\r\n".encode() + bytes(raw) + b"\r\n")
    return b"".join(out)


class _Reader:
    def __init__(self, sock: socket.socket) -> None:
        self._sock = sock
        self._buf = b""

    def _readline(self) -> bytes:
        while b"\r\n" not in self._buf:
            chunk = self._sock.recv(65536)
            if not chunk:
                raise ConnectionError("connection closed by server")
            self._buf += chunk
        line, self._buf = self._buf.split(b"\r\n", 1)
        return line

    def _read_exact(self, n: int) -> bytes:
        while len(self._buf) < n + 2:
            chunk = self._sock.recv(65536)
            if not chunk:
                raise ConnectionError("connection closed by server")
            self._buf += chunk
        data, self._buf = self._buf[:n], self._buf[n + 2 :]
        return data

    def read_reply(self) -> Any:
        line = self._readline()
        marker, payload = line[:1], line[1:]
        if marker == b"+":
            return payload.decode()
        if marker == b"-":
            raise RedisError(payload.decode())
        if marker == b":":
            return int(payload)
        if marker == b"$":
            length = int(payload)
            if length == -1:
                return None
            return self._read_exact(length).decode()
        if marker == b"*":
            length = int(payload)
            if length == -1:
                return None
            return [self.read_reply() for _ in range(length)]
        raise ValueError(f"unknown RESP marker {marker!r} in reply {line!r}")


def _pairs_to_dict(flat: list[Any]) -> dict[str, Any]:
    return {flat[i]: flat[i + 1] for i in range(0, len(flat), 2)}


def _entry(raw: Any) -> list[Any] | None:
    """``[id, [f1, v1, f2, v2, ...]]`` -> ``[id, {f1: v1, ...}]``, matching
    redis-py's shape for ``first-entry``/``last-entry``."""

    if raw is None:
        return None
    entry_id, fields = raw
    return [entry_id, _pairs_to_dict(fields) if fields is not None else {}]


class MinimalRedisClient:
    """A tiny, real socket-level Redis client covering exactly the six
    read-only Streams commands :class:`~redis_streams_audit.protocol.RedisStreamsClient`
    declares. Reply shapes match redis-py's ``decode_responses=True`` client."""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        *,
        password: str | None = None,
        db: int = 0,
        socket_timeout: float = 5.0,
    ) -> None:
        self._sock = socket.create_connection((host, port), timeout=socket_timeout)
        self._reader = _Reader(self._sock)
        if password is not None:
            self._call("AUTH", password)
        if db:
            self._call("SELECT", db)

    def _call(self, *parts: object) -> Any:
        self._sock.sendall(_encode(parts))
        return self._reader.read_reply()

    def close(self) -> None:
        self._sock.close()

    def __enter__(self) -> "MinimalRedisClient":
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    # -- RedisStreamsClient -------------------------------------------------

    def xlen(self, name: str) -> int:
        return int(self._call("XLEN", name))

    def xinfo_stream(self, name: str) -> Mapping[str, Any]:
        flat = self._call("XINFO", "STREAM", name)
        info = _pairs_to_dict(flat)
        if "first-entry" in info:
            info["first-entry"] = _entry(info["first-entry"])
        if "last-entry" in info:
            info["last-entry"] = _entry(info["last-entry"])
        return info

    def xinfo_groups(self, name: str) -> Sequence[Mapping[str, Any]]:
        groups = self._call("XINFO", "GROUPS", name)
        return [_pairs_to_dict(g) for g in groups]

    def xinfo_consumers(self, name: str, groupname: str) -> Sequence[Mapping[str, Any]]:
        consumers = self._call("XINFO", "CONSUMERS", name, groupname)
        return [_pairs_to_dict(c) for c in consumers]

    def xpending(self, name: str, groupname: str) -> Mapping[str, Any] | None:
        count, min_id, max_id, consumers_raw = self._call("XPENDING", name, groupname)
        consumers = (
            [{"name": n, "pending": int(p)} for n, p in consumers_raw]
            if consumers_raw
            else []
        )
        return {"pending": count, "min": min_id, "max": max_id, "consumers": consumers}

    def xpending_range(
        self,
        name: str,
        groupname: str,
        min: str,
        max: str,
        count: int,
        consumername: str | None = None,
    ) -> Sequence[Mapping[str, Any]]:
        args: list[object] = ["XPENDING", name, groupname, min, max, count]
        if consumername is not None:
            args.append(consumername)
        rows = self._call(*args)
        return [
            {
                "message_id": row[0],
                "consumer": row[1],
                "time_since_delivered": row[2],
                "times_delivered": row[3],
            }
            for row in rows
        ]
