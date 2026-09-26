"""The types every audit result is made of."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import IntEnum
from typing import Any


class Severity(IntEnum):
    """Ordered so ``max(severities)`` picks the worst one."""

    INFO = 0
    WARNING = 1
    CRITICAL = 2

    def __str__(self) -> str:  # pragma: no cover - trivial
        return self.name


@dataclass(frozen=True, slots=True)
class Remedy:
    """A concrete, copy-pasteable next step -- never just "investigate"."""

    command: str
    """The Redis command to run, with real argument names filled in."""

    explanation: str
    """Why this specific command, in this specific case."""


@dataclass(frozen=True, slots=True)
class Finding:
    """One observation about one stream/group (and, often, one consumer or
    one PEL entry)."""

    stream: str
    group: str
    kind: str
    severity: Severity
    message: str
    remedy: Remedy | None = None
    consumer: str | None = None
    message_id: str | None = None
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "stream": self.stream,
            "group": self.group,
            "kind": self.kind,
            "severity": self.severity.name,
            "message": self.message,
        }
        if self.consumer is not None:
            d["consumer"] = self.consumer
        if self.message_id is not None:
            d["message_id"] = self.message_id
        if self.remedy is not None:
            d["remedy"] = {
                "command": self.remedy.command,
                "explanation": self.remedy.explanation,
            }
        if self.details:
            d["details"] = self.details
        return d
