"""Turning an :class:`~redis_streams_audit.audit.AuditReport` into text."""

from __future__ import annotations

import json
from typing import TextIO

from .audit import AuditReport
from .findings import Severity

_SEVERITY_MARK = {
    Severity.INFO: "info",
    Severity.WARNING: "WARN",
    Severity.CRITICAL: "CRIT",
}


def to_json(report: AuditReport, *, indent: int | None = 2) -> str:
    return json.dumps(report.to_dict(), indent=indent, sort_keys=False)


def render_text(report: AuditReport) -> str:
    lines: list[str] = []

    for g in report.groups:
        lines.append(f"== {g.stream} / {g.group} ==")
        lines.append(
            f"  pending: {g.pending_total}  lag: {g.lag if g.lag is not None else 'unknown'}"
        )
        if g.age_stats.count:
            bucket_str = ", ".join(
                f"{label}={count}" for label, count in g.age_stats.buckets.items() if count
            )
            lines.append(
                f"  age: min={g.age_stats.min_ms}ms max={g.age_stats.max_ms}ms "
                f"mean={g.age_stats.mean_ms:.0f}ms  [{bucket_str}]"
            )
        for c in g.consumers:
            lines.append(
                f"  consumer {c.name!r}: pending={c.pending} idle={c.idle_ms}ms "
                f"inactive={c.inactive_ms if c.inactive_ms is not None else 'n/a'}ms"
            )
        if not g.findings:
            lines.append("  (no findings)")
        for f in g.findings:
            mark = _SEVERITY_MARK[f.severity]
            who = f" [{f.consumer}]" if f.consumer else ""
            lines.append(f"  [{mark}] {f.kind}{who}: {f.message}")
            if f.remedy:
                lines.append(f"         -> {f.remedy.command}")
                lines.append(f"            {f.remedy.explanation}")
        lines.append("")

    if report.errors:
        lines.append("== errors ==")
        for err in report.errors:
            lines.append(f"  [ERROR] {err}")
        lines.append("")

    total = len(report.findings)
    crit = sum(1 for f in report.findings if f.severity is Severity.CRITICAL)
    warn = sum(1 for f in report.findings if f.severity is Severity.WARNING)
    lines.append(
        f"{total} finding(s): {crit} critical, {warn} warning, "
        f"{total - crit - warn} info; {len(report.errors)} error(s); "
        f"exit {report.exit_code}"
    )
    return "\n".join(lines)


def print_report(report: AuditReport, *, as_json: bool, stream: TextIO) -> None:
    if as_json:
        print(to_json(report), file=stream)
    else:
        print(render_text(report), file=stream)
