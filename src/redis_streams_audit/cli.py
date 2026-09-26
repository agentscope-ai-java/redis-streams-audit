"""``redis-streams-audit`` command-line entry point."""

from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from typing import Sequence

from .audit import audit
from .report import print_report
from .resp import MinimalRedisClient
from .thresholds import Thresholds


_DEFAULTS = Thresholds()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="redis-streams-audit",
        description=(
            "Audit Redis Streams consumer groups: Pending Entries List size and "
            "age, dead-consumer candidates, poison messages, entries orphaned by "
            "XTRIM, and group lag. Read-only -- never claims, acks or trims "
            "anything itself."
        ),
    )
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=6379)
    parser.add_argument("--password", default=None)
    parser.add_argument("--db", type=int, default=0)
    parser.add_argument(
        "--stream",
        action="append",
        dest="streams",
        metavar="NAME",
        required=True,
        help="A stream to audit. Repeat for multiple streams. Required -- "
        "this tool never scans the keyspace to find streams for you.",
    )
    parser.add_argument(
        "--group",
        action="append",
        dest="groups",
        metavar="STREAM:GROUP",
        default=[],
        help="Restrict a stream to one consumer group, as STREAM:GROUP. "
        "Repeatable. A stream with no --group given here has every group "
        "XINFO GROUPS reports for it audited.",
    )
    parser.add_argument("--json", action="store_true", help="Emit JSON instead of text.")
    parser.add_argument(
        "--stale-after-seconds",
        type=float,
        default=_DEFAULTS.stale_after_ms / 1000,
        help="A pending entry idle at least this long is reported stale (default: %(default)s).",
    )
    parser.add_argument(
        "--dead-consumer-after-seconds",
        type=float,
        default=_DEFAULTS.dead_consumer_after_ms / 1000,
        help="A consumer inactive at least this long, with pending entries, is a "
        "dead-consumer candidate (default: %(default)s).",
    )
    parser.add_argument(
        "--poison-threshold",
        type=int,
        default=_DEFAULTS.poison_delivery_count,
        help="A pending entry delivered more than this many times is a poison "
        "candidate (default: %(default)s).",
    )
    parser.add_argument("--lag-warn", type=int, default=_DEFAULTS.lag_warn)
    parser.add_argument("--lag-critical", type=int, default=_DEFAULTS.lag_critical)
    parser.add_argument("--pel-warn", type=int, default=_DEFAULTS.pel_size_warn)
    parser.add_argument("--pel-critical", type=int, default=_DEFAULTS.pel_size_critical)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    groups_by_stream: dict[str, list[str]] = defaultdict(list)
    for spec in args.groups:
        stream, sep, group = spec.partition(":")
        if not sep:
            parser.error(f"--group must be STREAM:GROUP, got {spec!r}")
        groups_by_stream[stream].append(group)

    thresholds = Thresholds(
        stale_after_ms=int(args.stale_after_seconds * 1000),
        dead_consumer_after_ms=int(args.dead_consumer_after_seconds * 1000),
        poison_delivery_count=args.poison_threshold,
        lag_warn=args.lag_warn,
        lag_critical=args.lag_critical,
        pel_size_warn=args.pel_warn,
        pel_size_critical=args.pel_critical,
    )

    try:
        client = MinimalRedisClient(
            host=args.host, port=args.port, password=args.password, db=args.db
        )
    except OSError as exc:
        print(f"redis-streams-audit: could not connect to {args.host}:{args.port}: {exc}", file=sys.stderr)
        return 3

    try:
        report = audit(client, args.streams, groups=groups_by_stream or None, thresholds=thresholds)
    finally:
        client.close()

    print_report(report, as_json=args.json, stream=sys.stdout)
    return report.exit_code


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
