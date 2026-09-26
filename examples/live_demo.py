"""Runs the audit against a real redis:7-alpine container and prints real
output -- the same transcript pasted into the README.

Reproduces two of the failure modes from the README in one pass:
- a crashed consumer ("doomed-worker") that read 3 entries and never acked
- a stream ("events") trimmed to zero while 3 entries were still pending

    docker run -d -p 16379:6379 redis:7-alpine
    python examples/live_demo.py
"""

from __future__ import annotations

import os
import sys
import time

import redis

from redis_streams_audit import Thresholds, audit
from redis_streams_audit.report import render_text

HOST = os.environ.get("REDIS_STREAMS_AUDIT_TEST_HOST", "localhost")
PORT = int(os.environ.get("REDIS_STREAMS_AUDIT_TEST_PORT", "16379"))


def main() -> int:
    client = redis.Redis(host=HOST, port=PORT, decode_responses=True)
    client.flushall()

    # -- orders/workers: a consumer that crashed before XACK ----------------
    for i in range(3):
        client.xadd("orders", {"order_id": str(i)})
    client.xgroup_create("orders", "workers", id="0")
    client.xreadgroup("workers", "doomed-worker", {"orders": ">"}, count=3)
    time.sleep(0.5)  # stand-in for "the crash happened a while ago"

    # -- events/g: XTRIM removes stream data while entries are pending -------
    for i in range(3):
        client.xadd("events", {"k": f"v{i}"})
    client.xgroup_create("events", "g", id="0")
    client.xreadgroup("g", "c1", {"events": ">"}, count=3)
    client.xtrim("events", maxlen=0, approximate=False)

    report = audit(
        client,
        ["orders", "events"],
        thresholds=Thresholds(dead_consumer_after_ms=300),
    )
    print(render_text(report))

    client.flushall()
    client.close()
    return report.exit_code


if __name__ == "__main__":
    sys.exit(main())
