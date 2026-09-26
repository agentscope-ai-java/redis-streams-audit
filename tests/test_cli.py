from __future__ import annotations

import json
import os

import pytest

from redis_streams_audit.cli import main

REDIS_HOST = os.environ.get("REDIS_STREAMS_AUDIT_TEST_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_STREAMS_AUDIT_TEST_PORT", "16379"))


def test_cli_json_exit_code_clean(real_redis, capsys: pytest.CaptureFixture[str]) -> None:
    real_redis.xadd("cli-stream", {"k": "v"})
    real_redis.xgroup_create("cli-stream", "g", id="0")
    real_redis.xreadgroup("g", "c1", {"cli-stream": ">"}, count=1)
    real_redis.xack("cli-stream", "g", *[
        m[0] for m in real_redis.xrange("cli-stream", "-", "+")
    ])

    code = main(
        [
            "--host",
            REDIS_HOST,
            "--port",
            str(REDIS_PORT),
            "--stream",
            "cli-stream",
            "--json",
        ]
    )
    out = json.loads(capsys.readouterr().out)
    assert code == 0
    assert out["exit_code"] == 0
    assert out["groups"][0]["pending_total"] == 0


def test_cli_reports_critical_exit_code_for_orphaned_pel(
    real_redis, capsys: pytest.CaptureFixture[str]
) -> None:
    real_redis.xadd("cli-trim", {"k": "v"})
    real_redis.xgroup_create("cli-trim", "g", id="0")
    real_redis.xreadgroup("g", "c1", {"cli-trim": ">"}, count=1)
    real_redis.xtrim("cli-trim", maxlen=0, approximate=False)

    code = main(
        [
            "--host",
            REDIS_HOST,
            "--port",
            str(REDIS_PORT),
            "--stream",
            "cli-trim",
            "--json",
        ]
    )
    out = json.loads(capsys.readouterr().out)
    assert code == 2
    kinds = {f["kind"] for f in out["groups"][0]["findings"]}
    assert "orphaned-pel-entry" in kinds


def test_cli_group_filter_parses_stream_colon_group(
    real_redis, capsys: pytest.CaptureFixture[str]
) -> None:
    real_redis.xadd("cli-filter", {"k": "v"})
    real_redis.xgroup_create("cli-filter", "g1", id="0")
    real_redis.xgroup_create("cli-filter", "g2", id="0")

    code = main(
        [
            "--host",
            REDIS_HOST,
            "--port",
            str(REDIS_PORT),
            "--stream",
            "cli-filter",
            "--group",
            "cli-filter:g1",
            "--json",
        ]
    )
    out = json.loads(capsys.readouterr().out)
    assert code == 0
    assert [g["group"] for g in out["groups"]] == ["g1"]


def test_cli_reports_connection_failure() -> None:
    code = main(["--host", "127.0.0.1", "--port", "1", "--stream", "x"])
    assert code == 3
