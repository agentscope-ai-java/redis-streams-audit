from __future__ import annotations

import os
import socket
import warnings

import pytest

REDIS_HOST = os.environ.get("REDIS_STREAMS_AUDIT_TEST_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_STREAMS_AUDIT_TEST_PORT", "16379"))


def _redis_reachable() -> bool:
    try:
        with socket.create_connection((REDIS_HOST, REDIS_PORT), timeout=0.5):
            return True
    except OSError:
        return False


@pytest.fixture(scope="session")
def redis_available() -> bool:
    return _redis_reachable()


@pytest.fixture
def real_redis(redis_available: bool):
    """A real redis-py client against a live redis:7-alpine container, used
    to build test fixtures (streams, groups, deliveries, trims) with real
    Redis semantics -- never mocked. Skips loudly, never silently, when no
    server is reachable.
    """

    if not redis_available:
        pytest.skip(
            f"SKIPPED LOUDLY: no Redis reachable at {REDIS_HOST}:{REDIS_PORT} -- "
            f"start one with `docker run -d -p {REDIS_PORT}:6379 redis:7-alpine` "
            f"to run the real-server test suite.",
            allow_module_level=False,
        )

    redis = pytest.importorskip("redis")
    client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
    client.flushall()
    yield client
    client.flushall()
    client.close()


@pytest.fixture
def resp_client(redis_available: bool):
    """Our own zero-dependency RESP client, pointed at the same server as
    ``real_redis``. Used to prove the CLI's built-in client parses replies
    identically to redis-py, against the same live data."""

    if not redis_available:
        pytest.skip(
            f"SKIPPED LOUDLY: no Redis reachable at {REDIS_HOST}:{REDIS_PORT}",
            allow_module_level=False,
        )

    from redis_streams_audit.resp import MinimalRedisClient

    client = MinimalRedisClient(host=REDIS_HOST, port=REDIS_PORT)
    yield client
    client.close()


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    if not _redis_reachable():
        warnings.warn(
            f"redis-streams-audit: no Redis reachable at {REDIS_HOST}:{REDIS_PORT} -- "
            f"integration tests will be SKIPPED, not silently passed.",
            stacklevel=1,
        )
