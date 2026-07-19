"""The retry ZSET and its atomic Lua move. Requires Redis on localhost.

These run against a dedicated Redis database: the live retry-scheduler
container polls the real `relay:retries` continuously, so sharing a keyspace
with it makes any assertion about queue depth a race.
"""

import time
import uuid

import pytest
import redis.asyncio as aioredis

from relay.delivery import retry_queue as retry_queue_mod
from relay.delivery.enqueue import shard_for, stream_key
from relay.delivery.retry_queue import (
    RETRY_ZSET,
    encode_member,
    move_due_to_streams,
    remove_from_retry_queue,
    retry_queue_depth,
    schedule_retry,
)
from tests.conftest import requires_infra

pytestmark = requires_infra

# db 15: isolated from the running stack (which uses db 0).
TEST_REDIS_URL = "redis://localhost:6379/15"


@pytest.fixture(autouse=True)
async def isolated_redis(monkeypatch):
    client = aioredis.from_url(TEST_REDIS_URL, decode_responses=True)
    await client.flushdb()
    monkeypatch.setattr(retry_queue_mod, "get_redis", lambda: client)
    yield client
    await client.flushdb()
    await client.aclose()


def get_redis():
    return retry_queue_mod.get_redis()


def _ids():
    return uuid.uuid4(), uuid.uuid4(), uuid.uuid4()


async def _clear():
    await get_redis().delete(RETRY_ZSET)


def test_member_encoding_round_trip():
    delivery_id, event_id, endpoint_id = _ids()
    member = encode_member(delivery_id, event_id, endpoint_id)
    parts = member.split("|")
    assert parts[0] == str(delivery_id)
    assert parts[1] == str(event_id)
    assert parts[2] == str(endpoint_id)
    # Shard is baked in so the scheduler never needs Postgres.
    assert parts[3] == str(shard_for(endpoint_id))


async def test_schedule_sets_due_score():
    await _clear()
    delivery_id, event_id, endpoint_id = _ids()
    now = time.time()
    due_at = await schedule_retry(delivery_id, event_id, endpoint_id, 30.0, now=now)
    assert due_at == now + 30.0
    assert await retry_queue_depth() == 1


async def test_not_due_stays_queued():
    await _clear()
    delivery_id, event_id, endpoint_id = _ids()
    await schedule_retry(delivery_id, event_id, endpoint_id, 3600.0)
    assert await move_due_to_streams() == 0
    assert await retry_queue_depth() == 1


async def test_due_entry_moves_to_correct_shard_stream():
    await _clear()
    delivery_id, event_id, endpoint_id = _ids()
    key = stream_key(shard_for(endpoint_id))
    before = await get_redis().xlen(key)

    await schedule_retry(delivery_id, event_id, endpoint_id, -1.0)  # already due
    assert await move_due_to_streams() == 1
    assert await retry_queue_depth() == 0
    assert await get_redis().xlen(key) == before + 1

    entries = await get_redis().xrange(key, count=200)
    payloads = [fields for _mid, fields in entries]
    assert {
        "delivery_id": str(delivery_id),
        "event_id": str(event_id),
        "endpoint_id": str(endpoint_id),
    } in payloads


async def test_concurrent_moves_enqueue_exactly_once():
    """Two schedulers polling together must not double-enqueue (ZREM+XADD is atomic)."""
    import asyncio

    await _clear()
    delivery_id, event_id, endpoint_id = _ids()
    key = stream_key(shard_for(endpoint_id))
    before = await get_redis().xlen(key)

    await schedule_retry(delivery_id, event_id, endpoint_id, -1.0)
    results = await asyncio.gather(*[move_due_to_streams() for _ in range(5)])

    assert sum(results) == 1, f"expected exactly one mover to win, got {results}"
    assert await get_redis().xlen(key) == before + 1


async def test_batch_limit_respected():
    await _clear()
    for _ in range(10):
        await schedule_retry(*_ids(), -1.0)
    assert await move_due_to_streams(limit=4) == 4
    assert await retry_queue_depth() == 6


async def test_remove_from_retry_queue():
    await _clear()
    delivery_id, event_id, endpoint_id = _ids()
    await schedule_retry(delivery_id, event_id, endpoint_id, 3600.0)
    assert await remove_from_retry_queue(delivery_id, event_id, endpoint_id) is True
    assert await retry_queue_depth() == 0
    # removing again is a no-op, not an error
    assert await remove_from_retry_queue(delivery_id, event_id, endpoint_id) is False
