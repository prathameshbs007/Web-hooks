"""Regression tests for the worker's failure handling.

These cover defects found by runtime verification of Phase 2:
  1. a Redis blip killed the shard task, wedging the worker silently;
  2. entries a crashed worker had consumed were never reclaimed;
  3. the simulator's conn_reset mode returned HTTP 500 instead of resetting.
"""

import asyncio

import httpx
import pytest
from redis.exceptions import ConnectionError as RedisConnectionError

from relay.delivery import worker as worker_mod
from relay.delivery.sender import classify_exception


class FakeRedis:
    """Minimal stand-in that can be told to fail a given number of times."""

    def __init__(self, fail_times: int = 0):
        self.fail_times = fail_times
        self.xreadgroup_calls = 0
        self.acked: list[str] = []
        self.autoclaim_calls = 0

    async def xreadgroup(self, *args, **kwargs):
        self.xreadgroup_calls += 1
        if self.fail_times > 0:
            self.fail_times -= 1
            raise RedisConnectionError("connection lost")
        # Real xreadgroup blocks; yield so the loop can't spin hot and starve
        # the event loop (which would hang the test rather than fail it).
        await asyncio.sleep(0.01)
        return []

    async def xautoclaim(self, *args, **kwargs):
        self.autoclaim_calls += 1
        return "0-0", [], []

    async def xack(self, key, group, message_id):
        self.acked.append(message_id)

    async def xgroup_create(self, *args, **kwargs):
        return True


async def _run_briefly(coro_factory, seconds: float = 1.5) -> None:
    stop = asyncio.Event()
    task = asyncio.create_task(coro_factory(stop))
    await asyncio.sleep(seconds)
    stop.set()
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


async def test_consume_shard_survives_redis_connection_error(monkeypatch):
    """A Redis blip must be retried, not kill the task.

    Regression: xreadgroup sat outside the try/except, so one ConnectionError
    terminated the shard task while the process stayed alive and idle.
    """
    fake = FakeRedis(fail_times=3)
    monkeypatch.setattr(worker_mod, "get_redis", lambda: fake)
    monkeypatch.setattr(worker_mod, "get_session_factory", lambda: None)
    monkeypatch.setattr(worker_mod, "RECONNECT_BASE_DELAY_S", 0.05)
    monkeypatch.setattr(worker_mod, "RECONNECT_MAX_DELAY_S", 0.05)

    async with httpx.AsyncClient() as client:
        await _run_briefly(
            lambda stop: worker_mod.consume_shard(0, "test-consumer", client, stop)
        )

    # It kept calling past the 3 induced failures instead of dying on the first.
    assert fake.fail_times == 0
    assert fake.xreadgroup_calls > 3


async def test_consume_shard_recreates_group_after_redis_wipe(monkeypatch):
    """After a Redis restart the group may be gone; the loop must recreate it."""
    fake = FakeRedis(fail_times=2)
    created: list[int] = []

    async def fake_ensure(shard: int) -> None:
        created.append(shard)

    monkeypatch.setattr(worker_mod, "get_redis", lambda: fake)
    monkeypatch.setattr(worker_mod, "get_session_factory", lambda: None)
    monkeypatch.setattr(worker_mod, "ensure_group", fake_ensure)
    monkeypatch.setattr(worker_mod, "RECONNECT_BASE_DELAY_S", 0.05)
    monkeypatch.setattr(worker_mod, "RECONNECT_MAX_DELAY_S", 0.05)

    async with httpx.AsyncClient() as client:
        await _run_briefly(
            lambda stop: worker_mod.consume_shard(3, "test-consumer", client, stop)
        )

    assert created == [3, 3]


async def test_supervisor_restarts_a_dead_shard_task(monkeypatch):
    """An unexpected task death must be logged and the shard restarted.

    Regression: run_worker awaited a stop event and never noticed dead tasks,
    so the container reported healthy while delivering nothing.
    """
    starts = 0

    async def flaky_consume(shard, consumer_name, client, stop):
        nonlocal starts
        starts += 1
        if starts <= 2:
            raise RuntimeError("simulated shard crash")
        await stop.wait()

    monkeypatch.setattr(worker_mod, "consume_shard", flaky_consume)

    class OneShard:
        stream_shards = 1

    monkeypatch.setattr(worker_mod, "get_settings", lambda: OneShard())

    async with httpx.AsyncClient() as client:
        await _run_briefly(
            lambda stop: worker_mod.supervise_shards("test-consumer", client, stop),
            seconds=1.0,
        )

    assert starts >= 3, f"supervisor did not restart dead tasks (starts={starts})"


async def test_reclaim_stale_processes_abandoned_entries(monkeypatch):
    """Entries orphaned by a dead worker get claimed and processed."""
    handled: list[str] = []

    class ClaimingRedis(FakeRedis):
        def __init__(self):
            super().__init__()
            self.served = False

        async def xautoclaim(self, *args, **kwargs):
            self.autoclaim_calls += 1
            if not self.served:
                self.served = True
                return "0-0", [("1-1", {"delivery_id": "abc"})], []
            return "0-0", [], []

    fake = ClaimingRedis()
    monkeypatch.setattr(worker_mod, "get_redis", lambda: fake)

    async def fake_handle(key, message_id, fields, session_factory, client):
        handled.append(message_id)

    monkeypatch.setattr(worker_mod, "handle_message", fake_handle)

    async with httpx.AsyncClient() as client:
        count = await worker_mod.reclaim_stale("relay:deliveries:0", "c1", None, client)

    assert count == 1
    assert handled == ["1-1"]


async def test_reclaim_idle_threshold_exceeds_delivery_timeout():
    """Reclaiming sooner than the delivery timeout would steal live work."""
    from relay.config import get_settings

    assert worker_mod.RECLAIM_MIN_IDLE_MS > get_settings().delivery_timeout_seconds * 1000


def test_protocol_error_classifies_as_connection_failure():
    """conn_reset must not look like an HTTP status."""
    assert classify_exception(httpx.RemoteProtocolError("peer closed")) == "conn_refused"


@pytest.mark.parametrize("mode", ["healthy", "http_500", "conn_reset"])
def test_flaky_modes_are_registered(mode):
    from flaky_endpoint.main import MODES

    assert mode in MODES
