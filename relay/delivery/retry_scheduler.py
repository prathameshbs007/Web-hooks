"""Polls `relay:retries` and moves due deliveries back onto their streams.

Runs as its own process (compose service `retry-scheduler`). Multiple copies
are safe: the Lua move is atomic, so a delivery is re-enqueued exactly once
per due time even if several schedulers poll simultaneously.
"""

import asyncio
import contextlib
import signal

from redis.exceptions import RedisError
from sqlalchemy import func, select

from relay.db.engine import get_engine, get_session_factory
from relay.db.models import Delivery, Endpoint
from relay.delivery.circuit_breaker import get_state as get_breaker_state
from relay.delivery.enqueue import close_redis
from relay.delivery.retry_queue import move_due_to_streams, retry_queue_depth
from relay.metrics import (
    BREAKER_STATE_VALUES,
    breaker_state,
    dlq_size,
    serve_metrics,
)
from relay.metrics import retry_queue_depth as retry_queue_depth_gauge
from relay.observability import get_logger, setup_logging

log = get_logger(__name__)

POLL_INTERVAL_S = 0.5
BATCH_LIMIT = 100

SCHEDULER_METRICS_PORT = 9101
# Gauges are swept far less often than the retry poll: they cost DB queries and
# Prometheus only scrapes every 5s anyway.
GAUGE_REFRESH_INTERVAL_S = 5.0

# Same reasoning as the worker: a Redis blip must never kill the loop, or the
# process stays alive while silently scheduling nothing.
RECONNECT_BASE_DELAY_S = 0.5
RECONNECT_MAX_DELAY_S = 10.0


async def poll_once() -> int:
    moved = await move_due_to_streams(limit=BATCH_LIMIT)
    if moved:
        log.info("retries_requeued", count=moved, queue_depth=await retry_queue_depth())
    return moved


async def refresh_gauges() -> None:
    """Publish the point-in-time gauges (spec Section 10).

    These live here rather than in the API because they need a periodic sweep,
    and computing them on every /metrics scrape would put unbounded query load
    on Postgres from anyone who can reach the endpoint.
    """
    retry_queue_depth_gauge.set(await retry_queue_depth())

    async with get_session_factory()() as session:
        rows = await session.execute(
            select(Endpoint.tenant_id, func.count(Delivery.id))
            .join(Delivery, Delivery.endpoint_id == Endpoint.id)
            .where(Delivery.status == "dead")
            .group_by(Endpoint.tenant_id)
        )
        for tenant_id, count in rows:
            dlq_size.labels(tenant=str(tenant_id)).set(count)

        # Only endpoints with a live breaker key are exported, so this gauge is
        # bounded by unhealthy endpoints rather than by the whole fleet.
        endpoints = (
            await session.execute(select(Endpoint.id).where(Endpoint.status == "active"))
        ).scalars()
        for endpoint_id in endpoints:
            state = (await get_breaker_state(endpoint_id))["state"]
            if state != "closed":
                breaker_state.labels(endpoint=str(endpoint_id)).set(
                    BREAKER_STATE_VALUES[state]
                )


async def run_scheduler(stop: asyncio.Event | None = None) -> None:
    setup_logging()
    stop = stop or asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop.set)

    serve_metrics(SCHEDULER_METRICS_PORT)
    log.info(
        "retry_scheduler_started",
        poll_interval_s=POLL_INTERVAL_S,
        metrics_port=SCHEDULER_METRICS_PORT,
    )
    delay = RECONNECT_BASE_DELAY_S
    loop_clock = asyncio.get_running_loop()
    next_gauge_refresh = loop_clock.time()

    while not stop.is_set():
        try:
            moved = await poll_once()
            delay = RECONNECT_BASE_DELAY_S
            if loop_clock.time() >= next_gauge_refresh:
                await refresh_gauges()
                next_gauge_refresh = loop_clock.time() + GAUGE_REFRESH_INTERVAL_S
            # Drain a burst without sleeping; otherwise wait for the next tick.
            if moved < BATCH_LIMIT:
                await asyncio.sleep(POLL_INTERVAL_S)
        except asyncio.CancelledError:
            raise
        except (RedisError, OSError) as exc:
            log.warning("redis_unavailable_retrying", error=str(exc), retry_in_s=delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, RECONNECT_MAX_DELAY_S)

    await close_redis()
    await get_engine().dispose()
    log.info("retry_scheduler_stopped")


if __name__ == "__main__":
    asyncio.run(run_scheduler())
