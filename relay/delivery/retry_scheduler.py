"""Polls `relay:retries` and moves due deliveries back onto their streams.

Runs as its own process (compose service `retry-scheduler`). Multiple copies
are safe: the Lua move is atomic, so a delivery is re-enqueued exactly once
per due time even if several schedulers poll simultaneously.
"""

import asyncio
import contextlib
import signal

from redis.exceptions import RedisError

from relay.db.engine import get_engine
from relay.delivery.enqueue import close_redis
from relay.delivery.retry_queue import move_due_to_streams, retry_queue_depth
from relay.observability import get_logger, setup_logging

log = get_logger(__name__)

POLL_INTERVAL_S = 0.5
BATCH_LIMIT = 100

# Same reasoning as the worker: a Redis blip must never kill the loop, or the
# process stays alive while silently scheduling nothing.
RECONNECT_BASE_DELAY_S = 0.5
RECONNECT_MAX_DELAY_S = 10.0


async def poll_once() -> int:
    moved = await move_due_to_streams(limit=BATCH_LIMIT)
    if moved:
        log.info("retries_requeued", count=moved, queue_depth=await retry_queue_depth())
    return moved


async def run_scheduler(stop: asyncio.Event | None = None) -> None:
    setup_logging()
    stop = stop or asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop.set)

    log.info("retry_scheduler_started", poll_interval_s=POLL_INTERVAL_S)
    delay = RECONNECT_BASE_DELAY_S

    while not stop.is_set():
        try:
            moved = await poll_once()
            delay = RECONNECT_BASE_DELAY_S
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
