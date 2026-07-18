import asyncio
import contextlib
import os
import signal
import uuid

import httpx
from redis.exceptions import ResponseError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from relay.config import get_settings
from relay.db.engine import get_engine, get_session_factory
from relay.db.models import Delivery, DeliveryAttempt, Endpoint, Event
from relay.delivery.enqueue import close_redis, get_redis, stream_key
from relay.delivery.sender import send_delivery
from relay.observability import get_logger, setup_logging

log = get_logger(__name__)

CONSUMER_GROUP = "workers"
BLOCK_MS = 2000


async def ensure_group(shard: int) -> None:
    """Create the consumer group, tolerating races with other workers."""
    try:
        await get_redis().xgroup_create(stream_key(shard), CONSUMER_GROUP, id="0", mkstream=True)
    except ResponseError as exc:
        if "BUSYGROUP" not in str(exc):
            raise


async def process_delivery(
    session: AsyncSession, client: httpx.AsyncClient, delivery_id: uuid.UUID
) -> bool:
    """Deliver one message and record the attempt. Returns True if it may be ACKed.

    Returning True does not mean the delivery succeeded — only that its outcome
    is durably recorded, so replaying the stream entry would add nothing.
    """
    delivery = (
        await session.execute(select(Delivery).where(Delivery.id == delivery_id))
    ).scalar_one_or_none()
    if delivery is None:
        # Event/endpoint was deleted; nothing to do, drop the entry.
        log.warning("delivery_missing", delivery_id=str(delivery_id))
        return True
    if delivery.status == "delivered":
        # Duplicate stream entry (at-least-once): already done.
        return True

    endpoint = (
        await session.execute(select(Endpoint).where(Endpoint.id == delivery.endpoint_id))
    ).scalar_one_or_none()
    event = (
        await session.execute(select(Event).where(Event.id == delivery.event_id))
    ).scalar_one_or_none()
    if endpoint is None or event is None:
        log.warning("delivery_orphaned", delivery_id=str(delivery_id))
        return True

    delivery.status = "delivering"
    delivery.attempt_count += 1
    attempt_number = delivery.attempt_count
    await session.commit()

    result = await send_delivery(
        client,
        url=endpoint.url,
        secret=endpoint.signing_secret,
        delivery_id=delivery.id,
        event_id=event.id,
        payload=event.payload,
    )

    session.add(
        DeliveryAttempt(
            delivery_id=delivery.id,
            attempt_number=attempt_number,
            latency_ms=result.latency_ms,
            http_status=result.http_status,
            error_class=result.error_class,
            response_snippet=result.response_snippet,
        )
    )
    # Phase 3 replaces 'failed' with backoff scheduling / DLQ transitions.
    delivery.status = "delivered" if result.succeeded else "failed"
    await session.commit()

    log.info(
        "delivery_attempted",
        delivery_id=str(delivery.id),
        event_id=str(event.id),
        endpoint_id=str(endpoint.id),
        attempt=attempt_number,
        status=delivery.status,
        http_status=result.http_status,
        error_class=result.error_class,
        latency_ms=result.latency_ms,
    )
    return True


async def consume_shard(
    shard: int, consumer_name: str, client: httpx.AsyncClient, stop: asyncio.Event
) -> None:
    key = stream_key(shard)
    redis = get_redis()
    session_factory = get_session_factory()

    while not stop.is_set():
        entries = await redis.xreadgroup(
            CONSUMER_GROUP, consumer_name, {key: ">"}, count=1, block=BLOCK_MS
        )
        if not entries:
            continue
        for _stream, messages in entries:
            for message_id, fields in messages:
                delivery_id = uuid.UUID(fields["delivery_id"])
                try:
                    async with session_factory() as session:
                        should_ack = await process_delivery(session, client, delivery_id)
                except Exception as exc:
                    # Leave the entry pending: it stays in the PEL and can be
                    # reclaimed rather than silently dropped.
                    log.error(
                        "delivery_processing_failed",
                        delivery_id=str(delivery_id),
                        error=str(exc),
                        exc_info=True,
                    )
                    continue
                if should_ack:
                    # ACK only after the attempt row is committed — at-least-once.
                    await redis.xack(key, CONSUMER_GROUP, message_id)


async def run_worker() -> None:
    setup_logging()
    settings = get_settings()
    consumer_name = f"worker-{os.getpid()}-{uuid.uuid4().hex[:6]}"

    for shard in range(settings.stream_shards):
        await ensure_group(shard)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop.set)

    log.info("worker_started", consumer=consumer_name, shards=settings.stream_shards)

    timeout = httpx.Timeout(settings.delivery_timeout_seconds)
    limits = httpx.Limits(max_connections=settings.worker_concurrency)
    async with httpx.AsyncClient(timeout=timeout, limits=limits, follow_redirects=False) as client:
        tasks = [
            asyncio.create_task(consume_shard(shard, consumer_name, client, stop))
            for shard in range(settings.stream_shards)
        ]
        await stop.wait()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    await close_redis()
    await get_engine().dispose()
    log.info("worker_stopped", consumer=consumer_name)


if __name__ == "__main__":
    asyncio.run(run_worker())
