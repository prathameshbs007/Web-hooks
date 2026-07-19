import asyncio
import contextlib
import os
import signal
import time
import uuid
from datetime import UTC, datetime, timedelta

import httpx
from redis.exceptions import RedisError, ResponseError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from relay.config import get_settings
from relay.db.engine import get_engine, get_session_factory
from relay.db.models import Delivery, DeliveryAttempt, Endpoint, Event, Tenant
from relay.delivery.circuit_breaker import allow_request, record_outcome, should_auto_disable
from relay.delivery.enqueue import close_redis, get_redis, stream_key
from relay.delivery.ordering import acquire_lock, peek_head, pop_head, release_lock
from relay.delivery.rate_limit import acquire_slot, release_slot, take_token
from relay.delivery.retry import next_delay_seconds
from relay.delivery.retry_queue import schedule_retry
from relay.delivery.sender import send_delivery
from relay.observability import get_logger, setup_logging

log = get_logger(__name__)

CONSUMER_GROUP = "workers"
BLOCK_MS = 2000

# Redis outage handling: retry with capped exponential backoff rather than
# letting the shard task die. A blip must never wedge the worker.
RECONNECT_BASE_DELAY_S = 0.5
RECONNECT_MAX_DELAY_S = 10.0

# Entries a dead worker consumed but never ACKed are reclaimed after this long.
# Must exceed the delivery timeout so we don't steal in-flight work from a
# healthy peer.
RECLAIM_MIN_IDLE_MS = 60_000
RECLAIM_INTERVAL_S = 30.0

# How long a gated delivery waits before being reconsidered. Rate/concurrency
# gates clear in well under a second, so they retry fast; an open breaker
# should not be hammered, so it waits longer.
RATE_LIMIT_RETRY_DELAY_S = 0.5
CONCURRENCY_RETRY_DELAY_S = 0.5
ORDERED_WAIT_S = 0.25
BREAKER_RECHECK_DELAY_S = 30.0


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

    delay_seconds: float | None = None
    if result.succeeded:
        delivery.status = "delivered"
        delivery.next_attempt_at = None
    else:
        delay_seconds = next_delay_seconds(
            attempt_number,
            http_status=result.http_status,
            retry_after=result.retry_after,
            configured_max=get_settings().max_attempts,
        )
        if delay_seconds is None:
            # Attempts exhausted (or a terminal 4xx) → DLQ.
            delivery.status = "dead"
            delivery.next_attempt_at = None
        else:
            # 'failed' + next_attempt_at means "awaiting retry"; the scheduler
            # owns the re-enqueue, so nothing else writes to this row meanwhile.
            delivery.status = "failed"
            delivery.next_attempt_at = datetime.now(UTC) + timedelta(seconds=delay_seconds)

    await session.commit()

    # Schedule after the commit: the row must already say 'failed' before the
    # delivery can be picked up again, or a fast retry could race the write.
    if delay_seconds is not None:
        await schedule_retry(delivery.id, event.id, endpoint.id, delay_seconds)

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
        retry_in_s=round(delay_seconds, 2) if delay_seconds is not None else None,
    )
    if delivery.status == "dead":
        log.warning(
            "delivery_dead_lettered",
            delivery_id=str(delivery.id),
            endpoint_id=str(endpoint.id),
            attempts=attempt_number,
            last_error=result.error_class,
            last_http_status=result.http_status,
        )
    return True


async def defer_delivery(
    delivery_id: uuid.UUID, event_id: uuid.UUID, endpoint_id: uuid.UUID, delay_s: float
) -> None:
    """Push a delivery back onto the retry queue *without* recording an attempt.

    Gate rejections (breaker open, rate limited, at concurrency cap, waiting on
    an ordering lock) are not delivery failures — they must not consume the
    attempt budget or land in the DLQ.
    """
    await schedule_retry(delivery_id, event_id, endpoint_id, delay_s)


async def check_gates(
    session: AsyncSession, delivery: Delivery, endpoint: Endpoint, now: float
) -> tuple[bool, str | None, float]:
    """Breaker, rate-limit and concurrency gates. Returns (allowed, reason, retry_in)."""
    settings = get_settings()

    decision, state = await allow_request(endpoint.id, now)
    if decision == "deny":
        # 72h continuously open → disable the endpoint (matches industry behavior).
        if await should_auto_disable(endpoint.id, now):
            endpoint.status = "disabled"
            await session.commit()
            log.warning("endpoint_auto_disabled", endpoint_id=str(endpoint.id), after_hours=72)
            return False, "endpoint_disabled", BREAKER_RECHECK_DELAY_S
        return False, f"breaker_{state}", BREAKER_RECHECK_DELAY_S
    if decision == "probe":
        log.info("breaker_probe_allowed", endpoint_id=str(endpoint.id))

    tenant = (
        await session.execute(select(Tenant).where(Tenant.id == endpoint.tenant_id))
    ).scalar_one()
    rate = tenant.rate_per_sec or settings.default_tenant_rate_per_sec
    if not await take_token(tenant.id, rate, now):
        return False, "rate_limited", RATE_LIMIT_RETRY_DELAY_S

    return True, None, 0.0


async def deliver_with_gates(
    session: AsyncSession, client: httpx.AsyncClient, delivery_id: uuid.UUID
) -> bool:
    """Apply ordering, breaker, rate and concurrency gates, then deliver.

    Returns True when the stream entry may be ACKed. A gated delivery is ACKed
    too: it has been re-scheduled on the retry queue, so replaying the entry
    would only duplicate it.
    """
    now = time.time()
    delivery = (
        await session.execute(select(Delivery).where(Delivery.id == delivery_id))
    ).scalar_one_or_none()
    if delivery is None or delivery.status in {"delivered", "dead"}:
        return True

    endpoint = (
        await session.execute(select(Endpoint).where(Endpoint.id == delivery.endpoint_id))
    ).scalar_one_or_none()
    if endpoint is None:
        return True
    if endpoint.status != "active":
        log.info(
            "delivery_skipped_inactive_endpoint",
            delivery_id=str(delivery_id),
            endpoint_status=endpoint.status,
        )
        return True

    lock_token: str | None = None
    if endpoint.ordering == "ordered":
        lock_token = await acquire_lock(endpoint.id)
        if lock_token is None:
            # Another worker is delivering for this endpoint; try again shortly.
            await defer_delivery(delivery.id, delivery.event_id, endpoint.id, ORDERED_WAIT_S)
            return True
        head = await peek_head(endpoint.id)
        if head is not None and head != delivery.id:
            # Not our turn. Wait behind the head — this is the head-of-line
            # blocking that 'ordered' explicitly trades throughput for.
            await release_lock(endpoint.id, lock_token)
            await defer_delivery(delivery.id, delivery.event_id, endpoint.id, ORDERED_WAIT_S)
            return True

    slot_token: str | None = None
    try:
        allowed, reason, retry_in = await check_gates(session, delivery, endpoint, now)
        if not allowed:
            log.info(
                "delivery_gated",
                delivery_id=str(delivery_id),
                endpoint_id=str(endpoint.id),
                reason=reason,
                retry_in_s=retry_in,
            )
            await defer_delivery(delivery.id, delivery.event_id, endpoint.id, retry_in)
            return True

        settings = get_settings()
        tenant = (
            await session.execute(select(Tenant).where(Tenant.id == endpoint.tenant_id))
        ).scalar_one()
        cap = tenant.max_inflight or settings.default_tenant_max_inflight
        slot_token = await acquire_slot(tenant.id, cap, now)
        if slot_token is None:
            log.info(
                "delivery_gated",
                delivery_id=str(delivery_id),
                endpoint_id=str(endpoint.id),
                reason="max_inflight",
                retry_in_s=CONCURRENCY_RETRY_DELAY_S,
            )
            await defer_delivery(
                delivery.id, delivery.event_id, endpoint.id, CONCURRENCY_RETRY_DELAY_S
            )
            return True

        should_ack = await process_delivery(session, client, delivery_id)

        # Breaker sees every real attempt; gate rejections never reach here.
        await session.refresh(delivery)
        await record_outcome(endpoint.id, delivery.status == "delivered", time.time())

        if endpoint.ordering == "ordered" and delivery.status in {"delivered", "dead"}:
            # Settled: let the queue advance. A 'failed' delivery keeps the head
            # so retries stay in front of everything behind them.
            await pop_head(endpoint.id, delivery.id)
        return should_ack
    finally:
        if slot_token is not None:
            await release_slot(endpoint.tenant_id, slot_token)
        if lock_token is not None:
            await release_lock(endpoint.id, lock_token)


async def handle_message(
    key: str,
    message_id: str,
    fields: dict,
    session_factory: async_sessionmaker[AsyncSession],
    client: httpx.AsyncClient,
) -> None:
    """Process one stream entry, ACKing only after the attempt row commits."""
    delivery_id = uuid.UUID(fields["delivery_id"])
    try:
        async with session_factory() as session:
            should_ack = await deliver_with_gates(session, client, delivery_id)
    except Exception as exc:
        # Leave the entry unACKed: it stays in the PEL and is picked up by
        # reclaim_stale() once it goes idle, rather than being lost.
        log.error(
            "delivery_processing_failed",
            delivery_id=str(delivery_id),
            error=str(exc),
            exc_info=True,
        )
        return
    if should_ack:
        # ACK only after the attempt row is committed — at-least-once.
        await get_redis().xack(key, CONSUMER_GROUP, message_id)


async def reclaim_stale(
    key: str,
    consumer_name: str,
    session_factory: async_sessionmaker[AsyncSession],
    client: httpx.AsyncClient,
) -> int:
    """Take over entries a crashed worker consumed but never ACKed.

    Without this, a worker that dies mid-delivery strands its in-flight entries
    in the pending-entries list forever and the delivery is silently lost.
    """
    redis = get_redis()
    cursor = "0-0"
    reclaimed = 0
    while True:
        cursor, messages, _deleted = await redis.xautoclaim(
            key,
            CONSUMER_GROUP,
            consumer_name,
            min_idle_time=RECLAIM_MIN_IDLE_MS,
            start_id=cursor,
            count=10,
        )
        for message_id, fields in messages:
            log.warning("reclaimed_stale_entry", stream=key, message_id=message_id)
            await handle_message(key, message_id, fields, session_factory, client)
            reclaimed += 1
        if cursor == "0-0" or not messages:
            break
    return reclaimed


async def consume_shard(
    shard: int, consumer_name: str, client: httpx.AsyncClient, stop: asyncio.Event
) -> None:
    """Consume one shard forever, surviving Redis outages.

    Every Redis call is inside the retry envelope: a connection error backs off
    and retries instead of killing the task, because a dead task is invisible —
    the process keeps running and silently stops delivering.
    """
    key = stream_key(shard)
    session_factory = get_session_factory()
    delay = RECONNECT_BASE_DELAY_S
    loop = asyncio.get_running_loop()
    next_reclaim = loop.time()

    while not stop.is_set():
        try:
            if loop.time() >= next_reclaim:
                await reclaim_stale(key, consumer_name, session_factory, client)
                next_reclaim = loop.time() + RECLAIM_INTERVAL_S

            entries = await get_redis().xreadgroup(
                CONSUMER_GROUP, consumer_name, {key: ">"}, count=1, block=BLOCK_MS
            )
            delay = RECONNECT_BASE_DELAY_S  # a successful call resets backoff
            for _stream, messages in entries or []:
                for message_id, fields in messages:
                    await handle_message(key, message_id, fields, session_factory, client)
        except asyncio.CancelledError:
            raise
        except (RedisError, OSError) as exc:
            # Covers restarts, network blips, and NOGROUP after a Redis wipe.
            log.warning(
                "redis_unavailable_retrying",
                shard=shard,
                error=str(exc),
                retry_in_s=delay,
            )
            await asyncio.sleep(delay)
            delay = min(delay * 2, RECONNECT_MAX_DELAY_S)
            # The group may not exist anymore if Redis lost its data.
            with contextlib.suppress(Exception):
                await ensure_group(shard)


async def supervise_shards(
    consumer_name: str, client: httpx.AsyncClient, stop: asyncio.Event
) -> None:
    """Keep one live task per shard, restarting any that dies unexpectedly.

    consume_shard already survives Redis errors, so reaching here means an
    unforeseen bug. Restarting (loudly) beats the alternative: a process that
    reports healthy while delivering nothing.
    """
    settings = get_settings()
    tasks: dict[asyncio.Task, int] = {}

    def spawn(shard: int) -> None:
        tasks[asyncio.create_task(consume_shard(shard, consumer_name, client, stop))] = shard

    for shard in range(settings.stream_shards):
        spawn(shard)

    try:
        while not stop.is_set():
            done, _pending = await asyncio.wait(
                tasks.keys(), timeout=5.0, return_when=asyncio.FIRST_COMPLETED
            )
            for task in done:
                shard = tasks.pop(task)
                if stop.is_set():
                    continue
                exc = task.exception() if not task.cancelled() else None
                log.error(
                    "shard_task_died_restarting",
                    shard=shard,
                    error=str(exc) if exc else "exited without error",
                )
                spawn(shard)
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks.keys(), return_exceptions=True)


async def run_worker() -> None:
    setup_logging()
    settings = get_settings()
    consumer_name = f"worker-{os.getpid()}-{uuid.uuid4().hex[:6]}"

    for shard in range(settings.stream_shards):
        with contextlib.suppress(Exception):
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
        await supervise_shards(consumer_name, client, stop)

    await close_redis()
    await get_engine().dispose()
    log.info("worker_stopped", consumer=consumer_name)


if __name__ == "__main__":
    asyncio.run(run_worker())
