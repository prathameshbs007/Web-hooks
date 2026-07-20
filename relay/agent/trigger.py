"""Agent trigger: listens for breaker events, enforces budgets, runs diagnoses.

Guardrails (spec Section 8): at most one run per endpoint per hour, ten runs per
day globally, and a hard cost cap per run. Skipped triggers are logged rather
than silently dropped.

Agent failure must never affect delivery: every run is wrapped, and a failure
writes a diagnosis row with root_cause='agent_error' instead of propagating.
"""

import asyncio
import contextlib
import json
import signal
import uuid
from datetime import UTC, datetime

from redis.exceptions import RedisError

from relay.agent.graph import run_diagnosis
from relay.config import get_settings
from relay.db.engine import get_engine, get_session_factory
from relay.delivery.circuit_breaker import BREAKER_EVENTS_CHANNEL
from relay.delivery.enqueue import close_redis, get_redis
from relay.metrics import agent_cost_usd, agent_runs, serve_metrics
from relay.observability import get_logger

log = get_logger(__name__)

AGENT_METRICS_PORT = 9102
RECONNECT_BASE_DELAY_S = 1.0
RECONNECT_MAX_DELAY_S = 15.0

# Budget keys. TTLs do the accounting so there is no sweeper to run.
def endpoint_budget_key(endpoint_id: uuid.UUID) -> str:
    return f"relay:agent:ran:{endpoint_id}"


def daily_budget_key(day: str) -> str:
    return f"relay:agent:daily:{day}"


async def claim_budget(endpoint_id: uuid.UUID) -> tuple[bool, str | None]:
    """Atomically claim a run slot. Returns (allowed, reason_if_denied)."""
    settings = get_settings()
    redis = get_redis()

    # Per-endpoint: SET NX with an hour TTL is the claim; losing it means a run
    # already happened this hour.
    per_hour_seconds = max(1, 3600 // max(1, settings.agent_max_runs_per_endpoint_per_hour))
    claimed = await redis.set(
        endpoint_budget_key(endpoint_id), "1", nx=True, ex=per_hour_seconds
    )
    if not claimed:
        return False, "endpoint_rate_limited"

    day = datetime.now(UTC).strftime("%Y-%m-%d")
    used = await redis.incr(daily_budget_key(day))
    if used == 1:
        await redis.expire(daily_budget_key(day), 86_400 * 2)
    if used > settings.agent_max_runs_per_day:
        # Give the per-endpoint claim back so a legitimate run tomorrow isn't blocked.
        await redis.delete(endpoint_budget_key(endpoint_id))
        return False, "daily_budget_exhausted"
    return True, None


async def diagnose_and_record(endpoint_id: uuid.UUID, triggered_by: str) -> uuid.UUID | None:
    """Run the agent and persist the result. Never raises."""
    session_factory = get_session_factory()
    try:
        # run_diagnosis persists the diagnosis row itself (a stub up front, then
        # the final result). We only observe the outcome here.
        async with session_factory() as session:
            state = await run_diagnosis(session, endpoint_id, triggered_by)
        result = state.result or {}
        root_cause = result.get("root_cause", "agent_error")

        agent_runs.labels(outcome=root_cause).inc()
        agent_cost_usd.inc(state.cost_usd)
        log.info(
            "agent_run_complete",
            endpoint_id=str(endpoint_id),
            diagnosis_id=str(state.diagnosis_id),
            root_cause=root_cause,
            confidence=result.get("confidence", "low"),
            verified=state.verified,
            cost_usd=round(state.cost_usd, 4),
        )
        return state.diagnosis_id
    except Exception as exc:
        # The delivery pipeline must not care that the agent broke. run_diagnosis
        # commits an 'agent_error' stub before the graph runs, so a mid-run
        # failure already leaves an honest record — nothing more to write here.
        log.error("agent_run_failed", endpoint_id=str(endpoint_id), error=str(exc), exc_info=True)
        agent_runs.labels(outcome="agent_error").inc()
        return None


async def handle_breaker_event(payload: str) -> None:
    """React to the two diagnosis triggers on the channel (spec §8)."""
    try:
        event = json.loads(payload)
    except json.JSONDecodeError:
        log.warning("agent_bad_breaker_event", payload=payload[:200])
        return

    if event.get("to_state") == "open":
        triggered_by = "breaker_open"
    elif event.get("trigger") == "consecutive_failures":
        triggered_by = "consecutive_failures"
    else:
        return  # breaker closing/half-opening, or an unrelated event

    endpoint_id = uuid.UUID(event["endpoint_id"])
    # Budget guard also dedupes the two triggers: if 5-failures already ran a
    # diagnosis this hour, the breaker opening at 10 is skipped as rate-limited.
    allowed, reason = await claim_budget(endpoint_id)
    if not allowed:
        log.info(
            "agent_trigger_skipped",
            endpoint_id=str(endpoint_id),
            reason=reason,
            triggered_by=triggered_by,
        )
        agent_runs.labels(outcome=f"skipped_{reason}").inc()
        return
    await diagnose_and_record(endpoint_id, triggered_by)


async def run_trigger(stop: asyncio.Event | None = None) -> None:
    from relay.observability import setup_logging

    setup_logging()
    stop = stop or asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop.set)

    serve_metrics(AGENT_METRICS_PORT)
    log.info("agent_trigger_started", channel=BREAKER_EVENTS_CHANNEL)
    delay = RECONNECT_BASE_DELAY_S

    while not stop.is_set():
        pubsub = None
        try:
            pubsub = get_redis().pubsub()
            await pubsub.subscribe(BREAKER_EVENTS_CHANNEL)
            delay = RECONNECT_BASE_DELAY_S
            while not stop.is_set():
                message = await pubsub.get_message(
                    ignore_subscribe_messages=True, timeout=2.0
                )
                if message and message.get("type") == "message":
                    await handle_breaker_event(message["data"])
        except asyncio.CancelledError:
            raise
        except (RedisError, OSError) as exc:
            log.warning("redis_unavailable_retrying", error=str(exc), retry_in_s=delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, RECONNECT_MAX_DELAY_S)
        finally:
            if pubsub is not None:
                with contextlib.suppress(Exception):
                    await pubsub.aclose()

    await close_redis()
    await get_engine().dispose()
    log.info("agent_trigger_stopped")


if __name__ == "__main__":
    asyncio.run(run_trigger())
