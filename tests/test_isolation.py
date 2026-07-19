"""Token bucket, concurrency caps, and breaker state machine. Requires Redis."""

import asyncio
import time
import uuid

from relay.delivery.circuit_breaker import (
    CONSECUTIVE_FAILURE_THRESHOLD,
    HALF_OPEN_PROBE_INTERVAL_SECONDS,
    WINDOW_MIN_ATTEMPTS,
    allow_request,
    get_state,
    record_outcome,
    reset,
    should_auto_disable,
)
from relay.delivery.enqueue import get_redis
from relay.delivery.rate_limit import (
    SLOT_TTL_SECONDS,
    acquire_slot,
    bucket_key,
    inflight_count,
    inflight_key,
    release_slot,
    take_token,
)
from tests.conftest import requires_infra

pytestmark = requires_infra


# --- token bucket ---


async def test_bucket_allows_up_to_rate_then_denies():
    tenant = uuid.uuid4()
    await get_redis().delete(bucket_key(tenant))
    now = time.time()

    granted = [await take_token(tenant, 5, now) for _ in range(5)]
    assert all(granted), "a full bucket must grant its whole burst"
    assert await take_token(tenant, 5, now) is False, "6th in the same instant must be denied"


async def test_bucket_refills_over_time():
    tenant = uuid.uuid4()
    await get_redis().delete(bucket_key(tenant))
    now = time.time()
    for _ in range(10):
        await take_token(tenant, 10, now)
    assert await take_token(tenant, 10, now) is False

    # One second later, a 10/s bucket has refilled completely.
    assert await take_token(tenant, 10, now + 1.0) is True


async def test_bucket_does_not_exceed_burst_when_idle():
    """A long-idle tenant must not bank unlimited tokens."""
    tenant = uuid.uuid4()
    await get_redis().delete(bucket_key(tenant))
    now = time.time()
    await take_token(tenant, 3, now)
    # An hour idle: capped at burst (=rate), so only 3 are available.
    later = now + 3600
    assert [await take_token(tenant, 3, later) for _ in range(3)] == [True, True, True]
    assert await take_token(tenant, 3, later) is False


async def test_buckets_are_isolated_per_tenant():
    a, b = uuid.uuid4(), uuid.uuid4()
    now = time.time()
    for key in (bucket_key(a), bucket_key(b)):
        await get_redis().delete(key)

    while await take_token(a, 2, now):
        pass
    # A exhausted its budget; B must be untouched.
    assert await take_token(b, 2, now) is True


async def test_concurrent_token_takes_do_not_oversubscribe():
    """Lua atomicity: N parallel takes on a 5-token bucket grant exactly 5."""
    tenant = uuid.uuid4()
    await get_redis().delete(bucket_key(tenant))
    now = time.time()
    results = await asyncio.gather(*[take_token(tenant, 5, now) for _ in range(40)])
    assert sum(results) == 5


# --- concurrency cap ---


async def test_slots_cap_in_flight_deliveries():
    tenant = uuid.uuid4()
    await get_redis().delete(inflight_key(tenant))
    now = time.time()

    tokens = [await acquire_slot(tenant, 3, now) for _ in range(3)]
    assert all(tokens)
    assert await acquire_slot(tenant, 3, now) is None, "must refuse beyond the cap"

    await release_slot(tenant, tokens[0])
    assert await acquire_slot(tenant, 3, now) is not None, "released slot must be reusable"


async def test_concurrent_slot_acquisition_respects_cap():
    tenant = uuid.uuid4()
    await get_redis().delete(inflight_key(tenant))
    now = time.time()
    results = await asyncio.gather(*[acquire_slot(tenant, 4, now) for _ in range(30)])
    assert sum(1 for r in results if r is not None) == 4


async def test_stale_slots_expire_so_a_dead_worker_cannot_leak_capacity():
    tenant = uuid.uuid4()
    await get_redis().delete(inflight_key(tenant))
    now = time.time()
    for _ in range(3):
        await acquire_slot(tenant, 3, now)
    assert await acquire_slot(tenant, 3, now) is None

    # Simulate time passing beyond the slot TTL without any release.
    later = now + SLOT_TTL_SECONDS + 1
    assert await inflight_count(tenant, later) == 0
    assert await acquire_slot(tenant, 3, later) is not None


# --- circuit breaker ---


async def test_breaker_opens_after_consecutive_failures():
    endpoint = uuid.uuid4()
    await reset(endpoint)
    now = time.time()

    for i in range(CONSECUTIVE_FAILURE_THRESHOLD - 1):
        result = await record_outcome(endpoint, success=False, now=now + i)
        assert result["state"] == "closed", f"opened too early at failure {i + 1}"

    result = await record_outcome(endpoint, success=False, now=now + 10)
    assert result["state"] == "open"
    assert result["transitioned"] is True


async def test_success_resets_consecutive_counter():
    endpoint = uuid.uuid4()
    await reset(endpoint)
    now = time.time()
    for i in range(CONSECUTIVE_FAILURE_THRESHOLD - 1):
        await record_outcome(endpoint, success=False, now=now + i)
    await record_outcome(endpoint, success=True, now=now + 20)

    result = await record_outcome(endpoint, success=False, now=now + 21)
    assert result["state"] == "closed", "a success must break the consecutive streak"
    assert result["consecutive"] == 1


async def test_breaker_opens_on_failure_rate_in_window():
    """>50% failures over >=20 attempts trips it even without a long streak."""
    endpoint = uuid.uuid4()
    await reset(endpoint)
    now = time.time()

    # Two failures per success: ~67% failure rate, and the consecutive counter
    # never reaches 10. Stop as soon as it trips — once open, the gate stops
    # further attempts, so continuing to record outcomes would not be realistic.
    opened_at_attempt = None
    for i in range(WINDOW_MIN_ATTEMPTS + 10):
        result = await record_outcome(endpoint, success=(i % 3 == 0), now=now + i * 0.1)
        assert result["consecutive"] < CONSECUTIVE_FAILURE_THRESHOLD, "streak, not rate, tripped it"
        if result["state"] == "open":
            opened_at_attempt = i + 1
            break

    assert opened_at_attempt is not None, "failure rate over the window must open the breaker"
    # It must not trip before the minimum-attempts floor.
    assert opened_at_attempt >= WINDOW_MIN_ATTEMPTS


async def test_closed_breaker_allows_requests():
    endpoint = uuid.uuid4()
    await reset(endpoint)
    decision, state = await allow_request(endpoint, time.time())
    assert (decision, state) == ("allow", "closed")


async def test_open_breaker_denies_then_half_opens_for_one_probe():
    endpoint = uuid.uuid4()
    await reset(endpoint)
    now = time.time()
    for i in range(CONSECUTIVE_FAILURE_THRESHOLD):
        await record_outcome(endpoint, success=False, now=now + i)

    decision, state = await allow_request(endpoint, now + 11)
    assert decision == "deny" and state == "open"

    # After the probe interval, exactly one caller is promoted to half_open.
    probe_time = now + HALF_OPEN_PROBE_INTERVAL_SECONDS + 20
    decision, state = await allow_request(endpoint, probe_time)
    assert decision == "probe" and state == "half_open"

    # A second concurrent worker must not also get through.
    decision2, _ = await allow_request(endpoint, probe_time + 1)
    assert decision2 == "deny"


async def test_half_open_success_closes_and_failure_reopens():
    endpoint = uuid.uuid4()
    await reset(endpoint)
    now = time.time()
    for i in range(CONSECUTIVE_FAILURE_THRESHOLD):
        await record_outcome(endpoint, success=False, now=now + i)
    probe_time = now + HALF_OPEN_PROBE_INTERVAL_SECONDS + 20
    await allow_request(endpoint, probe_time)

    failed = await record_outcome(endpoint, success=False, now=probe_time + 1)
    assert failed["state"] == "open", "a failed probe must reopen the breaker"

    probe2 = probe_time + HALF_OPEN_PROBE_INTERVAL_SECONDS + 20
    await allow_request(endpoint, probe2)
    recovered = await record_outcome(endpoint, success=True, now=probe2 + 1)
    assert recovered["state"] == "closed"
    assert (await allow_request(endpoint, probe2 + 2))[0] == "allow"


async def test_auto_disable_only_after_72h_open():
    endpoint = uuid.uuid4()
    await reset(endpoint)
    now = time.time()
    for i in range(CONSECUTIVE_FAILURE_THRESHOLD):
        await record_outcome(endpoint, success=False, now=now + i)

    assert await should_auto_disable(endpoint, now + 3600) is False
    assert await should_auto_disable(endpoint, now + 71 * 3600) is False
    assert await should_auto_disable(endpoint, now + 73 * 3600) is True


async def test_breaker_transitions_publish_events():
    """Transitions are emitted on relay:breaker-events for the agent trigger."""
    endpoint = uuid.uuid4()
    await reset(endpoint)
    pubsub = get_redis().pubsub()
    await pubsub.subscribe("relay:breaker-events")
    await asyncio.sleep(0.2)

    now = time.time()
    for i in range(CONSECUTIVE_FAILURE_THRESHOLD):
        await record_outcome(endpoint, success=False, now=now + i)

    received = []
    deadline = asyncio.get_running_loop().time() + 5
    while asyncio.get_running_loop().time() < deadline:
        msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
        if msg:
            received.append(msg["data"])
            break
    await pubsub.unsubscribe("relay:breaker-events")
    await pubsub.aclose()

    assert received, "opening the breaker must publish an event"
    import json

    event = json.loads(received[0])
    assert event["endpoint_id"] == str(endpoint)
    assert event["to_state"] == "open"
    assert event["from_state"] == "closed"


async def test_breaker_state_is_per_endpoint():
    a, b = uuid.uuid4(), uuid.uuid4()
    await reset(a)
    await reset(b)
    now = time.time()
    for i in range(CONSECUTIVE_FAILURE_THRESHOLD):
        await record_outcome(a, success=False, now=now + i)

    assert (await get_state(a))["state"] == "open"
    assert (await get_state(b))["state"] == "closed"
    assert (await allow_request(b, now + 11))[0] == "allow"
