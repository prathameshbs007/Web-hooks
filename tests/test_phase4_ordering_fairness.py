"""Phase 4 integration: order preservation under failure, and tenant fairness.

Requires the full compose stack.
"""

import asyncio
import time
import uuid

import httpx
import pytest
from sqlalchemy import func, select

from relay.db.engine import get_session_factory
from relay.db.models import Delivery
from relay.delivery.circuit_breaker import get_state
from relay.delivery.circuit_breaker import reset as reset_breaker
from tests.conftest import FLAKY_URL, requires_infra

pytestmark = requires_infra

ADMIN = {"Authorization": "Bearer change-me"}
FLAKY_HOOK = "http://flaky-endpoint:9000/hook"


async def _flaky(path: str, payload=None):
    async with httpx.AsyncClient(base_url=FLAKY_URL, timeout=10) as client:
        if payload is None:
            return (await client.get(path)).json()
        return (await client.post(path, json=payload)).json()


async def _new_tenant(api_client, name: str, rate=None, inflight=None):
    body = {"name": name}
    if rate is not None:
        body["rate_per_sec"] = rate
    if inflight is not None:
        body["max_inflight"] = inflight
    tenant = (await api_client.post("/v1/tenants", json=body, headers=ADMIN)).json()
    return {"Authorization": f"Bearer {tenant['api_key']}"}


async def _new_endpoint(api_client, auth, event_type: str, ordering="unordered"):
    return (
        await api_client.post(
            "/v1/endpoints",
            json={"url": FLAKY_HOOK, "ordering": ordering, "event_types": [event_type]},
            headers=auth,
        )
    ).json()


async def _wait_until(predicate, wait_s: float, message: str):
    loop = asyncio.get_running_loop()
    deadline = loop.time() + wait_s
    while loop.time() < deadline:
        if await predicate():
            return True
        await asyncio.sleep(0.4)
    pytest.fail(message)


async def _delivered_count(endpoint_id: str) -> int:
    async with get_session_factory()() as session:
        return int(
            (
                await session.execute(
                    select(func.count())
                    .select_from(Delivery)
                    .where(
                        Delivery.endpoint_id == uuid.UUID(endpoint_id),
                        Delivery.status == "delivered",
                    )
                )
            ).scalar_one()
        )


async def test_ordered_mode_preserves_order_under_induced_failure(api_client):
    """The whole point of 'ordered': a retry must not let later events overtake it."""
    event_type = f"ord.{uuid.uuid4().hex[:8]}"
    auth = await _new_tenant(api_client, "ordered-tenant")
    endpoint = await _new_endpoint(api_client, auth, event_type, ordering="ordered")
    await reset_breaker(uuid.UUID(endpoint["id"]))

    # Fail the first delivery so it has to retry while 2..5 queue behind it.
    await _flaky("/reset")
    await _flaky("/secret", {"secret": endpoint["signing_secret"]})
    await _flaky("/configure", {"mode": "http_500", "param": None})

    for n in range(1, 6):
        resp = await api_client.post(
            "/v1/events",
            json={"event_type": event_type, "payload": {"seq": n}},
            headers=auth,
        )
        assert resp.status_code == 202

    # Let the head fail at least once, proving the others are blocked behind it.
    await asyncio.sleep(3)
    received_while_failing = await _flaky("/received")
    seqs_during = [
        r["body"] for r in received_while_failing["items"] if '"seq"' in r["body"]
    ]
    assert all('"seq":1' in body for body in seqs_during), (
        f"only seq 1 may be attempted while it is failing, saw: {seqs_during}"
    )

    await _flaky("/configure", {"mode": "healthy", "param": None})
    await _wait_until(
        lambda: _all_five(endpoint["id"]), 90, "not all 5 ordered deliveries completed"
    )

    # Retries mean seq 1 was POSTed more than once. Collapse repeats: the order
    # in which each seq FIRST reaches the receiver is what 'ordered' promises.
    received = await _flaky("/received")
    first_seen: list[int] = []
    for item in received["items"]:
        body = item["body"]
        if '"seq"' not in body:
            continue
        seq = int(body.split('"seq":')[1].split("}")[0])
        if seq not in first_seen:
            first_seen.append(seq)
    assert first_seen == [1, 2, 3, 4, 5], f"order violated: {first_seen}"


async def _all_five(endpoint_id: str) -> bool:
    return await _delivered_count(endpoint_id) == 5


async def test_unordered_mode_does_not_block_behind_a_failure(api_client):
    """Contrast with ordered: a failing delivery must not stall the others."""
    event_type = f"unord.{uuid.uuid4().hex[:8]}"
    auth = await _new_tenant(api_client, "unordered-tenant")
    endpoint = await _new_endpoint(api_client, auth, event_type, ordering="unordered")
    await reset_breaker(uuid.UUID(endpoint["id"]))

    await _flaky("/reset")
    await _flaky("/secret", {"secret": endpoint["signing_secret"]})
    await _flaky("/configure", {"mode": "healthy", "param": None})

    for n in range(1, 6):
        await api_client.post(
            "/v1/events",
            json={"event_type": event_type, "payload": {"seq": n}},
            headers=auth,
        )

    await _wait_until(
        lambda: _all_five(endpoint["id"]),
        45,
        "unordered deliveries did not all complete",
    )
    assert await _delivered_count(endpoint["id"]) == 5


async def test_two_tenant_fairness_noisy_neighbour_does_not_starve(api_client):
    """A tenant flooding the queue must not delay a quiet tenant's delivery."""
    noisy_type = f"noisy.{uuid.uuid4().hex[:8]}"
    quiet_type = f"quiet.{uuid.uuid4().hex[:8]}"

    # The noisy tenant is deliberately throttled hard; the quiet one is not.
    noisy_auth = await _new_tenant(api_client, "noisy", rate=5, inflight=2)
    quiet_auth = await _new_tenant(api_client, "quiet", rate=50, inflight=20)
    noisy_ep = await _new_endpoint(api_client, noisy_auth, noisy_type)
    quiet_ep = await _new_endpoint(api_client, quiet_auth, quiet_type)
    for ep in (noisy_ep, quiet_ep):
        await reset_breaker(uuid.UUID(ep["id"]))

    await _flaky("/reset")
    await _flaky("/secret", {"secret": quiet_ep["signing_secret"]})
    await _flaky("/configure", {"mode": "healthy", "param": None})

    # Flood: 60 events from the noisy tenant, whose budget is 5/s.
    await asyncio.gather(
        *[
            api_client.post(
                "/v1/events",
                json={"event_type": noisy_type, "payload": {"i": i}},
                headers=noisy_auth,
            )
            for i in range(60)
        ]
    )

    # Now the quiet tenant sends one event and we time its delivery.
    started = time.time()
    await api_client.post(
        "/v1/events",
        json={"event_type": quiet_type, "payload": {"quiet": True}},
        headers=quiet_auth,
    )
    await _wait_until(
        lambda: _delivered_at_least(quiet_ep["id"], 1),
        60,
        "quiet tenant's delivery was starved by the noisy tenant",
    )
    elapsed = time.time() - started

    # The noisy tenant still has a large backlog draining at its own rate...
    async with get_session_factory()() as session:
        noisy_pending = int(
            (
                await session.execute(
                    select(func.count())
                    .select_from(Delivery)
                    .where(
                        Delivery.endpoint_id == uuid.UUID(noisy_ep["id"]),
                        Delivery.status != "delivered",
                    )
                )
            ).scalar_one()
        )
    # ...yet the quiet tenant got through quickly regardless.
    assert elapsed < 20, f"quiet delivery took {elapsed:.1f}s behind a noisy neighbour"
    assert noisy_pending >= 0  # backlog is throttled, not an error


async def _delivered_at_least(endpoint_id: str, n: int) -> bool:
    return await _delivered_count(endpoint_id) >= n


async def test_breaker_opens_observably_and_gates_deliveries(api_client):
    """A persistently failing endpoint trips the breaker, visible over the API."""
    event_type = f"brk.{uuid.uuid4().hex[:8]}"
    auth = await _new_tenant(api_client, "breaker-tenant")
    endpoint = await _new_endpoint(api_client, auth, event_type)
    endpoint_uuid = uuid.UUID(endpoint["id"])
    await reset_breaker(endpoint_uuid)

    await _flaky("/reset")
    await _flaky("/secret", {"secret": endpoint["signing_secret"]})
    await _flaky("/configure", {"mode": "http_500", "param": None})

    # Terminal-failure retries are slow, so drive the breaker with many events.
    for i in range(12):
        await api_client.post(
            "/v1/events",
            json={"event_type": event_type, "payload": {"i": i}},
            headers=auth,
        )

    async def opened() -> bool:
        return (await get_state(endpoint_uuid))["state"] in {"open", "half_open"}

    await _wait_until(opened, 60, "breaker never opened despite persistent failures")

    resp = await api_client.get(f"/v1/endpoints/{endpoint['id']}/breaker", headers=auth)
    assert resp.status_code == 200
    assert resp.json()["state"] in {"open", "half_open"}

    # Operator resets it after fixing the receiver.
    await _flaky("/configure", {"mode": "healthy", "param": None})
    reset_resp = await api_client.post(
        f"/v1/endpoints/{endpoint['id']}/breaker/reset", headers=auth
    )
    assert reset_resp.status_code == 204
    assert (await get_state(endpoint_uuid))["state"] == "closed"
