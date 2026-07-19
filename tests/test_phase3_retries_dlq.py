"""Phase 3 integration: retry-then-succeed, and exhaust-then-dead-then-replay.

Requires the full stack (api, worker, retry-scheduler, flaky-endpoint, redis, pg).
These exercise the real backoff schedule, so they are deliberately slow:
attempt 1 retries after ~5s and attempt 2 after ~30s (both jittered).
"""

import asyncio
import uuid

import httpx
import pytest
from sqlalchemy import select

from relay.db.engine import get_session_factory
from relay.db.models import Delivery, DeliveryAttempt
from tests.conftest import FLAKY_URL, requires_infra

pytestmark = requires_infra

ADMIN = {"Authorization": "Bearer change-me"}
FLAKY_HOOK = "http://flaky-endpoint:9000/hook"


async def _configure_flaky(mode: str, secret: str | None = None, param=None) -> None:
    async with httpx.AsyncClient(base_url=FLAKY_URL, timeout=10) as client:
        await client.post("/configure", json={"mode": mode, "param": param})
        if secret is not None:
            await client.post("/secret", json={"secret": secret})


async def _setup(api_client, event_type: str):
    tenant = (
        await api_client.post("/v1/tenants", json={"name": "phase3"}, headers=ADMIN)
    ).json()
    auth = {"Authorization": f"Bearer {tenant['api_key']}"}
    endpoint = (
        await api_client.post(
            "/v1/endpoints",
            json={"url": FLAKY_HOOK, "event_types": [event_type]},
            headers=auth,
        )
    ).json()
    return auth, endpoint


async def _ingest(api_client, auth, event_type: str):
    resp = await api_client.post(
        "/v1/events",
        json={"event_type": event_type, "payload": {"probe": event_type}},
        headers=auth,
    )
    assert resp.status_code == 202
    event_id = uuid.UUID(resp.json()["event_id"])
    async with get_session_factory()() as session:
        delivery = (
            await session.execute(select(Delivery).where(Delivery.event_id == event_id))
        ).scalar_one()
        return event_id, delivery.id


async def _wait_for_status(delivery_id: uuid.UUID, statuses: set[str], wait_s: float):
    loop = asyncio.get_running_loop()
    deadline = loop.time() + wait_s
    last = None
    while loop.time() < deadline:
        async with get_session_factory()() as session:
            delivery = (
                await session.execute(select(Delivery).where(Delivery.id == delivery_id))
            ).scalar_one()
            last = (delivery.status, delivery.attempt_count)
            if delivery.status in statuses:
                return delivery
        await asyncio.sleep(0.5)
    pytest.fail(f"delivery {delivery_id} stuck at {last}, never reached {statuses}")


async def _attempts(delivery_id: uuid.UUID):
    async with get_session_factory()() as session:
        return list(
            (
                await session.execute(
                    select(DeliveryAttempt)
                    .where(DeliveryAttempt.delivery_id == delivery_id)
                    .order_by(DeliveryAttempt.attempt_number)
                )
            ).scalars()
        )


async def test_retry_then_succeed(api_client):
    """http_500 first, then healthy → the delivery eventually succeeds."""
    event_type = f"retry.{uuid.uuid4().hex[:8]}"
    auth, endpoint = await _setup(api_client, event_type)
    await _configure_flaky("http_500", secret=endpoint["signing_secret"])

    _event_id, delivery_id = await _ingest(api_client, auth, event_type)

    # First attempt fails and a retry is scheduled.
    failed = await _wait_for_status(delivery_id, {"failed"}, wait_s=30)
    assert failed.attempt_count == 1
    assert failed.next_attempt_at is not None, "a scheduled retry must record next_attempt_at"

    # Heal the receiver; the scheduler re-enqueues and the retry succeeds.
    await _configure_flaky("healthy")
    delivered = await _wait_for_status(delivery_id, {"delivered"}, wait_s=60)

    assert delivered.attempt_count >= 2
    assert delivered.next_attempt_at is None

    attempts = await _attempts(delivery_id)
    assert len(attempts) >= 2
    assert attempts[0].http_status == 500
    assert attempts[0].error_class == "http_5xx"
    assert attempts[-1].http_status == 200
    assert attempts[-1].error_class is None
    # Attempt numbers are sequential — the audit trail has no gaps.
    assert [a.attempt_number for a in attempts] == list(range(1, len(attempts) + 1))


async def test_exhaust_then_dead_then_replay(api_client):
    """A terminal 4xx dies after 3 attempts, lands in the DLQ, and replays."""
    event_type = f"dlq.{uuid.uuid4().hex[:8]}"
    auth, endpoint = await _setup(api_client, event_type)
    await _configure_flaky("auth_401", secret=endpoint["signing_secret"])

    _event_id, delivery_id = await _ingest(api_client, auth, event_type)

    # 5s + 30s of backoff (jittered) before the third attempt kills it.
    dead = await _wait_for_status(delivery_id, {"dead"}, wait_s=120)
    assert dead.attempt_count == 3, "terminal 4xx must give up after 3 attempts"
    assert dead.next_attempt_at is None

    attempts = await _attempts(delivery_id)
    assert len(attempts) == 3
    assert all(a.http_status == 401 for a in attempts)
    assert all(a.error_class == "http_4xx" for a in attempts)

    # It shows up in the DLQ.
    dlq = (
        await api_client.get(f"/v1/dlq?endpoint_id={endpoint['id']}", headers=auth)
    ).json()
    assert str(delivery_id) in [d["id"] for d in dlq]

    # Fix the receiver, then replay.
    await _configure_flaky("healthy")
    replay = await api_client.post(
        "/v1/dlq/replay", json={"endpoint_id": endpoint["id"]}, headers=auth
    )
    assert replay.status_code == 200
    assert replay.json()["replayed"] == 1
    assert str(delivery_id) in replay.json()["delivery_ids"]

    delivered = await _wait_for_status(delivery_id, {"delivered"}, wait_s=60)
    assert delivered.status == "delivered"
    # Replay resets the counter, so this is attempt 1 of a fresh cycle.
    assert delivered.attempt_count == 1

    # And it's out of the DLQ.
    dlq_after = (
        await api_client.get(f"/v1/dlq?endpoint_id={endpoint['id']}", headers=auth)
    ).json()
    assert str(delivery_id) not in [d["id"] for d in dlq_after]


async def test_attempts_endpoint_exposes_history(api_client):
    event_type = f"hist.{uuid.uuid4().hex[:8]}"
    auth, endpoint = await _setup(api_client, event_type)
    await _configure_flaky("http_500", secret=endpoint["signing_secret"])

    _event_id, delivery_id = await _ingest(api_client, auth, event_type)
    await _wait_for_status(delivery_id, {"failed"}, wait_s=30)

    resp = await api_client.get(f"/v1/deliveries/{delivery_id}/attempts", headers=auth)
    assert resp.status_code == 200
    body = resp.json()
    assert body[0]["http_status"] == 500
    assert body[0]["error_class"] == "http_5xx"
    assert body[0]["attempt_number"] == 1

    await _configure_flaky("healthy")


async def test_dlq_and_replay_are_tenant_scoped(api_client):
    """One tenant must not see or replay another's dead deliveries."""
    event_type = f"iso.{uuid.uuid4().hex[:8]}"
    _auth_a, endpoint_a = await _setup(api_client, event_type)
    tenant_b = (
        await api_client.post("/v1/tenants", json={"name": "phase3-b"}, headers=ADMIN)
    ).json()
    auth_b = {"Authorization": f"Bearer {tenant_b['api_key']}"}

    listed = await api_client.get(f"/v1/dlq?endpoint_id={endpoint_a['id']}", headers=auth_b)
    assert listed.status_code == 404

    replayed = await api_client.post(
        "/v1/dlq/replay", json={"endpoint_id": endpoint_a["id"]}, headers=auth_b
    )
    assert replayed.status_code == 404
