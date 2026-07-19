"""Phase 2 E2E: event → worker container → flaky endpoint receives a signed POST.

Requires the full compose stack (postgres, redis, api, worker, flaky-endpoint).
Deliveries are performed by the real `worker` service, so these tests exercise
the deployed path rather than an in-process imitation. Endpoint URLs therefore
use the compose DNS name; assertions read flaky's state over the host port.
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
# As seen by the worker container (compose service DNS), not by the test host.
# The /{mode} suffix pins behavior per request, so a slow or failing endpoint in
# one test cannot change how another test's endpoint behaves.
FLAKY_BASE = "http://flaky-endpoint:9000/hook"


def hook_url(mode: str) -> str:
    return f"{FLAKY_BASE}/{mode}"


async def _set_secret(secret: str) -> None:
    async with httpx.AsyncClient(base_url=FLAKY_URL, timeout=10) as client:
        await client.post("/secret", json={"secret": secret})


async def _flaky_items() -> list[dict]:
    async with httpx.AsyncClient(base_url=FLAKY_URL, timeout=10) as client:
        return (await client.get("/received")).json()["items"]


async def _setup_tenant_endpoint(api_client, mode: str = "healthy") -> tuple[dict, dict]:
    tenant = (
        await api_client.post("/v1/tenants", json={"name": "phase2"}, headers=ADMIN)
    ).json()
    auth = {"Authorization": f"Bearer {tenant['api_key']}"}
    endpoint = (
        await api_client.post("/v1/endpoints", json={"url": hook_url(mode)}, headers=auth)
    ).json()
    await _set_secret(endpoint["signing_secret"])
    return auth, endpoint


async def _ingest(api_client, auth, payload=None):
    resp = await api_client.post(
        "/v1/events",
        json={"event_type": "order.paid", "payload": payload or {"amount": 42}},
        headers=auth,
    )
    assert resp.status_code == 202
    event_id = uuid.UUID(resp.json()["event_id"])
    async with get_session_factory()() as session:
        delivery = (
            await session.execute(select(Delivery).where(Delivery.event_id == event_id))
        ).scalar_one()
        return event_id, delivery.id, delivery.endpoint_id


async def _wait_for_status(delivery_id: uuid.UUID, statuses: set[str], wait_s: float = 30.0):
    loop = asyncio.get_running_loop()
    deadline = loop.time() + wait_s
    last = None
    while loop.time() < deadline:
        async with get_session_factory()() as session:
            delivery = (
                await session.execute(select(Delivery).where(Delivery.id == delivery_id))
            ).scalar_one()
            last = delivery.status
            if delivery.status in statuses:
                return delivery
        await asyncio.sleep(0.25)
    pytest.fail(f"delivery {delivery_id} stuck in '{last}', never reached {statuses}")


async def _attempts(delivery_id: uuid.UUID) -> list[DeliveryAttempt]:
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


async def test_e2e_healthy_endpoint_receives_valid_signature(api_client):
    auth, _endpoint = await _setup_tenant_endpoint(api_client, "healthy")

    event_id, delivery_id, _ = await _ingest(api_client, auth, {"amount": 42})
    delivery = await _wait_for_status(delivery_id, {"delivered"})

    assert delivery.status == "delivered"
    assert delivery.attempt_count == 1

    # The receiver independently verified the HMAC signature.
    matching = [r for r in await _flaky_items() if r["delivery_id"] == str(delivery_id)]
    assert len(matching) == 1
    assert matching[0]["signature_valid"] is True
    assert matching[0]["event_id"] == str(event_id)
    assert '"amount":42' in matching[0]["body"]

    attempts = await _attempts(delivery_id)
    assert len(attempts) == 1
    assert attempts[0].attempt_number == 1
    assert attempts[0].http_status == 200
    assert attempts[0].error_class is None
    assert attempts[0].latency_ms >= 0


async def test_e2e_failing_endpoint_records_failed_attempt(api_client):
    auth, _endpoint = await _setup_tenant_endpoint(api_client, "http_500")

    _event_id, delivery_id, _ = await _ingest(api_client, auth)
    await _wait_for_status(delivery_id, {"failed"})

    attempts = await _attempts(delivery_id)
    assert len(attempts) == 1
    assert attempts[0].http_status == 500
    assert attempts[0].error_class == "http_5xx"
    assert "internal error" in attempts[0].response_snippet


async def test_ack_after_commit_means_exactly_one_attempt(api_client):
    """The entry is ACKed only after the attempt commits, so it isn't reprocessed."""
    auth, _endpoint = await _setup_tenant_endpoint(api_client, "healthy")

    _event_id, delivery_id, _ = await _ingest(api_client, auth)
    await _wait_for_status(delivery_id, {"delivered"})

    def _mine(items):
        # Scope to this delivery: the receiver is shared with other tests.
        return [i for i in items if i["delivery_id"] == str(delivery_id)]

    sent_first = len(_mine(await _flaky_items()))
    # Give the worker time to redeliver if the ACK had been mishandled.
    await asyncio.sleep(3)
    assert len(_mine(await _flaky_items())) == sent_first == 1
    assert len(await _attempts(delivery_id)) == 1


async def test_duplicate_stream_entry_does_not_resend(api_client):
    """At-least-once means duplicates happen; the worker must be idempotent."""
    from relay.delivery.enqueue import enqueue_delivery

    auth, _endpoint = await _setup_tenant_endpoint(api_client, "healthy")

    event_id, delivery_id, endpoint_id = await _ingest(api_client, auth)
    await _wait_for_status(delivery_id, {"delivered"})

    def _mine(items):
        # Count only this delivery: other tests share the receiver.
        return [i for i in items if i["delivery_id"] == str(delivery_id)]

    sent_first = len(_mine(await _flaky_items()))
    assert sent_first == 1

    await enqueue_delivery(delivery_id, event_id, endpoint_id)
    await asyncio.sleep(4)

    # Already 'delivered' → the worker drops the duplicate without re-sending.
    assert len(_mine(await _flaky_items())) == sent_first
    assert len(await _attempts(delivery_id)) == 1


async def test_timeout_is_recorded_as_timeout_error_class(api_client):
    """10s httpx timeout: a 30s receiver sleep must classify as 'timeout'."""
    auth, _endpoint = await _setup_tenant_endpoint(api_client, "timeout")

    _event_id, delivery_id, _ = await _ingest(api_client, auth)
    await _wait_for_status(delivery_id, {"failed"}, wait_s=40.0)

    attempts = await _attempts(delivery_id)
    assert attempts[0].error_class == "timeout"
    assert attempts[0].http_status is None
    # Proves the configured 10s cap fired rather than the receiver's 30s sleep.
    assert 9_000 <= attempts[0].latency_ms <= 15_000
