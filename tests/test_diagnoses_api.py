"""Diagnoses API + the human-approval flow for mutating agent actions."""

import uuid

from sqlalchemy import select

from relay.db.engine import get_session_factory
from relay.db.models import AgentAction, Delivery, Diagnosis, Endpoint, Event
from tests.conftest import requires_infra

pytestmark = requires_infra

ADMIN = {"Authorization": "Bearer change-me"}


async def _tenant_and_endpoint(api_client):
    tenant = (await api_client.post("/v1/tenants", json={"name": "diag"}, headers=ADMIN)).json()
    auth = {"Authorization": f"Bearer {tenant['api_key']}"}
    endpoint = (
        await api_client.post(
            "/v1/endpoints", json={"url": "http://flaky-endpoint:9000/hook"}, headers=auth
        )
    ).json()
    return auth, endpoint


async def _insert_diagnosis(endpoint_id: str, root_cause="endpoint_down") -> uuid.UUID:
    async with get_session_factory()() as session:
        diagnosis = Diagnosis(
            endpoint_id=uuid.UUID(endpoint_id),
            triggered_by="breaker_open",
            root_cause=root_cause,
            confidence="high",
            evidence={"tool_calls": ["query_attempts", "probe_endpoint"], "verified": True},
            recommendation="Fix the receiver.",
            draft_email="We observed persistent 5xx responses from your endpoint.",
        )
        session.add(diagnosis)
        await session.commit()
        return diagnosis.id


async def test_list_and_get_diagnoses(api_client):
    auth, endpoint = await _tenant_and_endpoint(api_client)
    diag_id = await _insert_diagnosis(endpoint["id"])

    listed = await api_client.get(f"/v1/diagnoses?endpoint_id={endpoint['id']}", headers=auth)
    assert listed.status_code == 200
    assert str(diag_id) in [d["id"] for d in listed.json()]

    got = await api_client.get(f"/v1/diagnoses/{diag_id}", headers=auth)
    assert got.status_code == 200
    body = got.json()
    assert body["root_cause"] == "endpoint_down"
    assert body["confidence"] == "high"
    assert body["draft_email"]


async def test_acknowledge_diagnosis(api_client):
    auth, endpoint = await _tenant_and_endpoint(api_client)
    diag_id = await _insert_diagnosis(endpoint["id"])

    acked = await api_client.post(f"/v1/diagnoses/{diag_id}/ack", headers=auth)
    assert acked.status_code == 200
    assert acked.json()["status"] == "acknowledged"


async def test_diagnoses_are_tenant_scoped(api_client):
    _auth_a, endpoint_a = await _tenant_and_endpoint(api_client)
    diag_id = await _insert_diagnosis(endpoint_a["id"])
    tenant_b = (await api_client.post("/v1/tenants", json={"name": "b"}, headers=ADMIN)).json()
    auth_b = {"Authorization": f"Bearer {tenant_b['api_key']}"}

    assert (await api_client.get(f"/v1/diagnoses/{diag_id}", headers=auth_b)).status_code == 404
    assert (
        await api_client.post(f"/v1/diagnoses/{diag_id}/ack", headers=auth_b)
    ).status_code == 404


async def _pending_action(diagnosis_id: uuid.UUID, endpoint_id: str, action: str) -> uuid.UUID:
    async with get_session_factory()() as session:
        pending = AgentAction(
            diagnosis_id=diagnosis_id,
            endpoint_id=uuid.UUID(endpoint_id),
            action=action,
            reason="agent proposed this",
        )
        session.add(pending)
        await session.commit()
        return pending.id


async def test_pause_action_only_pauses_after_approval(api_client):
    auth, endpoint = await _tenant_and_endpoint(api_client)
    diag_id = await _insert_diagnosis(endpoint["id"])
    action_id = await _pending_action(diag_id, endpoint["id"], "pause_endpoint")

    # Before approval the endpoint is untouched.
    before = await api_client.get(f"/v1/endpoints/{endpoint['id']}", headers=auth)
    assert before.json()["status"] == "active"

    approved = await api_client.post(f"/v1/diagnoses/actions/{action_id}/approve", headers=auth)
    assert approved.status_code == 200
    assert approved.json()["status"] == "approved"

    after = await api_client.get(f"/v1/endpoints/{endpoint['id']}", headers=auth)
    assert after.json()["status"] == "paused", "approval must actually pause the endpoint"


async def test_rejecting_an_action_does_not_apply_it(api_client):
    auth, endpoint = await _tenant_and_endpoint(api_client)
    diag_id = await _insert_diagnosis(endpoint["id"])
    action_id = await _pending_action(diag_id, endpoint["id"], "pause_endpoint")

    rejected = await api_client.post(f"/v1/diagnoses/actions/{action_id}/reject", headers=auth)
    assert rejected.status_code == 200
    assert rejected.json()["status"] == "rejected"

    endpoint_after = await api_client.get(f"/v1/endpoints/{endpoint['id']}", headers=auth)
    assert endpoint_after.json()["status"] == "active", "a rejected pause must not apply"


async def test_action_cannot_be_decided_twice(api_client):
    auth, endpoint = await _tenant_and_endpoint(api_client)
    diag_id = await _insert_diagnosis(endpoint["id"])
    action_id = await _pending_action(diag_id, endpoint["id"], "pause_endpoint")

    await api_client.post(f"/v1/diagnoses/actions/{action_id}/approve", headers=auth)
    second = await api_client.post(f"/v1/diagnoses/actions/{action_id}/approve", headers=auth)
    assert second.status_code == 409
    assert second.json()["error"]["code"] == "already_decided"


async def test_replay_dlq_action_requeues_dead_deliveries(api_client):
    auth, endpoint = await _tenant_and_endpoint(api_client)
    diag_id = await _insert_diagnosis(endpoint["id"])

    # Seed a dead delivery for the endpoint.
    async with get_session_factory()() as session:
        tenant_id = (
            await session.execute(
                select(Endpoint.tenant_id).where(Endpoint.id == uuid.UUID(endpoint["id"]))
            )
        ).scalar_one()
        event = Event(tenant_id=tenant_id, event_type="t", payload={})
        session.add(event)
        await session.flush()
        dead = Delivery(
            event_id=event.id,
            endpoint_id=uuid.UUID(endpoint["id"]),
            status="dead",
            attempt_count=7,
        )
        session.add(dead)
        await session.commit()
        dead_id = dead.id

    action_id = await _pending_action(diag_id, endpoint["id"], "replay_dlq")
    approved = await api_client.post(f"/v1/diagnoses/actions/{action_id}/approve", headers=auth)
    assert approved.status_code == 200

    async with get_session_factory()() as session:
        delivery = (
            await session.execute(select(Delivery).where(Delivery.id == dead_id))
        ).scalar_one()
    assert delivery.status == "pending", "approved replay must resurrect the dead delivery"
    assert delivery.attempt_count == 0


async def test_action_approval_is_tenant_scoped(api_client):
    _auth_a, endpoint_a = await _tenant_and_endpoint(api_client)
    diag_id = await _insert_diagnosis(endpoint_a["id"])
    action_id = await _pending_action(diag_id, endpoint_a["id"], "pause_endpoint")
    tenant_b = (await api_client.post("/v1/tenants", json={"name": "b2"}, headers=ADMIN)).json()
    auth_b = {"Authorization": f"Bearer {tenant_b['api_key']}"}

    assert (
        await api_client.post(f"/v1/diagnoses/actions/{action_id}/approve", headers=auth_b)
    ).status_code == 404
