"""Phase 1 integration tests: tenancy, auth, endpoints CRUD, ingestion + fan-out.

Require the compose stack (postgres + redis on localhost).
"""

import uuid

import redis.asyncio as aioredis

from relay.config import get_settings
from relay.delivery.enqueue import shard_for, stream_key
from tests.conftest import requires_infra

pytestmark = requires_infra

ADMIN = {"Authorization": "Bearer change-me"}


async def _create_tenant(client, name="acme"):
    resp = await client.post("/v1/tenants", json={"name": name}, headers=ADMIN)
    assert resp.status_code == 201, resp.text
    return resp.json()


async def _auth(client):
    tenant = await _create_tenant(client)
    return {"Authorization": f"Bearer {tenant['api_key']}"}


async def _stream_len(endpoint_id: str) -> int:
    client = aioredis.from_url(get_settings().redis_url)
    try:
        return await client.xlen(stream_key(shard_for(uuid.UUID(endpoint_id))))
    finally:
        await client.aclose()


async def test_tenant_create_requires_admin_token(api_client):
    resp = await api_client.post(
        "/v1/tenants", json={"name": "x"}, headers={"Authorization": "Bearer wrong"}
    )
    assert resp.status_code == 401
    assert resp.json()["error"]["code"] == "unauthorized"


async def test_tenant_endpoints_require_api_key(api_client):
    resp = await api_client.get("/v1/endpoints")
    assert resp.status_code == 401
    resp = await api_client.post(
        "/v1/events",
        json={"event_type": "t", "payload": {}},
        headers={"Authorization": "Bearer rk_bogus"},
    )
    assert resp.status_code == 401


async def test_endpoint_crud_and_secret_shown_once(api_client):
    auth = await _auth(api_client)

    created = await api_client.post(
        "/v1/endpoints",
        json={"url": "http://example.com/hook", "event_types": ["order.paid"]},
        headers=auth,
    )
    assert created.status_code == 201
    body = created.json()
    assert body["signing_secret"].startswith("whsec_")
    endpoint_id = body["id"]

    # list + get never expose the secret again
    listed = await api_client.get("/v1/endpoints", headers=auth)
    assert listed.status_code == 200
    assert "signing_secret" not in listed.json()[0]
    got = await api_client.get(f"/v1/endpoints/{endpoint_id}", headers=auth)
    assert "signing_secret" not in got.json()

    patched = await api_client.patch(
        f"/v1/endpoints/{endpoint_id}", json={"status": "paused"}, headers=auth
    )
    assert patched.json()["status"] == "paused"

    deleted = await api_client.delete(f"/v1/endpoints/{endpoint_id}", headers=auth)
    assert deleted.status_code == 204
    assert (await api_client.get(f"/v1/endpoints/{endpoint_id}", headers=auth)).status_code == 404


async def test_tenant_cannot_see_other_tenants_endpoint(api_client):
    auth_a = await _auth(api_client)
    auth_b = await _auth(api_client)
    created = await api_client.post(
        "/v1/endpoints", json={"url": "http://a.example/hook"}, headers=auth_a
    )
    endpoint_id = created.json()["id"]
    assert (
        await api_client.get(f"/v1/endpoints/{endpoint_id}", headers=auth_b)
    ).status_code == 404


async def test_ingest_fans_out_and_enqueues(api_client):
    auth = await _auth(api_client)
    ep = (
        await api_client.post(
            "/v1/endpoints", json={"url": "http://example.com/hook"}, headers=auth
        )
    ).json()

    before = await _stream_len(ep["id"])
    resp = await api_client.post(
        "/v1/events",
        json={"event_type": "order.paid", "payload": {"amount": 42}},
        headers=auth,
    )
    assert resp.status_code == 202
    event_id = resp.json()["event_id"]

    event = (await api_client.get(f"/v1/events/{event_id}", headers=auth)).json()
    assert event["event_type"] == "order.paid"
    assert len(event["deliveries"]) == 1
    assert event["deliveries"][0]["status"] == "pending"
    assert event["deliveries"][0]["endpoint_id"] == ep["id"]

    assert await _stream_len(ep["id"]) == before + 1


async def test_event_type_filtering_and_paused_excluded(api_client):
    auth = await _auth(api_client)
    subscribed = (
        await api_client.post(
            "/v1/endpoints",
            json={"url": "http://example.com/a", "event_types": ["order.paid"]},
            headers=auth,
        )
    ).json()
    other_type = (
        await api_client.post(
            "/v1/endpoints",
            json={"url": "http://example.com/b", "event_types": ["user.created"]},
            headers=auth,
        )
    ).json()
    paused = (
        await api_client.post(
            "/v1/endpoints", json={"url": "http://example.com/c"}, headers=auth
        )
    ).json()
    await api_client.patch(
        f"/v1/endpoints/{paused['id']}", json={"status": "paused"}, headers=auth
    )

    resp = await api_client.post(
        "/v1/events", json={"event_type": "order.paid", "payload": {}}, headers=auth
    )
    event = (await api_client.get(f"/v1/events/{resp.json()['event_id']}", headers=auth)).json()
    target_ids = {d["endpoint_id"] for d in event["deliveries"]}
    assert target_ids == {subscribed["id"]}
    assert other_type["id"] not in target_ids
    assert paused["id"] not in target_ids


async def test_duplicate_idempotency_key_returns_same_event_no_refanout(api_client):
    auth = await _auth(api_client)
    ep = (
        await api_client.post(
            "/v1/endpoints", json={"url": "http://example.com/hook"}, headers=auth
        )
    ).json()
    key = f"idem-{uuid.uuid4()}"

    first = await api_client.post(
        "/v1/events",
        json={"event_type": "t", "payload": {"n": 1}, "idempotency_key": key},
        headers=auth,
    )
    assert first.status_code == 202
    stream_after_first = await _stream_len(ep["id"])

    second = await api_client.post(
        "/v1/events",
        json={"event_type": "t", "payload": {"n": 1}, "idempotency_key": key},
        headers=auth,
    )
    assert second.status_code == 202
    assert second.json()["event_id"] == first.json()["event_id"]

    # no second fan-out: delivery count and stream length unchanged
    event = (
        await api_client.get(f"/v1/events/{first.json()['event_id']}", headers=auth)
    ).json()
    assert len(event["deliveries"]) == 1
    assert await _stream_len(ep["id"]) == stream_after_first


async def test_idempotency_scoped_per_tenant(api_client):
    auth_a = await _auth(api_client)
    auth_b = await _auth(api_client)
    key = f"idem-{uuid.uuid4()}"
    body = {"event_type": "t", "payload": {}, "idempotency_key": key}
    a = await api_client.post("/v1/events", json=body, headers=auth_a)
    b = await api_client.post("/v1/events", json=body, headers=auth_b)
    assert a.json()["event_id"] != b.json()["event_id"]


async def test_error_shape(api_client):
    auth = await _auth(api_client)
    resp = await api_client.get(f"/v1/events/{uuid.uuid4()}", headers=auth)
    assert resp.status_code == 404
    err = resp.json()["error"]
    assert err["code"] == "not_found"
    assert "message" in err
