import httpx
import pytest

from relay.api import main as api_main


@pytest.fixture
def client():
    app = api_main.create_app()
    transport = httpx.ASGITransport(app=app)
    return httpx.AsyncClient(transport=transport, base_url="http://test")


async def _ok() -> bool:
    return True


async def _down() -> bool:
    return False


async def test_healthz_ok(client, monkeypatch):
    monkeypatch.setattr(api_main, "check_postgres", _ok)
    monkeypatch.setattr(api_main, "check_redis", _ok)
    async with client:
        resp = await client.get("/healthz")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok", "postgres": True, "redis": True}


async def test_healthz_degraded_when_redis_down(client, monkeypatch):
    monkeypatch.setattr(api_main, "check_postgres", _ok)
    monkeypatch.setattr(api_main, "check_redis", _down)
    async with client:
        resp = await client.get("/healthz")
    assert resp.status_code == 503
    body = resp.json()
    assert body["status"] == "degraded"
    assert body["redis"] is False
