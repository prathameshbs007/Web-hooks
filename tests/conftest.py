"""Test configuration.

Integration tests run against the compose stack (postgres/redis exposed on
localhost). Env vars are pointed at localhost BEFORE any relay import so the
cached Settings/engine pick them up.
"""

import os
import socket

# 5433: compose maps postgres there to avoid clashing with a native install.
os.environ["DATABASE_URL"] = "postgresql+asyncpg://relay:relay@localhost:5433/relay"
os.environ["REDIS_URL"] = "redis://localhost:6379/0"
os.environ["ADMIN_TOKEN"] = "change-me"

import httpx  # noqa: E402
import pytest  # noqa: E402

from relay.api.main import create_app  # noqa: E402


def _reachable(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=1):
            return True
    except OSError:
        return False


def infra_available() -> bool:
    return _reachable("localhost", 5433) and _reachable("localhost", 6379)


requires_infra = pytest.mark.skipif(
    not infra_available(),
    reason="integration test: requires `docker compose up` (postgres+redis on localhost)",
)


@pytest.fixture(scope="session")
def migrated():
    """Bring the localhost database to head before integration tests."""
    from alembic.config import Config

    from alembic import command

    command.upgrade(Config("alembic.ini"), "head")


@pytest.fixture
async def api_client(migrated):
    app = create_app()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client
    # pytest-asyncio gives every test its own event loop, but the engine's pool
    # and the shared redis client bind connections to the loop they were created
    # on — drop them so the next test starts clean.
    from relay.db.engine import get_engine
    from relay.delivery.enqueue import close_redis

    await get_engine().dispose()
    await close_redis()
