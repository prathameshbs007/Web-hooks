from contextlib import asynccontextmanager

import redis.asyncio as aioredis
from fastapi import FastAPI, Response
from sqlalchemy import text

from relay.config import get_settings
from relay.db.engine import get_engine
from relay.observability import get_logger, setup_logging

log = get_logger(__name__)


async def check_postgres() -> bool:
    try:
        async with get_engine().connect() as conn:
            await conn.execute(text("SELECT 1"))
        return True
    except Exception as exc:
        log.warning("healthz_postgres_failed", error=str(exc))
        return False


async def check_redis() -> bool:
    client = aioredis.from_url(get_settings().redis_url)
    try:
        return bool(await client.ping())
    except Exception as exc:
        log.warning("healthz_redis_failed", error=str(exc))
        return False
    finally:
        await client.aclose()


@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_logging()
    log.info("api_started")
    yield
    await get_engine().dispose()


def create_app() -> FastAPI:
    app = FastAPI(title="Relay", lifespan=lifespan)

    @app.get("/healthz")
    async def healthz(response: Response) -> dict:
        postgres_ok = await check_postgres()
        redis_ok = await check_redis()
        healthy = postgres_ok and redis_ok
        if not healthy:
            response.status_code = 503
        return {
            "status": "ok" if healthy else "degraded",
            "postgres": postgres_ok,
            "redis": redis_ok,
        }

    return app


app = create_app()
