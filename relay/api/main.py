from contextlib import asynccontextmanager

import redis.asyncio as aioredis
from fastapi import FastAPI, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from sqlalchemy import text
from starlette.exceptions import HTTPException as StarletteHTTPException

from relay.api.errors import ApiError
from relay.api.routes import deliveries, dlq, endpoints, events, tenants
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


def _error_response(status_code: int, code: str, message: str) -> JSONResponse:
    return JSONResponse(
        status_code=status_code, content={"error": {"code": code, "message": message}}
    )


def create_app() -> FastAPI:
    app = FastAPI(title="Relay", lifespan=lifespan)

    app.include_router(tenants.router)
    app.include_router(endpoints.router)
    app.include_router(events.router)
    app.include_router(deliveries.router)
    app.include_router(dlq.router)

    @app.exception_handler(ApiError)
    async def api_error_handler(request: Request, exc: ApiError) -> JSONResponse:
        return _error_response(exc.status_code, exc.code, exc.message)

    @app.exception_handler(RequestValidationError)
    async def validation_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
        return _error_response(422, "validation_error", str(exc.errors()[:3]))

    @app.exception_handler(StarletteHTTPException)
    async def http_exception_handler(
        request: Request, exc: StarletteHTTPException
    ) -> JSONResponse:
        return _error_response(exc.status_code, "http_error", str(exc.detail))

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
