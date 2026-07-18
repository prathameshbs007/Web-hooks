import hashlib
import hmac
import secrets
from typing import Annotated

from fastapi import Depends, Header
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from relay.api.errors import unauthorized
from relay.config import get_settings
from relay.db.engine import get_session
from relay.db.models import Tenant


def generate_api_key() -> str:
    return "rk_" + secrets.token_urlsafe(32)


def generate_signing_secret() -> str:
    return "whsec_" + secrets.token_urlsafe(24)


def hash_api_key(key: str) -> str:
    # SHA-256 (not bcrypt): API keys are high-entropy random strings, so
    # brute-force is infeasible and a fast hash keeps auth off the CPU profile.
    return hashlib.sha256(key.encode()).hexdigest()


def _bearer_token(authorization: str | None) -> str:
    if not authorization or not authorization.startswith("Bearer "):
        raise unauthorized("expected 'Authorization: Bearer <key>'")
    return authorization.removeprefix("Bearer ")


async def require_tenant(
    session: Annotated[AsyncSession, Depends(get_session)],
    authorization: Annotated[str | None, Header()] = None,
) -> Tenant:
    key_hash = hash_api_key(_bearer_token(authorization))
    tenant = (
        await session.execute(select(Tenant).where(Tenant.api_key_hash == key_hash))
    ).scalar_one_or_none()
    if tenant is None:
        raise unauthorized("invalid API key")
    return tenant


async def require_admin(authorization: Annotated[str | None, Header()] = None) -> None:
    token = _bearer_token(authorization)
    if not hmac.compare_digest(token, get_settings().admin_token):
        raise unauthorized("invalid admin token")
