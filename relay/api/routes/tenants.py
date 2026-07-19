from typing import Annotated

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from relay.api.auth import generate_api_key, hash_api_key, require_admin
from relay.api.schemas import TenantCreate, TenantCreated
from relay.db.engine import get_session
from relay.db.models import Tenant

router = APIRouter(prefix="/v1/tenants", tags=["tenants"], dependencies=[Depends(require_admin)])


@router.post("", status_code=201, response_model=TenantCreated)
async def create_tenant(
    body: TenantCreate,
    session: Annotated[AsyncSession, Depends(get_session)],
) -> TenantCreated:
    api_key = generate_api_key()
    tenant = Tenant(
        name=body.name,
        api_key_hash=hash_api_key(api_key),
        rate_per_sec=body.rate_per_sec,
        max_inflight=body.max_inflight,
    )
    session.add(tenant)
    await session.commit()
    return TenantCreated(
        id=tenant.id, name=tenant.name, api_key=api_key, created_at=tenant.created_at
    )
