import uuid
from typing import Annotated

from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from relay.api.auth import generate_signing_secret, require_tenant
from relay.api.errors import not_found
from relay.api.schemas import EndpointCreate, EndpointCreated, EndpointOut, EndpointPatch
from relay.db.engine import get_session
from relay.db.models import Endpoint, Tenant

router = APIRouter(prefix="/v1/endpoints", tags=["endpoints"])

SessionDep = Annotated[AsyncSession, Depends(get_session)]
TenantDep = Annotated[Tenant, Depends(require_tenant)]


async def _get_owned(session: AsyncSession, tenant: Tenant, endpoint_id: uuid.UUID) -> Endpoint:
    endpoint = (
        await session.execute(
            select(Endpoint).where(Endpoint.id == endpoint_id, Endpoint.tenant_id == tenant.id)
        )
    ).scalar_one_or_none()
    if endpoint is None:
        raise not_found("endpoint")
    return endpoint


@router.post("", status_code=201, response_model=EndpointCreated)
async def create_endpoint(
    body: EndpointCreate, session: SessionDep, tenant: TenantDep
) -> Endpoint:
    endpoint = Endpoint(
        tenant_id=tenant.id,
        url=body.url,
        signing_secret=generate_signing_secret(),
        ordering=body.ordering,
        event_types=body.event_types,
    )
    session.add(endpoint)
    await session.commit()
    # EndpointCreated includes signing_secret — this is the only response that ever does.
    return endpoint


@router.get("", response_model=list[EndpointOut])
async def list_endpoints(session: SessionDep, tenant: TenantDep) -> list[Endpoint]:
    rows = await session.execute(
        select(Endpoint).where(Endpoint.tenant_id == tenant.id).order_by(Endpoint.created_at)
    )
    return list(rows.scalars())


@router.get("/{endpoint_id}", response_model=EndpointOut)
async def get_endpoint(
    endpoint_id: uuid.UUID, session: SessionDep, tenant: TenantDep
) -> Endpoint:
    return await _get_owned(session, tenant, endpoint_id)


@router.patch("/{endpoint_id}", response_model=EndpointOut)
async def patch_endpoint(
    endpoint_id: uuid.UUID, body: EndpointPatch, session: SessionDep, tenant: TenantDep
) -> Endpoint:
    endpoint = await _get_owned(session, tenant, endpoint_id)
    for field in ("url", "status", "event_types"):
        value = getattr(body, field)
        if value is not None:
            setattr(endpoint, field, value)
    await session.commit()
    return endpoint


@router.delete("/{endpoint_id}", status_code=204)
async def delete_endpoint(
    endpoint_id: uuid.UUID, session: SessionDep, tenant: TenantDep
) -> None:
    # Hard delete; deliveries cascade. Tenants who want to keep history should
    # PATCH status='disabled' instead — DELETE means "forget this endpoint".
    endpoint = await _get_owned(session, tenant, endpoint_id)
    await session.delete(endpoint)
    await session.commit()
