import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from relay.api.auth import require_tenant
from relay.api.errors import ApiError, not_found
from relay.api.schemas import AttemptOut, DeliveryOut
from relay.db.engine import get_session
from relay.db.models import Delivery, DeliveryAttempt, Endpoint, Tenant

router = APIRouter(prefix="/v1/deliveries", tags=["deliveries"])

SessionDep = Annotated[AsyncSession, Depends(get_session)]
TenantDep = Annotated[Tenant, Depends(require_tenant)]

VALID_STATUSES = {"pending", "delivering", "delivered", "failed", "dead"}


async def _owned_endpoint_ids(session: AsyncSession, tenant: Tenant) -> list[uuid.UUID]:
    rows = await session.execute(select(Endpoint.id).where(Endpoint.tenant_id == tenant.id))
    return list(rows.scalars())


@router.get("", response_model=list[DeliveryOut])
async def list_deliveries(
    session: SessionDep,
    tenant: TenantDep,
    endpoint_id: uuid.UUID | None = None,
    status: str | None = None,
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
) -> list[Delivery]:
    if status is not None and status not in VALID_STATUSES:
        raise ApiError(422, "validation_error", f"status must be one of {sorted(VALID_STATUSES)}")

    owned = await _owned_endpoint_ids(session, tenant)
    if endpoint_id is not None:
        if endpoint_id not in owned:
            raise not_found("endpoint")
        owned = [endpoint_id]

    query = select(Delivery).where(Delivery.endpoint_id.in_(owned))
    if status is not None:
        query = query.where(Delivery.status == status)
    rows = await session.execute(
        query.order_by(Delivery.created_at.desc()).limit(limit).offset(offset)
    )
    return list(rows.scalars())


@router.get("/{delivery_id}/attempts", response_model=list[AttemptOut])
async def list_attempts(
    delivery_id: uuid.UUID, session: SessionDep, tenant: TenantDep
) -> list[DeliveryAttempt]:
    """Full attempt history — the audit trail for a delivery."""
    owned = await _owned_endpoint_ids(session, tenant)
    delivery = (
        await session.execute(
            select(Delivery).where(
                Delivery.id == delivery_id, Delivery.endpoint_id.in_(owned)
            )
        )
    ).scalar_one_or_none()
    if delivery is None:
        raise not_found("delivery")

    rows = await session.execute(
        select(DeliveryAttempt)
        .where(DeliveryAttempt.delivery_id == delivery_id)
        .order_by(DeliveryAttempt.attempt_number)
    )
    return list(rows.scalars())
