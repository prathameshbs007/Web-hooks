import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from relay.api.auth import require_tenant
from relay.api.errors import ApiError
from relay.api.schemas import DeliveryOut, ReplayRequest, ReplayResult
from relay.db.engine import get_session
from relay.db.models import Delivery, Endpoint, Tenant
from relay.delivery.enqueue import enqueue_delivery
from relay.delivery.retry_queue import remove_from_retry_queue
from relay.observability import get_logger

router = APIRouter(prefix="/v1/dlq", tags=["dlq"])
log = get_logger(__name__)

SessionDep = Annotated[AsyncSession, Depends(get_session)]
TenantDep = Annotated[Tenant, Depends(require_tenant)]


async def _tenant_endpoint_ids(session: AsyncSession, tenant: Tenant) -> list[uuid.UUID]:
    rows = await session.execute(select(Endpoint.id).where(Endpoint.tenant_id == tenant.id))
    return list(rows.scalars())


@router.get("", response_model=list[DeliveryOut])
async def list_dlq(
    session: SessionDep,
    tenant: TenantDep,
    endpoint_id: uuid.UUID | None = None,
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
) -> list[Delivery]:
    """Dead deliveries — attempts exhausted or terminal 4xx."""
    owned = await _tenant_endpoint_ids(session, tenant)
    if endpoint_id is not None:
        if endpoint_id not in owned:
            raise ApiError(404, "not_found", "endpoint not found")
        owned = [endpoint_id]

    rows = await session.execute(
        select(Delivery)
        .where(Delivery.endpoint_id.in_(owned), Delivery.status == "dead")
        .order_by(Delivery.updated_at.desc())
        .limit(limit)
        .offset(offset)
    )
    return list(rows.scalars())


@router.post("/replay", response_model=ReplayResult)
async def replay(body: ReplayRequest, session: SessionDep, tenant: TenantDep) -> ReplayResult:
    """Re-enqueue dead deliveries: attempt_count reset to 0, status 'pending'."""
    owned = await _tenant_endpoint_ids(session, tenant)
    if body.endpoint_id not in owned:
        raise ApiError(404, "not_found", "endpoint not found")

    query = select(Delivery).where(
        Delivery.endpoint_id == body.endpoint_id, Delivery.status == "dead"
    )
    if body.delivery_ids:
        query = query.where(Delivery.id.in_(body.delivery_ids))

    deliveries = list((await session.execute(query)).scalars())
    for delivery in deliveries:
        delivery.status = "pending"
        delivery.attempt_count = 0
        delivery.next_attempt_at = None
    await session.commit()

    replayed = []
    for delivery in deliveries:
        # A dead delivery shouldn't have a scheduled retry, but drop any stale
        # ZSET member so the replay can't be shadowed by an old entry.
        await remove_from_retry_queue(delivery.id, delivery.event_id, delivery.endpoint_id)
        await enqueue_delivery(delivery.id, delivery.event_id, delivery.endpoint_id)
        replayed.append(delivery.id)

    log.info(
        "dlq_replayed",
        tenant_id=str(tenant.id),
        endpoint_id=str(body.endpoint_id),
        count=len(replayed),
    )
    return ReplayResult(replayed=len(replayed), delivery_ids=replayed)
