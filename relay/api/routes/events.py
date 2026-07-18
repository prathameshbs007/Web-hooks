import uuid
from typing import Annotated

from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from relay.api.auth import require_tenant
from relay.api.errors import ApiError, not_found
from relay.api.schemas import DeliveryStatusOut, EventAccepted, EventCreate, EventOut
from relay.db.engine import get_session
from relay.db.models import Delivery, Endpoint, Event, Tenant
from relay.delivery.enqueue import enqueue_delivery
from relay.observability import get_logger

router = APIRouter(prefix="/v1/events", tags=["events"])
log = get_logger(__name__)

SessionDep = Annotated[AsyncSession, Depends(get_session)]
TenantDep = Annotated[Tenant, Depends(require_tenant)]


async def _existing_by_idempotency_key(
    session: AsyncSession, tenant_id: uuid.UUID, key: str
) -> Event | None:
    return (
        await session.execute(
            select(Event).where(Event.tenant_id == tenant_id, Event.idempotency_key == key)
        )
    ).scalar_one_or_none()


@router.post("", status_code=202, response_model=EventAccepted)
async def ingest_event(
    body: EventCreate, session: SessionDep, tenant: TenantDep
) -> EventAccepted:
    """Validate → persist (idempotently) → fan out → XADD → 202.

    Ingestion never blocks on delivery: the only work here is one transaction
    and one XADD per matching endpoint.
    """
    if body.idempotency_key is not None:
        existing = await _existing_by_idempotency_key(session, tenant.id, body.idempotency_key)
        if existing is not None:
            return EventAccepted(event_id=existing.id)

    # Explicit id: delivery rows reference it before the ORM flush would
    # otherwise assign the default.
    event = Event(
        id=uuid.uuid4(),
        tenant_id=tenant.id,
        event_type=body.event_type,
        payload=body.payload,
        idempotency_key=body.idempotency_key,
    )
    session.add(event)

    # Fan out: one delivery per active endpoint subscribed to this event type
    # (empty event_types = subscribed to all).
    endpoints = (
        (
            await session.execute(
                select(Endpoint).where(
                    Endpoint.tenant_id == tenant.id,
                    Endpoint.status == "active",
                )
            )
        )
        .scalars()
        .all()
    )
    matching = [e for e in endpoints if not e.event_types or body.event_type in e.event_types]
    deliveries = [Delivery(event_id=event.id, endpoint_id=e.id) for e in matching]
    session.add_all(deliveries)

    try:
        await session.commit()
    except IntegrityError:
        # Lost a race on (tenant_id, idempotency_key): another request inserted
        # the same event between our check and commit. Return its event_id.
        await session.rollback()
        assert body.idempotency_key is not None
        existing = await _existing_by_idempotency_key(session, tenant.id, body.idempotency_key)
        if existing is None:  # pragma: no cover - constraint violated but row missing
            raise ApiError(500, "internal", "idempotency conflict could not be resolved") from None
        return EventAccepted(event_id=existing.id)

    # Enqueue only after commit so a worker can never see a delivery_id that
    # isn't in Postgres yet. If XADD fails the rows stay 'pending' and are
    # picked up by the recovery sweep (Phase 3 scheduler).
    for delivery in deliveries:
        await enqueue_delivery(delivery.id, event.id, delivery.endpoint_id)

    log.info(
        "event_ingested",
        event_id=str(event.id),
        tenant_id=str(tenant.id),
        deliveries=len(deliveries),
    )
    return EventAccepted(event_id=event.id)


@router.get("/{event_id}", response_model=EventOut)
async def get_event(event_id: uuid.UUID, session: SessionDep, tenant: TenantDep) -> EventOut:
    event = (
        await session.execute(
            select(Event).where(Event.id == event_id, Event.tenant_id == tenant.id)
        )
    ).scalar_one_or_none()
    if event is None:
        raise not_found("event")
    deliveries = (
        (await session.execute(select(Delivery).where(Delivery.event_id == event.id)))
        .scalars()
        .all()
    )
    return EventOut(
        id=event.id,
        event_type=event.event_type,
        payload=event.payload,
        idempotency_key=event.idempotency_key,
        created_at=event.created_at,
        deliveries=[
            DeliveryStatusOut(
                delivery_id=d.id,
                endpoint_id=d.endpoint_id,
                status=d.status,
                attempt_count=d.attempt_count,
            )
            for d in deliveries
        ],
    )
