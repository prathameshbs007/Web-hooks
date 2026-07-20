import uuid
from datetime import UTC, datetime
from typing import Annotated

from fastapi import APIRouter, Depends, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from relay.api.auth import require_tenant
from relay.api.errors import ApiError, not_found
from relay.api.schemas import ActionOut, DiagnosisOut
from relay.db.engine import get_session
from relay.db.models import AgentAction, Delivery, Diagnosis, Endpoint, Tenant
from relay.delivery.circuit_breaker import reset as reset_breaker
from relay.delivery.enqueue import enqueue_delivery
from relay.observability import get_logger

router = APIRouter(prefix="/v1/diagnoses", tags=["diagnoses"])
log = get_logger(__name__)

SessionDep = Annotated[AsyncSession, Depends(get_session)]
TenantDep = Annotated[Tenant, Depends(require_tenant)]


async def _owned_endpoint_ids(session: AsyncSession, tenant: Tenant) -> list[uuid.UUID]:
    rows = await session.execute(select(Endpoint.id).where(Endpoint.tenant_id == tenant.id))
    return list(rows.scalars())


async def _get_owned(session: AsyncSession, tenant: Tenant, diagnosis_id: uuid.UUID) -> Diagnosis:
    owned = await _owned_endpoint_ids(session, tenant)
    diagnosis = (
        await session.execute(
            select(Diagnosis).where(
                Diagnosis.id == diagnosis_id, Diagnosis.endpoint_id.in_(owned)
            )
        )
    ).scalar_one_or_none()
    if diagnosis is None:
        raise not_found("diagnosis")
    return diagnosis


@router.get("", response_model=list[DiagnosisOut])
async def list_diagnoses(
    session: SessionDep,
    tenant: TenantDep,
    endpoint_id: uuid.UUID | None = None,
    status: str | None = None,
    limit: int = Query(50, ge=1, le=200),
) -> list[Diagnosis]:
    owned = await _owned_endpoint_ids(session, tenant)
    if endpoint_id is not None:
        if endpoint_id not in owned:
            raise not_found("endpoint")
        owned = [endpoint_id]
    query = select(Diagnosis).where(Diagnosis.endpoint_id.in_(owned))
    if status is not None:
        if status not in {"open", "acknowledged", "resolved"}:
            raise ApiError(422, "validation_error", "invalid status")
        query = query.where(Diagnosis.status == status)
    rows = await session.execute(query.order_by(Diagnosis.created_at.desc()).limit(limit))
    return list(rows.scalars())


@router.get("/{diagnosis_id}", response_model=DiagnosisOut)
async def get_diagnosis(
    diagnosis_id: uuid.UUID, session: SessionDep, tenant: TenantDep
) -> Diagnosis:
    return await _get_owned(session, tenant, diagnosis_id)


@router.post("/{diagnosis_id}/ack", response_model=DiagnosisOut)
async def acknowledge(
    diagnosis_id: uuid.UUID, session: SessionDep, tenant: TenantDep
) -> Diagnosis:
    diagnosis = await _get_owned(session, tenant, diagnosis_id)
    diagnosis.status = "acknowledged"
    await session.commit()
    return diagnosis


@router.get("/{diagnosis_id}/actions", response_model=list[ActionOut])
async def list_actions(
    diagnosis_id: uuid.UUID, session: SessionDep, tenant: TenantDep
) -> list[AgentAction]:
    await _get_owned(session, tenant, diagnosis_id)
    rows = await session.execute(
        select(AgentAction)
        .where(AgentAction.diagnosis_id == diagnosis_id)
        .order_by(AgentAction.created_at)
    )
    return list(rows.scalars())


@router.post("/actions/{action_id}/approve", response_model=ActionOut)
async def approve_action(
    action_id: uuid.UUID, session: SessionDep, tenant: TenantDep
) -> AgentAction:
    """Execute a mutating action the agent proposed. This is the only path that runs it."""
    owned = await _owned_endpoint_ids(session, tenant)
    action = (
        await session.execute(
            select(AgentAction).where(
                AgentAction.id == action_id, AgentAction.endpoint_id.in_(owned)
            )
        )
    ).scalar_one_or_none()
    if action is None:
        raise not_found("action")
    if action.status != "pending":
        raise ApiError(409, "already_decided", f"action is already {action.status}")

    endpoint = (
        await session.execute(select(Endpoint).where(Endpoint.id == action.endpoint_id))
    ).scalar_one()

    if action.action == "pause_endpoint":
        endpoint.status = "paused"
        replayed = 0
    else:  # replay_dlq
        dead = (
            (
                await session.execute(
                    select(Delivery).where(
                        Delivery.endpoint_id == endpoint.id, Delivery.status == "dead"
                    )
                )
            )
            .scalars()
            .all()
        )
        for delivery in dead:
            delivery.status = "pending"
            delivery.attempt_count = 0
            delivery.next_attempt_at = None
        replayed = len(dead)

    action.status = "approved"
    action.decided_at = datetime.now(UTC)
    await session.commit()

    if action.action == "replay_dlq" and replayed:
        await reset_breaker(endpoint.id)
        for delivery in dead:
            await enqueue_delivery(delivery.id, delivery.event_id, delivery.endpoint_id)

    log.info(
        "agent_action_approved",
        action=action.action,
        action_id=str(action.id),
        endpoint_id=str(endpoint.id),
        replayed=replayed,
    )
    return action


@router.post("/actions/{action_id}/reject", response_model=ActionOut)
async def reject_action(
    action_id: uuid.UUID, session: SessionDep, tenant: TenantDep
) -> AgentAction:
    owned = await _owned_endpoint_ids(session, tenant)
    action = (
        await session.execute(
            select(AgentAction).where(
                AgentAction.id == action_id, AgentAction.endpoint_id.in_(owned)
            )
        )
    ).scalar_one_or_none()
    if action is None:
        raise not_found("action")
    if action.status != "pending":
        raise ApiError(409, "already_decided", f"action is already {action.status}")
    action.status = "rejected"
    action.decided_at = datetime.now(UTC)
    await session.commit()
    return action
