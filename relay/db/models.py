import uuid
from datetime import datetime
from decimal import Decimal

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Identity,
    Index,
    Integer,
    Numeric,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Declarative base for all Relay models. Tables are added phase by phase."""


class Tenant(Base):
    __tablename__ = "tenants"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(Text)
    # Only a SHA-256 hash of the API key is stored; the plaintext key is shown once.
    api_key_hash: Mapped[str] = mapped_column(Text, unique=True)
    # NULL = fall back to DEFAULT_TENANT_* settings.
    rate_per_sec: Mapped[int | None] = mapped_column(Integer, nullable=True)
    max_inflight: Mapped[int | None] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class Endpoint(Base):
    __tablename__ = "endpoints"
    __table_args__ = (
        CheckConstraint("ordering IN ('ordered','unordered')", name="ck_endpoints_ordering"),
        CheckConstraint("status IN ('active','paused','disabled')", name="ck_endpoints_status"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("tenants.id", ondelete="CASCADE")
    )
    url: Mapped[str] = mapped_column(Text)
    signing_secret: Mapped[str] = mapped_column(Text)
    ordering: Mapped[str] = mapped_column(Text, server_default=text("'unordered'"))
    status: Mapped[str] = mapped_column(Text, server_default=text("'active'"))
    # Empty array = subscribed to all event types.
    event_types: Mapped[list[str]] = mapped_column(ARRAY(Text), server_default=text("'{}'"))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class Event(Base):
    __tablename__ = "events"
    __table_args__ = (
        # Postgres treats NULLs as distinct, so events without an idempotency
        # key are never deduplicated — exactly the semantics we want.
        UniqueConstraint("tenant_id", "idempotency_key", name="uq_events_tenant_idempotency"),
        Index("ix_events_tenant_created", "tenant_id", "created_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("tenants.id", ondelete="CASCADE")
    )
    event_type: Mapped[str] = mapped_column(Text)
    payload: Mapped[dict] = mapped_column(JSONB)
    idempotency_key: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class Delivery(Base):
    """One row per (event, endpoint) fan-out target."""

    __tablename__ = "deliveries"
    __table_args__ = (
        CheckConstraint(
            "status IN ('pending','delivering','delivered','failed','dead')",
            name="ck_deliveries_status",
        ),
        Index("ix_deliveries_endpoint_status", "endpoint_id", "status"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    event_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("events.id", ondelete="CASCADE")
    )
    endpoint_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("endpoints.id", ondelete="CASCADE")
    )
    status: Mapped[str] = mapped_column(Text, server_default=text("'pending'"))
    attempt_count: Mapped[int] = mapped_column(Integer, server_default=text("0"))
    next_attempt_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )


class Diagnosis(Base):
    """One agent run's conclusion about why an endpoint is failing."""

    __tablename__ = "diagnoses"
    __table_args__ = (
        CheckConstraint("confidence IN ('low','medium','high')", name="ck_diagnoses_confidence"),
        CheckConstraint(
            "status IN ('open','acknowledged','resolved')", name="ck_diagnoses_status"
        ),
        Index("ix_diagnoses_endpoint_created", "endpoint_id", "created_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    endpoint_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("endpoints.id", ondelete="CASCADE")
    )
    triggered_by: Mapped[str] = mapped_column(Text)
    root_cause: Mapped[str] = mapped_column(Text)
    confidence: Mapped[str] = mapped_column(Text)
    evidence: Mapped[dict] = mapped_column(JSONB)
    recommendation: Mapped[str] = mapped_column(Text)
    draft_email: Mapped[str] = mapped_column(Text)
    status: Mapped[str] = mapped_column(Text, server_default=text("'open'"))
    cost_usd: Mapped[Decimal] = mapped_column(Numeric(10, 6), server_default=text("0"))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class AgentAction(Base):
    """A mutating action the agent proposed. Never executed without approval."""

    __tablename__ = "agent_actions"
    __table_args__ = (
        CheckConstraint(
            "action IN ('pause_endpoint','replay_dlq')", name="ck_agent_actions_action"
        ),
        CheckConstraint(
            "status IN ('pending','approved','rejected')", name="ck_agent_actions_status"
        ),
        Index("ix_agent_actions_status", "status"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    diagnosis_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("diagnoses.id", ondelete="CASCADE")
    )
    endpoint_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("endpoints.id", ondelete="CASCADE")
    )
    action: Mapped[str] = mapped_column(Text)
    reason: Mapped[str] = mapped_column(Text)
    status: Mapped[str] = mapped_column(Text, server_default=text("'pending'"))
    decided_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class DeliveryAttempt(Base):
    """Immutable record of one HTTP attempt. Never updated, only inserted."""

    __tablename__ = "delivery_attempts"
    __table_args__ = (
        Index("ix_delivery_attempts_delivery_number", "delivery_id", "attempt_number"),
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    delivery_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("deliveries.id", ondelete="CASCADE")
    )
    attempt_number: Mapped[int] = mapped_column(Integer)
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    latency_ms: Mapped[int] = mapped_column(Integer)
    http_status: Mapped[int | None] = mapped_column(Integer, nullable=True)
    error_class: Mapped[str | None] = mapped_column(Text, nullable=True)
    # Truncated to 1 KB: enough to diagnose, bounded so a chatty receiver
    # can't bloat the table.
    response_snippet: Mapped[str | None] = mapped_column(Text, nullable=True)
