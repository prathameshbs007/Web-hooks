import uuid
from datetime import datetime
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, Field

# --- tenants ---


class TenantCreate(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    # Omit to use DEFAULT_TENANT_RATE_PER_SEC / DEFAULT_TENANT_MAX_INFLIGHT.
    rate_per_sec: int | None = Field(default=None, ge=1, le=10_000)
    max_inflight: int | None = Field(default=None, ge=1, le=1_000)


class TenantCreated(BaseModel):
    id: uuid.UUID
    name: str
    api_key: str  # plaintext, returned exactly once
    created_at: datetime


# --- endpoints ---


class EndpointCreate(BaseModel):
    url: str = Field(pattern=r"^https?://", max_length=2000)
    ordering: Literal["ordered", "unordered"] = "unordered"
    event_types: list[str] = []


class BreakerState(BaseModel):
    state: str
    consecutive: int
    opened_at: float | None = None
    last_probe_at: float | None = None


class EndpointOut(BaseModel):
    id: uuid.UUID
    url: str
    ordering: str
    status: str
    event_types: list[str]
    created_at: datetime


class EndpointCreated(EndpointOut):
    signing_secret: str  # plaintext, returned exactly once


class EndpointPatch(BaseModel):
    url: str | None = Field(default=None, pattern=r"^https?://", max_length=2000)
    status: Literal["active", "paused", "disabled"] | None = None
    event_types: list[str] | None = None


# --- events ---


class EventCreate(BaseModel):
    event_type: str = Field(min_length=1, max_length=200)
    payload: dict
    idempotency_key: str | None = Field(default=None, max_length=200)


class EventAccepted(BaseModel):
    event_id: uuid.UUID


class DeliveryStatusOut(BaseModel):
    delivery_id: uuid.UUID
    endpoint_id: uuid.UUID
    status: str
    attempt_count: int


class DeliveryOut(BaseModel):
    id: uuid.UUID
    event_id: uuid.UUID
    endpoint_id: uuid.UUID
    status: str
    attempt_count: int
    next_attempt_at: datetime | None
    created_at: datetime
    updated_at: datetime


class AttemptOut(BaseModel):
    attempt_number: int
    started_at: datetime
    latency_ms: int
    http_status: int | None
    error_class: str | None
    response_snippet: str | None


class DiagnosisOut(BaseModel):
    id: uuid.UUID
    endpoint_id: uuid.UUID
    triggered_by: str
    root_cause: str
    confidence: str
    evidence: dict
    recommendation: str
    draft_email: str
    status: str
    cost_usd: Decimal
    created_at: datetime


class ActionOut(BaseModel):
    id: uuid.UUID
    diagnosis_id: uuid.UUID
    endpoint_id: uuid.UUID
    action: str
    reason: str
    status: str
    decided_at: datetime | None
    created_at: datetime


class ReplayRequest(BaseModel):
    endpoint_id: uuid.UUID
    # Omit to replay every dead delivery for the endpoint.
    delivery_ids: list[uuid.UUID] | None = None


class ReplayResult(BaseModel):
    replayed: int
    delivery_ids: list[uuid.UUID]


class EventOut(BaseModel):
    id: uuid.UUID
    event_type: str
    payload: dict
    idempotency_key: str | None
    created_at: datetime
    deliveries: list[DeliveryStatusOut]
