"""Agent tools (spec Section 8).

Read-only tools run immediately. The two mutating tools never act — they record
a pending row for a human to approve, and tell the model they did so. That is
the whole guardrail: the agent cannot pause an endpoint or replay a DLQ on its
own, no matter what it concludes or what a malicious payload tells it.
"""

import socket
import ssl
import statistics
import time
import uuid
from datetime import UTC, datetime, timedelta
from urllib.parse import urlparse

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from relay.db.models import AgentAction, Delivery, DeliveryAttempt, Endpoint
from relay.delivery.circuit_breaker import get_state
from relay.delivery.sender import send_delivery
from relay.observability import get_logger

log = get_logger(__name__)

PROBE_TIMEOUT_S = 10
SNIPPET_SAMPLES = 3

# Schemas handed to the model. Descriptions are prescriptive about *when* to
# call each tool — that measurably improves tool selection.
TOOL_SCHEMAS = [
    {
        "name": "query_attempts",
        "description": (
            "Aggregated delivery-attempt history for the endpoint: counts by error class "
            "and HTTP status, latency percentiles, when failures started, and sample "
            "response snippets. Call this FIRST on every investigation — it is the cheapest "
            "way to see the failure shape before probing anything."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "window_minutes": {
                    "type": "integer",
                    "description": "How far back to aggregate. Default 60.",
                }
            },
            "required": [],
        },
    },
    {
        "name": "get_endpoint_config",
        "description": (
            "The endpoint's URL, ordering mode, status, and current circuit-breaker state. "
            "Call this when you need to know whether the endpoint is paused/disabled or the "
            "breaker is open, or to see the URL before checking DNS/TLS."
        ),
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "probe_endpoint",
        "description": (
            "Send a real signed test delivery to the endpoint right now and report status, "
            "latency, and error. Marked with \"relay_probe\": true so the receiver can ignore "
            "it. Call this to VERIFY a hypothesis against live behavior — a diagnosis that "
            "was never tested against a probe or a DNS/TLS check is downgraded to low "
            "confidence."
        ),
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "check_dns_tls",
        "description": (
            "Resolve the endpoint's hostname and inspect its TLS certificate: expiry, "
            "hostname match, chain validity. Call this when attempts show dns or tls error "
            "classes, or when connections fail before any HTTP status is returned."
        ),
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "pause_endpoint",
        "description": (
            "MUTATING — REQUIRES HUMAN APPROVAL. Does not pause anything. Records a "
            "pending request for an operator to approve. Call this only when the endpoint "
            "is persistently broken and continued delivery attempts are pure waste."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "reason": {
                    "type": "string",
                    "description": "Why pausing is warranted, for the operator reviewing it.",
                }
            },
            "required": ["reason"],
        },
    },
    {
        "name": "replay_dlq",
        "description": (
            "MUTATING — REQUIRES HUMAN APPROVAL. Does not replay anything. Records a "
            "pending request for an operator to approve. Call this only when the "
            "underlying fault is resolved and dead deliveries should be retried."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "reason": {
                    "type": "string",
                    "description": "Why replaying is safe now, for the operator reviewing it.",
                }
            },
            "required": ["reason"],
        },
    },
]

MUTATING_TOOLS = {"pause_endpoint", "replay_dlq"}
VERIFICATION_TOOLS = {"probe_endpoint", "check_dns_tls"}


async def query_attempts(
    session: AsyncSession, endpoint_id: uuid.UUID, window_minutes: int = 60
) -> dict:
    since = datetime.now(UTC) - timedelta(minutes=window_minutes)
    rows = (
        (
            await session.execute(
                select(DeliveryAttempt)
                .join(Delivery, Delivery.id == DeliveryAttempt.delivery_id)
                .where(Delivery.endpoint_id == endpoint_id, DeliveryAttempt.started_at >= since)
                .order_by(DeliveryAttempt.started_at)
            )
        )
        .scalars()
        .all()
    )
    if not rows:
        return {"total_attempts": 0, "note": "no attempts recorded in this window"}

    by_error: dict[str, int] = {}
    by_status: dict[str, int] = {}
    latencies: list[int] = []
    snippets: list[str] = []
    for attempt in rows:
        key = attempt.error_class or "success"
        by_error[key] = by_error.get(key, 0) + 1
        status = str(attempt.http_status) if attempt.http_status else "none"
        by_status[status] = by_status.get(status, 0) + 1
        latencies.append(attempt.latency_ms)
        if attempt.error_class and attempt.response_snippet and len(snippets) < SNIPPET_SAMPLES:
            snippets.append(attempt.response_snippet[:200])

    failures = [a for a in rows if a.error_class]
    ordered = sorted(latencies)
    return {
        "total_attempts": len(rows),
        "failed_attempts": len(failures),
        "failure_rate": round(len(failures) / len(rows), 3),
        "counts_by_error_class": by_error,
        "counts_by_http_status": by_status,
        "latency_ms": {
            "p50": ordered[len(ordered) // 2],
            "p95": ordered[min(len(ordered) - 1, int(len(ordered) * 0.95))],
            "max": ordered[-1],
            "mean": round(statistics.mean(ordered), 1),
        },
        "first_failed_at": failures[0].started_at.isoformat() if failures else None,
        "last_failed_at": failures[-1].started_at.isoformat() if failures else None,
        "sample_response_snippets": snippets,
    }


async def get_endpoint_config(session: AsyncSession, endpoint_id: uuid.UUID) -> dict:
    endpoint = (
        await session.execute(select(Endpoint).where(Endpoint.id == endpoint_id))
    ).scalar_one()
    dead = (
        (
            await session.execute(
                select(Delivery).where(
                    Delivery.endpoint_id == endpoint_id, Delivery.status == "dead"
                )
            )
        )
        .scalars()
        .all()
    )
    return {
        "url": endpoint.url,
        "ordering": endpoint.ordering,
        "status": endpoint.status,
        "event_types": list(endpoint.event_types),
        "created_at": endpoint.created_at.isoformat(),
        "breaker": await get_state(endpoint_id),
        "dead_letter_count": len(dead),
    }


async def probe_endpoint(session: AsyncSession, endpoint_id: uuid.UUID) -> dict:
    """Send one real signed delivery. The only tool that touches the network."""
    endpoint = (
        await session.execute(select(Endpoint).where(Endpoint.id == endpoint_id))
    ).scalar_one()
    payload = {
        "relay_probe": True,
        "note": "synthetic diagnostic delivery from Relay; safe to ignore",
        "at": datetime.now(UTC).isoformat(),
    }
    async with httpx.AsyncClient(timeout=httpx.Timeout(PROBE_TIMEOUT_S)) as client:
        result = await send_delivery(
            client,
            url=endpoint.url,
            secret=endpoint.signing_secret,
            delivery_id=uuid.uuid4(),
            event_id=uuid.uuid4(),
            payload=payload,
        )
    return {
        "probed_url": endpoint.url,
        "http_status": result.http_status,
        "latency_ms": result.latency_ms,
        "error_class": result.error_class,
        "succeeded": result.succeeded,
        "response_snippet": (result.response_snippet or "")[:300],
    }


async def check_dns_tls(session: AsyncSession, endpoint_id: uuid.UUID) -> dict:
    endpoint = (
        await session.execute(select(Endpoint).where(Endpoint.id == endpoint_id))
    ).scalar_one()
    parsed = urlparse(endpoint.url)
    host = parsed.hostname or ""
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    out: dict = {"host": host, "port": port, "scheme": parsed.scheme}

    try:
        addresses = sorted({info[4][0] for info in socket.getaddrinfo(host, port)})
        out["dns_resolved"] = True
        out["addresses"] = addresses
    except socket.gaierror as exc:
        # DNS failure is terminal for this check — nothing to connect to.
        return out | {"dns_resolved": False, "dns_error": str(exc)}

    if parsed.scheme != "https":
        return out | {"tls": "not applicable (plain http)"}

    try:
        context = ssl.create_default_context()
        with socket.create_connection((host, port), timeout=PROBE_TIMEOUT_S) as sock:
            with context.wrap_socket(sock, server_hostname=host) as tls_sock:
                cert = tls_sock.getpeercert()
        not_after = datetime.strptime(cert["notAfter"], "%b %d %H:%M:%S %Y %Z").replace(tzinfo=UTC)
        out["tls"] = {
            "valid": True,
            "hostname_matches": True,  # wrap_socket would have raised otherwise
            "expires_at": not_after.isoformat(),
            "days_until_expiry": (not_after - datetime.now(UTC)).days,
            "issuer": dict(x[0] for x in cert.get("issuer", ())).get("organizationName"),
        }
    except ssl.SSLCertVerificationError as exc:
        out["tls"] = {"valid": False, "verification_error": str(exc)}
    except Exception as exc:
        out["tls"] = {"valid": False, "error": str(exc)}
    return out


async def request_mutation(
    session: AsyncSession,
    endpoint_id: uuid.UUID,
    diagnosis_id: uuid.UUID,
    action: str,
    reason: str,
) -> dict:
    """Record a mutating action as pending. Nothing is executed here."""
    pending = AgentAction(
        diagnosis_id=diagnosis_id, endpoint_id=endpoint_id, action=action, reason=reason
    )
    session.add(pending)
    await session.commit()
    log.info(
        "agent_action_pending",
        action=action,
        endpoint_id=str(endpoint_id),
        action_id=str(pending.id),
    )
    return {
        "status": "pending_human_approval",
        "action_id": str(pending.id),
        "action": action,
        "note": (
            "Recorded for operator review. Nothing has been changed. Do not assume "
            "this action has taken effect."
        ),
    }


async def dispatch(
    name: str,
    tool_input: dict,
    session: AsyncSession,
    endpoint_id: uuid.UUID,
    diagnosis_id: uuid.UUID,
) -> dict:
    """Route a tool call to its implementation, converting errors into results."""
    started = time.perf_counter()
    try:
        if name == "query_attempts":
            result = await query_attempts(
                session, endpoint_id, int(tool_input.get("window_minutes", 60))
            )
        elif name == "get_endpoint_config":
            result = await get_endpoint_config(session, endpoint_id)
        elif name == "probe_endpoint":
            result = await probe_endpoint(session, endpoint_id)
        elif name == "check_dns_tls":
            result = await check_dns_tls(session, endpoint_id)
        elif name in MUTATING_TOOLS:
            result = await request_mutation(
                session, endpoint_id, diagnosis_id, name, tool_input.get("reason", "")
            )
        else:
            return {"error": f"unknown tool: {name}"}
    except Exception as exc:
        # A failing tool must not kill the run — the model can work around it.
        log.warning("agent_tool_failed", tool=name, error=str(exc))
        return {"error": f"{type(exc).__name__}: {exc}"}
    log.info("agent_tool_called", tool=name, ms=int((time.perf_counter() - started) * 1000))
    return result
