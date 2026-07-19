import json
import ssl
import time
import uuid
from dataclasses import dataclass

import httpx

from relay.signing import compute_signature

RESPONSE_SNIPPET_LIMIT = 1024


@dataclass
class AttemptResult:
    latency_ms: int
    http_status: int | None
    error_class: str | None
    response_snippet: str | None

    @property
    def succeeded(self) -> bool:
        return self.error_class is None


def classify_exception(exc: Exception) -> str:
    """Map a transport exception to the taxonomy in the spec (Section 5)."""
    if isinstance(exc, httpx.TimeoutException):
        return "timeout"
    if isinstance(exc, httpx.ConnectError):
        # ConnectError covers DNS failure, refused connections, and TLS
        # handshake errors; disambiguate via the underlying cause.
        cause = exc.__cause__
        if isinstance(cause, ssl.SSLError) or "certificate" in str(exc).lower():
            return "tls"
        message = str(exc).lower()
        if "name or service not known" in message or "getaddrinfo" in message:
            return "dns"
        return "conn_refused"
    # Everything else — including RemoteProtocolError (peer closed mid-response
    # or spoke malformed HTTP) — is a connection-level failure, not a status.
    return "conn_refused"


def classify_status(status_code: int) -> str | None:
    """None means the attempt succeeded."""
    if 200 <= status_code < 300:
        return None
    if status_code >= 500:
        return "http_5xx"
    return "http_4xx"


def build_headers(
    delivery_id: uuid.UUID, event_id: uuid.UUID, secret: str, body: bytes, timestamp: int
) -> dict[str, str]:
    return {
        "Content-Type": "application/json",
        "User-Agent": "Relay/1.0",
        "Relay-Id": str(delivery_id),
        "Relay-Event-Id": str(event_id),
        "Relay-Timestamp": str(timestamp),
        "Relay-Signature": compute_signature(secret, timestamp, body),
    }


def serialize_payload(payload: dict) -> bytes:
    # Serialize once and sign these exact bytes — re-encoding before sending
    # would risk a signature the receiver cannot reproduce.
    return json.dumps(payload, separators=(",", ":"), sort_keys=True).encode()


async def send_delivery(
    client: httpx.AsyncClient,
    *,
    url: str,
    secret: str,
    delivery_id: uuid.UUID,
    event_id: uuid.UUID,
    payload: dict,
) -> AttemptResult:
    """POST one delivery. Never raises: transport errors become AttemptResults."""
    body = serialize_payload(payload)
    headers = build_headers(delivery_id, event_id, secret, body, int(time.time()))

    started = time.perf_counter()
    try:
        response = await client.post(url, content=body, headers=headers)
    except Exception as exc:
        return AttemptResult(
            latency_ms=int((time.perf_counter() - started) * 1000),
            http_status=None,
            error_class=classify_exception(exc),
            response_snippet=str(exc)[:RESPONSE_SNIPPET_LIMIT],
        )

    return AttemptResult(
        latency_ms=int((time.perf_counter() - started) * 1000),
        http_status=response.status_code,
        error_class=classify_status(response.status_code),
        response_snippet=response.text[:RESPONSE_SNIPPET_LIMIT],
    )
