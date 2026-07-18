import hashlib
import hmac
import time

SIGNATURE_VERSION = "v1"
# Receivers must reject signatures older than this to blunt replay attacks.
DEFAULT_TOLERANCE_SECONDS = 300


def signed_payload(timestamp: int, body: bytes) -> bytes:
    """The exact bytes that get signed: "{timestamp}.{raw_body}".

    The timestamp is inside the signed material so an attacker cannot replay a
    captured body under a fresh timestamp.
    """
    return f"{timestamp}.".encode() + body


def compute_signature(secret: str, timestamp: int, body: bytes) -> str:
    digest = hmac.new(secret.encode(), signed_payload(timestamp, body), hashlib.sha256).hexdigest()
    return f"{SIGNATURE_VERSION}={digest}"


def verify_signature(
    secret: str,
    timestamp: int,
    body: bytes,
    signature: str,
    tolerance_seconds: int = DEFAULT_TOLERANCE_SECONDS,
    now: int | None = None,
) -> bool:
    """Reference receiver-side verification (documented in the README)."""
    current = int(time.time()) if now is None else now
    if abs(current - timestamp) > tolerance_seconds:
        return False
    # constant-time compare: never leak how much of the digest matched
    return hmac.compare_digest(compute_signature(secret, timestamp, body), signature)
