"""Known-answer vectors and verification rules for HMAC signing."""

import hashlib
import hmac

from relay.signing import compute_signature, signed_payload, verify_signature

SECRET = "whsec_test_secret"
TIMESTAMP = 1700000000
BODY = b'{"amount":42}'


def test_signed_payload_is_timestamp_dot_body():
    assert signed_payload(TIMESTAMP, BODY) == b"1700000000." + BODY


def test_known_answer_vector():
    expected = hmac.new(
        SECRET.encode(), b"1700000000." + BODY, hashlib.sha256
    ).hexdigest()
    assert compute_signature(SECRET, TIMESTAMP, BODY) == f"v1={expected}"


def test_signature_is_stable():
    assert compute_signature(SECRET, TIMESTAMP, BODY) == compute_signature(SECRET, TIMESTAMP, BODY)


def test_verify_accepts_valid_signature():
    sig = compute_signature(SECRET, TIMESTAMP, BODY)
    assert verify_signature(SECRET, TIMESTAMP, BODY, sig, now=TIMESTAMP) is True


def test_verify_rejects_wrong_secret():
    sig = compute_signature("other_secret", TIMESTAMP, BODY)
    assert verify_signature(SECRET, TIMESTAMP, BODY, sig, now=TIMESTAMP) is False


def test_verify_rejects_tampered_body():
    sig = compute_signature(SECRET, TIMESTAMP, BODY)
    assert verify_signature(SECRET, TIMESTAMP, b'{"amount":999}', sig, now=TIMESTAMP) is False


def test_verify_rejects_replayed_timestamp():
    """A captured signature must not verify once outside the 300s window."""
    sig = compute_signature(SECRET, TIMESTAMP, BODY)
    assert verify_signature(SECRET, TIMESTAMP, BODY, sig, now=TIMESTAMP + 299) is True
    assert verify_signature(SECRET, TIMESTAMP, BODY, sig, now=TIMESTAMP + 301) is False
    assert verify_signature(SECRET, TIMESTAMP, BODY, sig, now=TIMESTAMP - 301) is False


def test_body_signed_under_different_timestamp_does_not_verify():
    sig = compute_signature(SECRET, TIMESTAMP, BODY)
    assert verify_signature(SECRET, TIMESTAMP + 10, BODY, sig, now=TIMESTAMP + 10) is False
