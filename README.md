# Relay — Webhook Delivery Platform

Multi-tenant webhook delivery (retries, DLQ, HMAC signing, ordering, tenant isolation,
circuit breakers) plus a LangGraph agent that diagnoses failing customer endpoints.

## Quick start

```sh
cp .env.example .env
docker compose up -d
curl http://localhost:8000/healthz
```

## Development

```sh
python -m venv .venv
.venv/Scripts/activate      # Windows (source .venv/bin/activate on Unix)
pip install -e ".[dev]"
ruff check .
pytest
```

## Verifying signatures (receiver side)

Every delivery carries these headers:

```
Relay-Id: <delivery_id>
Relay-Event-Id: <event_id>
Relay-Timestamp: <unix_seconds>
Relay-Signature: v1=<hex hmac_sha256(secret, "{timestamp}.{raw_body}")>
```

Verify against the **raw request body** — parsing and re-encoding the JSON will
change the bytes and break the signature:

```python
import hashlib, hmac, time

def verify(secret: str, raw_body: bytes, timestamp: str, signature: str) -> bool:
    # Reject stale timestamps so a captured request can't be replayed later.
    if abs(int(time.time()) - int(timestamp)) > 300:
        return False
    expected = hmac.new(
        secret.encode(), f"{timestamp}.".encode() + raw_body, hashlib.sha256
    ).hexdigest()
    # Constant-time compare: never leak how much of the digest matched.
    return hmac.compare_digest(f"v1={expected}", signature)
```

A working implementation lives in [flaky_endpoint/main.py](flaky_endpoint/main.py),
which verifies every request it receives.

## Delivery semantics

At-least-once. Workers ACK a stream entry only after the attempt row is
committed, so a crash mid-delivery replays the entry rather than losing it —
receivers should deduplicate on `Relay-Event-Id`.

Two mechanisms make that guarantee hold in practice:

- **Redis outages are retried, not fatal.** Every Redis call in the consume loop
  sits inside a backoff-retry envelope. A dead shard task is invisible — the
  process keeps running and silently stops delivering — so the worker also
  supervises its tasks and restarts any that exit unexpectedly.
- **Abandoned entries are reclaimed.** A worker that dies mid-delivery leaves its
  entry in the consumer group's pending list. Peers `XAUTOCLAIM` entries idle
  longer than 60s (comfortably above the 10s delivery timeout, so live work is
  never stolen) and finish them.

Full specification and build phases: [CLAUDE.md](CLAUDE.md).
This README grows with each phase (architecture, signing verification, ordering
tradeoffs, load numbers, agent design).
