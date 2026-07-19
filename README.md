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

## Choosing an ordering mode

Set `ordering` when creating an endpoint. The choice is a real tradeoff, not a
default to skip past.

| | `unordered` (default) | `ordered` |
|---|---|---|
| Delivery | Concurrent, up to the tenant's in-flight cap | Strictly one at a time, in enqueue order |
| A failing delivery | Others continue past it | **Blocks everything behind it** until it succeeds or dies |
| Throughput | Scales with workers | Capped at one in-flight delivery per endpoint |
| Use it when | Events are independent (`invoice.paid`, `user.signup`) | Later events invalidate earlier ones (`order.created` → `order.shipped` → `order.cancelled`) |

**Head-of-line blocking is intentional.** In `ordered` mode a delivery that
enters the retry schedule keeps its place at the head of the endpoint's queue,
so a receiver that is down for an hour stalls that endpoint's backlog for an
hour. Skipping ahead would be the only alternative — and that is precisely the
guarantee `ordered` exists to provide. If a stalled backlog is worse for you
than out-of-order delivery, use `unordered` and carry a sequence number in your
payload.

Ordering is per-endpoint, so one tenant can have both.

## Tenant isolation

Three independent limits stop one tenant degrading another:

- **Rate limit** — a Redis token bucket per tenant (default 50 deliveries/sec,
  override with `rate_per_sec` at tenant creation). Ingestion is never rate
  limited; only delivery is, so `POST /v1/events` stays fast under load.
- **Concurrency cap** — at most `max_inflight` deliveries in flight per tenant
  (default 20). Slots are held in a sorted set keyed by expiry rather than a
  counter, so a worker that dies mid-delivery cannot permanently leak capacity.
- **Sharded streams** — deliveries are spread across `STREAM_SHARDS` (default 8)
  streams by `crc32(endpoint_id)`, so no single tenant's backlog monopolises the
  worker pool.

A delivery blocked by any gate is rescheduled, not failed: gate rejections never
consume the retry budget or land in the DLQ.

## Circuit breaker

Per endpoint, `closed → open → half_open`. It opens after 10 consecutive
failures, or a >50% failure rate over a 5-minute window with at least 20
attempts. While open, deliveries are deferred without attempts. Every 10
minutes one probe is allowed through: success closes the breaker and resumes
the backlog, failure reopens it. An endpoint continuously open for 72 hours is
auto-disabled.

Transitions are published on the `relay:breaker-events` Redis channel. Inspect
or override the state:

```sh
curl -H "Authorization: Bearer $KEY" localhost:8000/v1/endpoints/$ID/breaker
curl -X POST -H "Authorization: Bearer $KEY" localhost:8000/v1/endpoints/$ID/breaker/reset
```

Full specification and build phases: [CLAUDE.md](CLAUDE.md).
This README grows with each phase (architecture, signing verification, ordering
tradeoffs, load numbers, agent design).
