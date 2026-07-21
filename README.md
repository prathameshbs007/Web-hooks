# Relay — Webhook Delivery Platform

Multi-tenant webhook delivery (retries, DLQ, HMAC signing, ordering, tenant isolation,
circuit breakers) plus a LangGraph agent that diagnoses failing customer endpoints.

## Architecture

```
 tenant                                                    ┌─────────────┐
   │ POST /v1/events                                       │  Prometheus │
   ▼                                                       │  + Grafana  │
┌──────┐  validate, persist,     ┌────────────┐            └──────△──────┘
│ api  │  idempotency, fan-out   │ Postgres   │  /metrics scrape ─┘ (api,
│      │────────────────────────▶│ events,    │◀───────────────────  worker,
└──┬───┘  202 Accepted           │ deliveries,│                      scheduler,
   │                             │ attempts,  │                      agent)
   │ XADD (sharded by endpoint)  │ diagnoses  │
   ▼                             └────△───────┘
 Redis Stream relay:deliveries:{0..7}  │ attempt rows
   │  consumer group "workers"         │
   ▼                                   │
┌────────┐ ordering→rate-limit→        │        ┌───────────────┐
│ worker │ concurrency→breaker gates   │        │ retry-        │
│ (×N)   │ ── httpx POST (signed) ─────┘        │ scheduler     │
└──┬─────┘        │ success → delivered          │ ZSET poller,  │
   │ failure      │                              │ due → stream  │
   ▼              ▼                              └──────△────────┘
 Redis ZSET relay:retries ──────────────────────────────┘
   │  attempts exhausted → status 'dead' (DLQ)
   ▼
 breaker opens ──pub/sub relay:breaker-events──▶ ┌────────┐  LangGraph:
 (or 5 consecutive failures)                     │ agent  │  triage→investigate
                                                 │        │  →hypothesize→verify
                                                 └───┬────┘  →report
                                                     ▼
                                    diagnoses row + drafted email;
                                    mutating actions await human approval
```

## Quick start

```sh
cp .env.example .env
# optional: add a free Gemini key (LLM_API_KEY) for the agent — see "Agent LLM options"
docker compose up -d
curl http://localhost:8000/healthz
python scripts/demo.py          # the whole story end to end, ~2 min
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

## Observability

`docker compose up` brings up Prometheus (`:9090`) and Grafana (`:3000`, anonymous
viewer access) with the **Relay — Delivery Overview** dashboard provisioned:
ingestion rate, delivery outcomes, success rate, latency percentiles, retry-queue
depth, DLQ by tenant, breaker map, and gated-delivery reasons.

Three scrape targets: `api:8000/metrics`, `worker:9100/metrics`,
`retry-scheduler:9101/metrics`. The worker and scheduler have no HTTP server of
their own, so each starts a `prometheus_client` endpoint purely for scraping.

Label cardinality is deliberate: `tenant` on counters, `endpoint` **only** on the
breaker and DLQ gauges (bounded by how many endpoints are unhealthy, not by fleet
size). Point-in-time gauges are refreshed by the scheduler every 5s rather than
computed per scrape, so `/metrics` can't be turned into a DB load generator.

### Measured load numbers

Dev hardware (Windows 11, Docker Desktop, single worker container, all services
plus Postgres/Redis/Prometheus/Grafana on one machine). Load via
`scripts/loadtest/locustfile.py`, each simulated user provisioning its own tenant:

| Metric | Result | Target |
|---|---|---|
| Sustained ingestion | **273 events/sec** (20,293 events / 75s) | ≥ 100/sec |
| **Delivery p95** | **9.8 ms** | < 1 s |
| Delivery p50 / p99 | 5.3 ms / 23.4 ms | — |
| Ingestion p95 (`POST /v1/events`) | 190 ms | — |
| Failures | **0** across 33,612 events | — |
| Retry queue / DLQ at steady state | 0 / 0 | — |

Reproduce:

```sh
docker compose up -d
ADMIN_TOKEN=change-me locust -f scripts/loadtest/locustfile.py \
  --host http://localhost:8000 --users 50 --spawn-rate 25 --run-time 60s --headless
```

Then watch http://localhost:3000 during the run.

Delivery latency is low because the receiver is the local `flaky-endpoint`
container — this measures Relay's overhead, not real-world network time. The
honest reading: Relay adds single-digit milliseconds on top of whatever the
customer's endpoint costs.

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

## Diagnosis agent

When a circuit breaker opens, the agent (a LangGraph workflow:
triage → investigate → hypothesize → **verify** → report) investigates why the
endpoint is failing. It queries the attempt history, probes the endpoint with a
signed synthetic delivery, and checks DNS/TLS, then writes a `diagnoses` row with
a root-cause label, confidence, evidence, and a drafted customer email. The
`verify` step is mandatory — a hypothesis never tested against a live probe is
forced to `low` confidence in code, not left to the model's say-so.

Mutating tools (`pause_endpoint`, `replay_dlq`) never act on their own: they
record a pending `agent_actions` row that a human approves via
`POST /v1/diagnoses/actions/{id}/approve`. Guardrails: at most one run per
endpoint per hour, ten runs per day, and a per-run cost cap. Agent failure is
isolated — it writes an `agent_error` diagnosis and never touches delivery.

### Agent LLM options

The agent's model is provider-neutral, selected by env var:

| Var | Default | Notes |
|---|---|---|
| `LLM_PROVIDER` | `gemini` | `gemini` or `anthropic` |
| `LLM_MODEL` | `gemini-2.5-flash` | any model the provider serves |
| `LLM_API_KEY` | — | the provider's key |

**Gemini (default).** Get a **free** key at
[aistudio.google.com](https://aistudio.google.com) — no credit card. Put it in
`.env` as `LLM_API_KEY=...` and restart the agent (`docker compose up -d agent`).
Uses `langchain-google-genai`.

Model note: the default is `gemini-3.1-flash-lite`. Newly created free keys
**404** on `gemini-2.5-flash`, and the heavier preview models rate-limit hard on
the free tier during a multi-call diagnosis. The adapter preserves Gemini 3.x
`thought_signature` tokens across tool-call turns (required by those models), so
any current Gemini flash model works — pick per your key's quota.

**Anthropic (alternate).** Set `LLM_PROVIDER=anthropic`, `LLM_MODEL=claude-...`,
`LLM_API_KEY=sk-ant-...`, and install the optional package:
`pip install langchain-anthropic`. Billed via the Anthropic API (separate from a
Claude Pro subscription).

Both go through a small adapter that speaks the same interface to the graph, so
the mocked golden tests are provider-independent and run for free in CI (the
provider packages are imported only when a real run happens).

> Free-tier models (e.g. `gemini-2.5-flash`) are capable but will generally
> produce lower-confidence diagnoses than a frontier model, and the free tier is
> rate-limited — the agent backs off and retries on 429, then fails the run
> gracefully (an `agent_error` diagnosis; delivery is unaffected).

## Deploying on a single VPS

The production overlay puts everything behind Caddy (automatic HTTPS via
Let's Encrypt) and stops exposing Postgres/Redis to the host.

1. Point two DNS `A` records (e.g. `relay.example.com`, `grafana.example.com`) at
   the VPS. Open ports 80 and 443.
2. Clone the repo, then create `.env`:

   ```sh
   cp .env.example .env
   # set, at minimum:
   #   ADMIN_TOKEN=<long random>
   #   RELAY_DOMAIN=relay.example.com
   #   GRAFANA_DOMAIN=grafana.example.com
   #   GF_SECURITY_ADMIN_PASSWORD=<random>
   #   LLM_API_KEY=<gemini key>            # optional, for the agent
   ```

3. Bring it up with the prod overlay:

   ```sh
   docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d --build
   ```

   Caddy provisions certificates on first request. The API is then at
   `https://relay.example.com`, Grafana at `https://grafana.example.com`.

What the overlay changes ([docker-compose.prod.yml](docker-compose.prod.yml)):
Caddy is the only service with published ports; Postgres/Redis are internal;
two workers by default (`--scale worker=N` for more); `restart: always`
everywhere; the flaky simulator is excluded; Grafana anonymous access is off.

**Operational note — Redis durability.** Delivery in-flight state (streams, the
retry ZSET, breaker and rate-limit keys) lives in Redis. The workers survive a
Redis restart, but this compose runs Redis without persistence, so a Redis data
loss drops in-flight retry schedules (the durable record of every delivery still
lives in Postgres). For production, enable Redis AOF and a volume.

## Limitations (honest list)

- **Single region, at-least-once.** No multi-region replication and no
  exactly-once delivery — receivers must dedupe on `Relay-Event-Id`. Duplicate
  sends are expected on retries and worker crashes.
- **No SLA / no billing / no SPA frontend** — explicit non-goals (spec §1).
- **Delivery throughput is per-worker.** Measured ~100 deliveries/sec with one
  worker container on dev hardware; ingestion sustains ~270/sec. Scale delivery
  with more workers; it's sharded, so they don't contend.
- **DLQ is slow to reach by design.** The backoff schedule runs to ~8h before a
  delivery dead-letters, so the DLQ/replay path is proven by tests, not the
  short demo.
- **Redis is not persisted by default** (see the operational note above).
- **Agent quality tracks the model.** On a free-tier Gemini model, diagnoses are
  solid on clear failures (down endpoints, timeouts, TLS) but lower-confidence on
  ambiguous flapping than a frontier model would be. Cost is metered
  (`relay_agent_cost_usd_total`) and capped per run.
- **The measured numbers are dev-hardware, local-receiver.** Delivery p95 of
  ~10ms reflects Relay's own overhead, not real internet round-trips to a
  customer endpoint.

## Measured numbers

Dev hardware, one worker, local receiver, 50 locust users / 60s:

| Metric | Value |
|---|---|
| Sustained ingestion | ~270 events/sec, 0 failures |
| Delivery p95 (Relay overhead) | ~10 ms |
| Agent diagnosis (gemini-3.1-flash-lite) | ~$0.005/run, ~15s, verified high-confidence |

Reproduce ingestion numbers with the locust file in
[scripts/loadtest/](scripts/loadtest/locustfile.py); reproduce the full story
with [scripts/demo.py](scripts/demo.py).

Full specification and build phases: [CLAUDE.md](CLAUDE.md).
This README grows with each phase (architecture, signing verification, ordering
tradeoffs, load numbers, agent design).
