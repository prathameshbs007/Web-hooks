# CLAUDE.md — Relay: Webhook Delivery Platform + Failure-Diagnosis Agent

**This is the single source of truth for building this project. Read this entire file before writing any code.**

Relay is a multi-tenant webhook delivery platform (retries, DLQ, HMAC signing, ordering guarantees, tenant isolation, circuit breakers) plus a LangGraph agent that autonomously diagnoses failing customer endpoints. Full technical specification is in Sections 1–14 below.

---

## BUILD PROTOCOL — non-negotiable rules

### Phase gating
1. Build strictly phase by phase per Section 12 (Phase 0 → 7). Never work on two phases at once. Never start phase N+1 without explicit user approval ("approved", "continue", "next phase" or similar).
2. On your **first run** in this repo: read this file fully, print a one-paragraph confirmation that you understand the phase gates and verification protocol, then execute **Phase 0 only**.
3. When the user says continue, first state which phase you are starting and its acceptance criteria, then build it.

### Mandatory verification after EVERY phase
Before declaring a phase complete, you must:
1. Run `ruff check .` — zero errors.
2. Run `pytest` — all tests pass. Every phase adds tests for its new behavior; a phase with no new tests is incomplete.
3. Run `docker compose up -d` from scratch (`docker compose down -v` first) and prove the stack is healthy (`curl /healthz`, plus curl commands exercising the phase's new endpoints/behavior).
4. Print the phase's acceptance criteria from Section 12 as a checklist. For each item: **PASS/FAIL + concrete evidence** (test name, command output, curl response). If anything fails, fix it and re-verify — do not present a failing checklist as done.
5. Commit as `phase-N: <summary>`. List every file changed. If you touched code from an earlier phase, explicitly call out each such file and why.
6. **STOP.** Print: "Phase N complete. Verify on your machine (docker compose up, pytest, curl), review the diff, then tell me to continue." Do not proceed.

### General rules
- Everything must run with `docker compose up` from a fresh clone at all times. If a change breaks this, fixing it is the top priority.
- Use only the libraries specified in Section 2. Ask before adding any other dependency.
- Secrets live in `.env` (git-ignored); keep `.env.example` current (Section 13).
- Never weaken, skip, or reinterpret acceptance criteria to make a phase pass. If a criterion seems wrong or infeasible, stop and raise it with the user instead.
- Write code you can defend: prefer clear, well-named modules over clever abstractions. Add brief comments on the *why* of non-obvious decisions (ack-after-commit, jitter bounds, Lua atomicity) — the user must be able to explain every design choice in interviews.
- If the user asks a question mid-phase, answer it; do not use it as a trigger to keep building.

---

# TECHNICAL SPECIFICATION

## 1. Project overview

Relay is a multi-tenant webhook delivery platform (the problem Svix/Hookdeck solve) plus a layer they don't ship: an autonomous agent that diagnoses **why** a customer's endpoint is failing and drafts the fix.

**Core contract:** tenants publish events via API → Relay delivers each event as a signed HTTP POST to the tenant's configured endpoints with at-least-once semantics, retries, ordering options, and full attempt history.

**Differentiator:** when an endpoint degrades, a LangGraph agent investigates (queries logs, probes the endpoint, checks TLS/DNS), produces a root-cause diagnosis with evidence, and drafts a customer-facing email. Mutating actions (pause endpoint, replay DLQ) require human approval.

### Goals
1. Production-grade architecture at hobby scale: correct semantics, observable, deployable on one VPS.
2. Demonstrable system design: backoff+jitter, DLQ, HMAC signing, per-endpoint ordering, tenant isolation, circuit breakers.
3. A real agent: tools with side effects, hypothesis → verification loop, guardrails.
4. Live public demo with measurable numbers (events/sec, p95 delivery latency).

### Non-goals
- Multi-region, exactly-once delivery, SOC2, billing, full SPA frontend. Do not build these.

---

## 2. Tech stack (Python)

| Concern | Choice |
|---|---|
| API service | FastAPI + Uvicorn (Python 3.12) |
| DB | PostgreSQL 16, SQLAlchemy 2.x (async) + Alembic migrations |
| Queue | Redis 7 — Streams + consumer groups for delivery; ZSET for scheduled retries |
| Delivery workers | Plain asyncio worker processes using `httpx.AsyncClient` |
| Rate limiting / circuit breaker state | Redis (Lua scripts where atomicity matters) |
| Agent | LangGraph + Anthropic API (`claude-sonnet-4-6`) |
| Metrics | `prometheus-client`, Prometheus, Grafana |
| Logging | `structlog`, JSON logs |
| Tests | pytest + pytest-asyncio + testcontainers (or compose-based test env) |
| Lint/format | ruff |
| Load test | locust |
| Packaging/runtime | Docker Compose; Caddy for TLS in deployment |

---

## 3. Repository layout

```
relay/
├── docker-compose.yml
├── docker-compose.prod.yml
├── .env.example
├── pyproject.toml
├── README.md
├── alembic/
├── relay/
│   ├── config.py              # pydantic-settings, all env vars
│   ├── db/                    # engine, session, models.py
│   ├── api/                   # FastAPI app
│   │   ├── main.py
│   │   ├── routes/            # tenants.py, endpoints.py, events.py, dlq.py, diagnoses.py
│   │   ├── auth.py            # API-key auth dependency
│   │   └── schemas.py         # pydantic request/response models
│   ├── delivery/
│   │   ├── enqueue.py         # write to Redis Streams
│   │   ├── worker.py          # consumer-group worker loop
│   │   ├── sender.py          # HTTP POST, signing, timeout, attempt recording
│   │   ├── retry_scheduler.py # ZSET poller that re-enqueues due retries
│   │   ├── circuit_breaker.py
│   │   └── rate_limit.py      # per-tenant token bucket + concurrency caps
│   ├── signing.py             # HMAC sign/verify helpers
│   ├── agent/
│   │   ├── graph.py           # LangGraph workflow
│   │   ├── tools.py           # query_attempts, probe_endpoint, check_dns_tls, ...
│   │   ├── trigger.py         # watches breaker events, enforces run budget
│   │   └── prompts.py
│   ├── metrics.py             # Prometheus counters/histograms
│   └── observability.py       # structlog setup
├── flaky_endpoint/            # separate FastAPI app: configurable failure simulator
│   └── main.py
├── dashboards/                # Grafana provisioning JSON
├── tests/
└── scripts/                   # seed.py, demo.py, loadtest/locustfile.py
```

Compose services: `api`, `worker` (scale to N), `retry-scheduler`, `agent`, `flaky-endpoint`, `postgres`, `redis`, `prometheus`, `grafana`.

---

## 4. Architecture & data flow

```
tenant POST /v1/events
        │ (validate, persist, idempotency check)
        ▼
   Postgres (events)          202 Accepted returned here
        │
        ▼
 Redis Stream  relay:deliveries:{shard}     (sharded by hash(endpoint_id))
        │  consumer group "workers"
        ▼
 Delivery worker ── rate-limit gate ── circuit-breaker gate ── httpx POST (signed, 10s timeout)
        │                                                        │
        ▼                                                        ▼
 Postgres (delivery_attempts)                        success → mark delivered
        │ failure
        ▼
 Redis ZSET relay:retries  (score = next_attempt_at)
        │ retry_scheduler polls due items → re-enqueue to stream
        │ attempts exhausted
        ▼
 DLQ (Postgres state + metric) ──► breaker opens ──► agent trigger ──► LangGraph run ──► diagnoses table + drafted email
```

Key invariants:
- Ingestion never blocks on delivery. `/v1/events` does: validate → insert → XADD → return 202.
- Every delivery attempt is recorded (status, latency, response snippet ≤ 1 KB, error class).
- At-least-once: workers ACK the stream entry only after the attempt row is committed. Duplicate sends are acceptable; receivers deduplicate by `event_id`.

---

## 5. Data model (Postgres)

```sql
tenants(
  id uuid pk, name text, api_key_hash text unique,
  created_at timestamptz
)

endpoints(
  id uuid pk, tenant_id uuid fk, url text,
  signing_secret text,                 -- generated, shown once
  ordering text check in ('ordered','unordered') default 'unordered',
  status text check in ('active','paused','disabled') default 'active',
  event_types text[] default '{}',     -- empty = subscribe to all
  created_at timestamptz
)

events(
  id uuid pk, tenant_id uuid fk,
  event_type text, payload jsonb,
  idempotency_key text,
  created_at timestamptz,
  unique(tenant_id, idempotency_key)
)

deliveries(                            -- one per (event, endpoint)
  id uuid pk, event_id uuid fk, endpoint_id uuid fk,
  status text check in ('pending','delivering','delivered','failed','dead') ,
  attempt_count int default 0,
  next_attempt_at timestamptz,
  created_at timestamptz, updated_at timestamptz
)

delivery_attempts(
  id bigserial pk, delivery_id uuid fk,
  attempt_number int, started_at timestamptz,
  latency_ms int, http_status int null,
  error_class text null,               -- 'timeout','conn_refused','dns','tls','http_4xx','http_5xx'
  response_snippet text null
)

diagnoses(
  id uuid pk, endpoint_id uuid fk,
  triggered_by text,                   -- 'breaker_open','consecutive_failures','manual'
  root_cause text, confidence text check in ('low','medium','high'),
  evidence jsonb,                      -- structured findings incl. probe results
  recommendation text, draft_email text,
  status text check in ('open','acknowledged','resolved'),
  created_at timestamptz
)
```

Indexes: `deliveries(endpoint_id, status)`, `delivery_attempts(delivery_id, attempt_number)`, `events(tenant_id, created_at)`.

---

## 6. API specification (all under /v1, API-key auth via `Authorization: Bearer <key>`)

Admin (no tenant auth, protected by `ADMIN_TOKEN`):
- `POST /v1/tenants` → create tenant, returns API key **once**.

Tenant-scoped:
- `POST /v1/endpoints` `{url, ordering?, event_types?}` → returns endpoint + `signing_secret` (once).
- `GET /v1/endpoints` / `PATCH /v1/endpoints/{id}` (url, status, event_types) / `DELETE`.
- `POST /v1/events` `{event_type, payload, idempotency_key?}` → `202 {event_id}`. Fan-out: one `deliveries` row per matching active endpoint.
- `GET /v1/events/{id}` → event + per-endpoint delivery status.
- `GET /v1/deliveries?endpoint_id=&status=` → paginated list.
- `GET /v1/deliveries/{id}/attempts` → full attempt history.
- `GET /v1/dlq?endpoint_id=` → dead deliveries.
- `POST /v1/dlq/replay` `{endpoint_id, delivery_ids?}` → re-enqueue dead deliveries (resets attempt_count, status='pending').
- `GET /v1/diagnoses?endpoint_id=` / `POST /v1/diagnoses/{id}/ack`.

System:
- `GET /healthz` (api, worker liveness), `GET /metrics` (Prometheus).

Error shape everywhere: `{"error": {"code": "...", "message": "..."}}`.

---

## 7. Core mechanics — exact specs

### 7.1 HMAC signing
Headers on every delivery:
```
Relay-Id: <delivery_id>
Relay-Event-Id: <event_id>
Relay-Timestamp: <unix_seconds>
Relay-Signature: v1=<hex hmac_sha256(secret, f"{timestamp}.{raw_body}")>
```
- Sign the exact raw body bytes. Document receiver-side verification (recompute, constant-time compare, reject if |now − timestamp| > 300s) in README with a copy-paste snippet.

### 7.2 Retry policy
- Timeout per attempt: 10s connect+read (httpx).
- Failure = timeout, connection error, DNS/TLS error, or HTTP ≥ 500. HTTP 429 = failure but respect `Retry-After` if present (cap 1h). HTTP 4xx (except 408/429) = terminal → straight to DLQ after 3 attempts (bad request won't fix itself; document this choice).
- Backoff schedule (attempt → delay): 1→5s, 2→30s, 3→2m, 4→10m, 5→30m, 6→2h, 7→5h. Jitter: multiply by uniform(0.8, 1.2). After attempt 7 fails → status `dead` (DLQ), emit metric + agent-trigger event.
- Implementation: on failure, compute `next_attempt_at`, ZADD to `relay:retries` with score = epoch. `retry_scheduler` polls with `ZRANGEBYSCORE ... LIMIT` every 500ms, moves due items back onto the stream atomically (Lua: ZREM + XADD).

### 7.3 Ordering modes
- `unordered` (default): deliveries for an endpoint proceed concurrently.
- `ordered`: strict per-endpoint FIFO. Implementation: per-endpoint Redis list/stream key; worker holds a per-endpoint lock (SET NX PX) and delivers one at a time in enqueue order; a retrying delivery **blocks** everything behind it (head-of-line blocking is the documented, intentional tradeoff). README must contain a "choosing an ordering mode" section explaining the tradeoff.

### 7.4 Tenant isolation
- Token bucket per tenant in Redis (Lua for atomic take): default 50 deliveries/sec, configurable per tenant.
- Concurrency cap: max 20 in-flight deliveries per tenant (Redis counter with TTL safety).
- Fairness: workers consume from N sharded streams; shard = hash(endpoint_id) % N (N=8). One tenant's backlog can saturate at most its own rate/concurrency budget, never the whole worker pool.

### 7.5 Circuit breaker (per endpoint)
- States: closed → open → half-open.
- Open when: 10 consecutive failures OR failure rate > 50% over a 5-minute sliding window (min 20 attempts). While open: deliveries stay `pending`, no attempts made.
- Half-open: every 10 minutes allow 1 probe delivery. Success → closed (resume backlog). Failure → open.
- Auto-`disabled` after 72h continuously open (matches industry behavior). Breaker state transitions are logged, exported as metrics, and emitted on Redis pub/sub channel `relay:breaker-events` (the agent trigger listens here).

---

## 8. Diagnosis agent (LangGraph)

### Triggers (agent/trigger.py)
- Breaker opens for an endpoint, OR 5 consecutive failures.
- Budget guardrails: max 1 run per endpoint per hour, max 10 runs/day globally, hard token/cost cap per run. Skipped triggers are logged.

### Tools (all read-only unless marked)
| Tool | Behavior |
|---|---|
| `query_attempts(endpoint_id, window)` | Aggregated failure history: counts by error_class/status, latency percentiles, first_failed_at, sample snippets |
| `get_endpoint_config(endpoint_id)` | url, ordering, status, breaker state, recent config changes |
| `probe_endpoint(endpoint_id)` | Sends a signed synthetic test event (marked `"relay_probe": true`), returns status/latency/error |
| `check_dns_tls(url)` | Resolves DNS; inspects cert chain: expiry, hostname match, chain validity |
| `pause_endpoint(endpoint_id)` | **Mutating — requires human approval** (writes a pending-approval action, not immediate) |
| `replay_dlq(endpoint_id)` | **Mutating — requires human approval** |

### Graph shape
```
triage → investigate (tool-use loop, max 6 calls) → hypothesize → verify (must call probe_endpoint or check_dns_tls) → report
```
- `verify` is mandatory: a hypothesis that isn't tested against a live probe gets confidence downgraded to 'low'. This loop is the point of the project — do not collapse it into one LLM call.
- `report` writes a `diagnoses` row: root_cause, confidence, evidence (structured tool outputs), recommendation, draft_email (plain, non-marketing tone).

### Diagnosis taxonomy the prompt must cover
endpoint down (conn refused / persistent 5xx), receiver too slow (timeouts → recommend ack-fast-process-async), receiver rate-limiting (429s → recommend lowering tenant rate or honoring Retry-After), TLS cert expired/mismatch, DNS failure, auth broken (401/403 → likely secret rotation), intermittent flapping (mixed pattern → recommend watching + no action).

### Failure handling
Agent errors must never affect the delivery pipeline. Wrap runs; on failure write a diagnosis row with root_cause='agent_error', confidence='low'.

---

## 9. Flaky endpoint simulator (flaky_endpoint/)

Standalone FastAPI app. `POST /configure {mode, param?}` switches behavior of `POST /hook`:
`healthy | http_500 | http_429 (with Retry-After) | timeout (sleep 30s) | conn_reset | slow (sleep 8s then 200) | auth_401 | flaky (random 30% failure)`.
It verifies Relay signatures and logs results — doubles as the reference receiver implementation. Used by tests, the demo script, and as the agent's sparring partner.

---

## 10. Observability

Prometheus metrics (labels kept low-cardinality — tenant_id yes, endpoint_id only on breaker/DLQ gauges):
- `relay_events_ingested_total{tenant}`, `relay_deliveries_total{tenant,outcome}`
- `relay_delivery_latency_seconds` histogram (p50/p95/p99 in Grafana)
- `relay_retry_queue_depth`, `relay_dlq_size{tenant}`, `relay_breaker_state{endpoint}`
- `relay_agent_runs_total{outcome}`, `relay_agent_cost_usd_total`

Grafana: one provisioned dashboard JSON — ingestion rate, success rate, latency percentiles, retry/DLQ depth, breaker map. structlog JSON logs with `delivery_id`/`event_id`/`tenant_id` on every line.

---

## 11. Testing strategy

- Unit: signing (known-answer vectors), backoff math incl. jitter bounds, breaker state machine, token bucket, terminal-4xx classification.
- Integration (compose): end-to-end happy path (ingest → delivered, signature verifies); retry path against flaky_endpoint in `http_500` then `healthy` (delivery eventually succeeds, attempts recorded); DLQ after exhaustion; replay works; ordered mode preserves order under induced failure (assert receive order); unordered mode doesn't block; tenant A backlog doesn't starve tenant B (two-tenant fairness test with assertions on B's p95).
- Agent: golden tests — run each simulator mode, assert diagnosis root_cause matches expected label. Mock the LLM in CI happy-path tests where possible; keep one live-API smoke test behind an env flag.
- Load: locustfile targeting `POST /v1/events`; document the numbers achieved (events/sec sustained, delivery p95) in README.

---

## 12. Build phases (execute in order)

**Phase 0 — Scaffolding.** pyproject (ruff, pytest), config.py, docker-compose with postgres/redis/api, healthz, alembic init, structlog, CI script. ✅ Fresh clone → `docker compose up` → healthz green; `pytest` runs.

**Phase 1 — Tenancy + ingestion.** Tenants/endpoints CRUD, API-key auth (hash keys), events endpoint with idempotency, fan-out to deliveries rows, XADD to stream. ✅ Duplicate idempotency_key returns same event_id; 202 under 50ms locally; tests pass.

**Phase 2 — Delivery pipeline.** Worker consumer group, sender with signing + 10s timeout, attempt recording, ACK-after-commit, flaky_endpoint app. ✅ E2E test: event → flaky(healthy) receives valid-signature POST; attempt row exists.

**Phase 3 — Retries + DLQ.** Failure classification, backoff+jitter, ZSET scheduler, terminal-4xx rule, DLQ status, replay API. ✅ Integration tests for retry-then-succeed and exhaust-then-dead-then-replay pass.

**Phase 4 — Ordering + isolation + breaker.** Ordered mode with per-endpoint lock, token bucket, concurrency caps, sharded streams, circuit breaker with pub/sub events. ✅ Order-preservation test and two-tenant fairness test pass; breaker opens/half-opens observably.

**Phase 5 — Observability.** Metrics wired everywhere, Grafana provisioning, load test run, numbers recorded in README. ✅ Dashboard shows a locust run live; p95 delivery latency < 1s at ≥100 events/sec on dev hardware.

**Phase 6 — Agent.** Tools, LangGraph graph, trigger with budgets, diagnoses API, approval flow for mutating tools. ✅ Golden tests: ≥ 6 of 8 simulator modes diagnosed correctly with confidence ≥ medium; agent failure doesn't impact delivery.

**Phase 7 — Deploy + demo.** docker-compose.prod.yml, Caddy TLS, deploy docs for a single VPS, `scripts/demo.py` (creates tenant, endpoint → flaky simulator, fires events, breaks endpoint, shows agent diagnosis), README with architecture diagram, tradeoffs section, and measured numbers. ✅ Publicly reachable API + Grafana; demo script runs clean start to finish.

---

## 13. Configuration (.env.example)

```
DATABASE_URL=postgresql+asyncpg://relay:relay@postgres:5432/relay
REDIS_URL=redis://redis:6379/0
ADMIN_TOKEN=change-me
ANTHROPIC_API_KEY=
AGENT_MODEL=claude-sonnet-4-6
AGENT_MAX_RUNS_PER_ENDPOINT_PER_HOUR=1
AGENT_MAX_RUNS_PER_DAY=10
WORKER_CONCURRENCY=32
STREAM_SHARDS=8
DEFAULT_TENANT_RATE_PER_SEC=50
DEFAULT_TENANT_MAX_INFLIGHT=20
DELIVERY_TIMEOUT_SECONDS=10
MAX_ATTEMPTS=7
```

---

## 14. Definition of done

1. `docker compose up` from fresh clone → working system; `scripts/demo.py` tells the full story in < 3 minutes.
2. All phase acceptance criteria green; `pytest` and `ruff` clean.
3. README: what/why, architecture diagram, signing verification snippet, ordering tradeoff writeup, isolation design, measured load numbers, agent design + guardrails, honest limitations section (single region, at-least-once, no SLA).
4. Deployed instance reachable over HTTPS with Grafana dashboard.
