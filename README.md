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

Full specification and build phases: [CLAUDE.md](CLAUDE.md).
This README grows with each phase (architecture, signing verification, ordering
tradeoffs, load numbers, agent design).
