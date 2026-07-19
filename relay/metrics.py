"""Prometheus metrics (spec Section 10).

Cardinality rule: `tenant` is a safe label (bounded, and the unit we bill and
alert on). `endpoint` appears only on breaker/DLQ gauges, which are bounded by
the number of *unhealthy* endpoints — never on per-delivery counters, where it
would multiply series by every endpoint in the system.
"""

from prometheus_client import Counter, Gauge, Histogram, start_http_server

events_ingested = Counter(
    "relay_events_ingested_total",
    "Events accepted at POST /v1/events",
    ["tenant"],
)

deliveries = Counter(
    "relay_deliveries_total",
    "Delivery attempts by outcome",
    ["tenant", "outcome"],  # delivered | failed | dead
)

delivery_latency = Histogram(
    "relay_delivery_latency_seconds",
    "Wall time of the outbound HTTP request",
    # Buckets straddle the 1s p95 target and the 10s timeout.
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)

deliveries_gated = Counter(
    "relay_deliveries_gated_total",
    "Deliveries deferred by a gate rather than attempted",
    ["reason"],  # rate_limited | max_inflight | breaker_open | ordered_wait
)

retry_queue_depth = Gauge(
    "relay_retry_queue_depth",
    "Deliveries waiting in the retry ZSET",
)

dlq_size = Gauge(
    "relay_dlq_size",
    "Dead deliveries currently in the DLQ",
    ["tenant"],
)

breaker_state = Gauge(
    "relay_breaker_state",
    "Circuit breaker state (0=closed, 1=half_open, 2=open)",
    ["endpoint"],
)

agent_runs = Counter(
    "relay_agent_runs_total",
    "Diagnosis agent runs by outcome",
    ["outcome"],
)

agent_cost_usd = Counter(
    "relay_agent_cost_usd_total",
    "Cumulative agent spend in USD",
)

BREAKER_STATE_VALUES = {"closed": 0, "half_open": 1, "open": 2}


def record_delivery(tenant_id: str, outcome: str, latency_seconds: float) -> None:
    deliveries.labels(tenant=tenant_id, outcome=outcome).inc()
    delivery_latency.observe(latency_seconds)


def serve_metrics(port: int) -> None:
    """Expose /metrics from a non-HTTP process (worker, scheduler)."""
    start_http_server(port)
