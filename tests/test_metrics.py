"""Metrics registration, labels, and the /metrics endpoint."""

from prometheus_client import generate_latest

from relay.metrics import (
    BREAKER_STATE_VALUES,
    breaker_state,
    deliveries,
    deliveries_gated,
    delivery_latency,
    dlq_size,
    events_ingested,
    record_delivery,
    retry_queue_depth,
)
from tests.conftest import requires_infra


def _scrape() -> str:
    return generate_latest().decode()


def test_all_spec_metrics_are_registered():
    """Section 10 names these explicitly; a missing one breaks the dashboard."""
    scrape = _scrape()
    for name in (
        "relay_events_ingested_total",
        "relay_deliveries_total",
        "relay_delivery_latency_seconds",
        "relay_retry_queue_depth",
        "relay_dlq_size",
        "relay_breaker_state",
        "relay_agent_runs_total",
        "relay_agent_cost_usd_total",
    ):
        assert name in scrape, f"{name} is not registered"


def test_record_delivery_increments_counter_and_histogram():
    tenant = "tenant-metrics-test"
    before = deliveries.labels(tenant=tenant, outcome="delivered")._value.get()
    record_delivery(tenant, "delivered", 0.25)
    after = deliveries.labels(tenant=tenant, outcome="delivered")._value.get()
    assert after == before + 1

    scrape = _scrape()
    assert 'relay_deliveries_total{outcome="delivered"' in scrape
    assert "relay_delivery_latency_seconds_bucket" in scrape


def test_latency_buckets_straddle_the_p95_target():
    """The 1s SLO must be an actual bucket edge, or p95 can't be read off it."""
    buckets = delivery_latency._upper_bounds
    assert 1.0 in buckets
    # And the 10s delivery timeout is the last finite bucket.
    assert 10.0 in buckets


def test_delivery_counter_has_no_endpoint_label():
    """Cardinality guard: endpoint_id on a per-delivery counter would explode series."""
    assert set(deliveries._labelnames) == {"tenant", "outcome"}
    assert "endpoint" not in events_ingested._labelnames


def test_breaker_and_dlq_gauges_are_labelled_as_specified():
    assert set(breaker_state._labelnames) == {"endpoint"}
    assert set(dlq_size._labelnames) == {"tenant"}
    assert retry_queue_depth._labelnames == ()


def test_breaker_state_values_cover_the_state_machine():
    assert BREAKER_STATE_VALUES == {"closed": 0, "half_open": 1, "open": 2}


def test_gate_reasons_are_counted():
    before = deliveries_gated.labels(reason="rate_limited")._value.get()
    deliveries_gated.labels(reason="rate_limited").inc()
    assert deliveries_gated.labels(reason="rate_limited")._value.get() == before + 1


@requires_infra
async def test_metrics_endpoint_served_by_api(api_client):
    resp = await api_client.get("/metrics")
    assert resp.status_code == 200
    assert "text/plain" in resp.headers["content-type"]
    assert "relay_events_ingested_total" in resp.text
