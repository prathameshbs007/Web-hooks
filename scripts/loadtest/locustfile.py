"""Load test for POST /v1/events (spec Section 11).

Run against a live stack:

    locust -f scripts/loadtest/locustfile.py --host http://localhost:8000 \
           --users 50 --spawn-rate 10 --run-time 60s --headless

Each simulated user provisions its own tenant + endpoint on start, so the run
also exercises per-tenant rate limiting and shard distribution rather than
hammering a single tenant.
"""

import os
import uuid

from locust import HttpUser, between, task

ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN", "change-me")
RECEIVER_URL = os.environ.get("LOADTEST_RECEIVER", "http://flaky-endpoint:9000/hook")


class RelayUser(HttpUser):
    wait_time = between(0.01, 0.05)

    def on_start(self) -> None:
        tenant = self.client.post(
            "/v1/tenants",
            json={"name": f"load-{uuid.uuid4().hex[:8]}", "rate_per_sec": 500},
            headers={"Authorization": f"Bearer {ADMIN_TOKEN}"},
            name="/v1/tenants",
        ).json()
        self.auth = {"Authorization": f"Bearer {tenant['api_key']}"}
        self.event_type = f"load.{uuid.uuid4().hex[:6]}"
        self.client.post(
            "/v1/endpoints",
            json={"url": RECEIVER_URL, "event_types": [self.event_type]},
            headers=self.auth,
            name="/v1/endpoints",
        )

    @task
    def ingest(self) -> None:
        self.client.post(
            "/v1/events",
            json={
                "event_type": self.event_type,
                "payload": {"id": uuid.uuid4().hex, "amount": 42},
            },
            headers=self.auth,
            name="/v1/events",
        )
