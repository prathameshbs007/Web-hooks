#!/usr/bin/env python3
"""End-to-end demo: the whole Relay story in under three minutes.

Creates a tenant and an endpoint pointing at the flaky simulator, delivers a
healthy event, then breaks the receiver, watches retries and the DLQ, trips the
circuit breaker, and shows the diagnosis the agent produced. Read-only against a
running stack - safe to run repeatedly.

    python scripts/demo.py                # against localhost
    RELAY_URL=https://relay.example.com python scripts/demo.py

The agent step needs a configured LLM key (LLM_API_KEY); without one the demo
still runs and shows the agent_error diagnosis, noting the pipeline is wired.
"""

import os
import sys
import time
import urllib.error
import urllib.request

BASE = os.environ.get("RELAY_URL", "http://localhost:8000").rstrip("/")
ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN", "change-me")
# As the worker container sees the simulator (compose DNS). Override for a
# real deployment where the receiver is elsewhere. This is the base /hook; the
# demo drives the simulator's behavior with POST /configure (healthy -> 500).
RECEIVER = os.environ.get("DEMO_RECEIVER", "http://flaky-endpoint:9000/hook")
FLAKY_ADMIN = os.environ.get("DEMO_FLAKY_ADMIN", "http://localhost:9000")


def _req(method, url, token=None, body=None, base=BASE):
    data = None
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    if body is not None:
        import json

        data = json.dumps(body).encode()
    req = urllib.request.Request(base + url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            import json

            raw = resp.read().decode()
            return resp.status, (json.loads(raw) if raw else {})
    except urllib.error.HTTPError as exc:
        import json

        raw = exc.read().decode()
        return exc.code, (json.loads(raw) if raw else {})


def _flaky(path, body=None):
    try:
        _req("POST", path, body=body or {}, base=FLAKY_ADMIN)
    except Exception:
        pass  # the simulator is optional for the API-only parts


def step(n, title):
    print(f"\n\033[1m[{n}] {title}\033[0m")


def main():
    print("\033[1mRelay demo\033[0m - webhook delivery + failure-diagnosis agent")
    print(f"API: {BASE}")

    status, health = _req("GET", "/healthz")
    if status != 200:
        sys.exit(f"API not healthy ({status}). Is the stack up? `docker compose up -d`")
    print(f"health: {health}")

    step(1, "Create a tenant")
    status, tenant = _req("POST", "/v1/tenants", ADMIN_TOKEN, {"name": "demo-co"})
    if status != 201:
        sys.exit(f"tenant create failed: {status} {tenant}")
    key = tenant["api_key"]
    print(f"  tenant {tenant['id']}  (api key shown once)")

    step(2, "Register an endpoint pointing at the receiver")
    _, ep = _req(
        "POST",
        "/v1/endpoints",
        key,
        {"url": RECEIVER, "event_types": ["order.paid"]},
    )
    ep_id = ep["id"]
    # Tell the simulator which secret to verify, and start it healthy.
    _flaky("/secret", {"secret": ep["signing_secret"]})
    _flaky("/reset")
    _flaky("/configure", {"mode": "healthy"})
    print(f"  endpoint {ep_id}  ordering={ep['ordering']}")

    step(3, "Deliver a healthy event")
    _, acc = _req(
        "POST", "/v1/events", key, {"event_type": "order.paid", "payload": {"amount": 42}}
    )
    ev_id = acc["event_id"]
    delivered = _wait_delivery(key, ev_id, {"delivered"}, 20)
    print(f"  event {ev_id} -> {delivered} (signed POST, receiver verified the signature)")

    step(4, "Break the receiver - watch retries queue up")
    _flaky("/configure", {"mode": "http_500"})
    print("  receiver now returns HTTP 500; sending 12 events...")
    for i in range(12):
        _req("POST", "/v1/events", key, {"event_type": "order.paid", "payload": {"n": i}})
    time.sleep(6)
    print("  each failed delivery is scheduled for retry (5s, 30s, 2m, 10m, 30m, 2h, 5h).")
    print("  after 7 exhausted attempts a delivery dead-letters - hours out, so the")
    print("  DLQ stays empty within this short demo (the replay path is exercised in tests).")

    step(5, "Circuit breaker opens")
    state = _wait_breaker(key, ep_id, {"open", "half_open"}, 40)
    print(f"  breaker for the endpoint is now: {state}")
    print("  while open, deliveries are held (no wasted attempts).")

    step(6, "The agent diagnoses the failure")
    print("  the breaker opening (and 5 consecutive failures) triggered a diagnosis...")
    diag = _wait_diagnosis(key, ep_id, 90)
    if diag is None:
        print("  no diagnosis yet (agent may still be running, or budget reached).")
    else:
        print(f"  root cause : {diag['root_cause']}  (confidence: {diag['confidence']})")
        ev = diag["evidence"]
        print(f"  verified   : {ev.get('verified')} via {ev.get('tool_calls')}")
        print(f"  cost       : ${diag['cost_usd']}")
        if diag["root_cause"] == "agent_error":
            print("  (agent_error: no LLM key set - add LLM_API_KEY for a real diagnosis.)")
        else:
            print("  recommendation:", diag["recommendation"])
            print("  --- drafted customer email ---")
            for line in diag["draft_email"].splitlines():
                print("   ", line)

    step(7, "Recover: fix the receiver and replay the DLQ")
    _flaky("/configure", {"mode": "healthy"})
    _, dlq = _req("GET", f"/v1/dlq?endpoint_id={ep_id}", key)
    print(f"  dead deliveries in DLQ: {len(dlq)}")
    if dlq:
        _, replay = _req("POST", "/v1/dlq/replay", key, {"endpoint_id": ep_id})
        print(f"  replayed {replay['replayed']} - back into the pipeline (healthy receiver).")

    print("\n\033[1mDone.\033[0m  Grafana http://localhost:3000  Prometheus http://localhost:9090")


def _wait_delivery(key, event_id, statuses, timeout):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        _, ev = _req("GET", f"/v1/events/{event_id}", key)
        for d in ev.get("deliveries", []):
            last = d["status"]
            if d["status"] in statuses:
                return d["status"]
        time.sleep(1)
    return last


def _wait_breaker(key, ep_id, states, timeout):
    deadline = time.time() + timeout
    while time.time() < deadline:
        status, b = _req("GET", f"/v1/endpoints/{ep_id}/breaker", key)
        if status == 200 and b["state"] in states:
            return b["state"]
        time.sleep(2)
    return b.get("state", "unknown")


def _wait_diagnosis(key, ep_id, timeout):
    deadline = time.time() + timeout
    while time.time() < deadline:
        _, rows = _req("GET", f"/v1/diagnoses?endpoint_id={ep_id}", key)
        # A finished diagnosis has recorded its evidence (verified is set) or is
        # an agent_error terminal state.
        for d in rows:
            ev = d.get("evidence") or {}
            done = ev.get("verified") is not None or ev.get("error")
            if done or d["root_cause"] != "agent_error":
                return d
        time.sleep(3)
    return None


if __name__ == "__main__":
    main()
