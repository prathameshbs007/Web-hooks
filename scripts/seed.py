#!/usr/bin/env python3
"""Seed a tenant + endpoint and print the credentials, for manual poking.

    python scripts/seed.py
    RELAY_URL=http://localhost:8000 python scripts/seed.py
"""

import json
import os
import urllib.request

BASE = os.environ.get("RELAY_URL", "http://localhost:8000").rstrip("/")
ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN", "change-me")
RECEIVER = os.environ.get("SEED_RECEIVER", "http://flaky-endpoint:9000/hook")


def _post(url, token, body):
    req = urllib.request.Request(
        BASE + url,
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {token}"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read().decode())


def main():
    tenant = _post("/v1/tenants", ADMIN_TOKEN, {"name": "seed-tenant"})
    key = tenant["api_key"]
    endpoint = _post("/v1/endpoints", key, {"url": RECEIVER, "event_types": []})

    print("Seeded a tenant and endpoint.\n")
    print(f"  RELAY_API_KEY={key}")
    print(f"  ENDPOINT_ID={endpoint['id']}")
    print(f"  SIGNING_SECRET={endpoint['signing_secret']}\n")
    print("Fire an event:")
    print(
        f'  curl -X POST {BASE}/v1/events -H "Authorization: Bearer {key}" \\\n'
        '    -H "Content-Type: application/json" \\\n'
        '    -d \'{"event_type":"order.paid","payload":{"amount":42}}\''
    )


if __name__ == "__main__":
    main()
