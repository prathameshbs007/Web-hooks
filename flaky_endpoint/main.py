"""Configurable failure simulator + reference receiver implementation.

`POST /configure {mode, param?}` switches how `POST /hook` behaves. It verifies
Relay signatures on every request, so it doubles as the copy-paste example of
correct receiver-side verification.
"""

import asyncio
import os
from typing import Literal

from fastapi import FastAPI, Request, Response
from pydantic import BaseModel

from relay.signing import verify_signature

MODES = (
    "healthy",
    "http_500",
    "http_429",
    "timeout",
    "conn_reset",
    "slow",
    "auth_401",
    "flaky",
)

Mode = Literal[
    "healthy", "http_500", "http_429", "timeout", "conn_reset", "slow", "auth_401", "flaky"
]

app = FastAPI(title="flaky-endpoint")

# Process-local state; this is a test fixture, not a production service.
state: dict = {
    "mode": "healthy",
    "param": None,
    # Signing secret is injected by whoever creates the endpoint in Relay.
    "secret": os.environ.get("FLAKY_SIGNING_SECRET", ""),
    "received": [],
}


class _TruncatedResponse(Response):
    """Promise a body, then close without sending it.

    Raising ConnectionResetError inside a handler doesn't reach the wire —
    uvicorn catches it and returns a normal HTTP 500, which is indistinguishable
    from the http_500 mode. Breaking the response at the protocol level is what
    actually surfaces to the sender as a transport error.
    """

    async def __call__(self, scope, receive, send) -> None:
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [(b"content-length", b"100"), (b"content-type", b"text/plain")],
            }
        )
        await send({"type": "http.response.body", "body": b"", "more_body": False})


class Configure(BaseModel):
    mode: Mode
    param: float | None = None


class SetSecret(BaseModel):
    secret: str


@app.post("/configure")
async def configure(body: Configure) -> dict:
    state["mode"] = body.mode
    state["param"] = body.param
    return {"mode": state["mode"], "param": state["param"]}


@app.post("/secret")
async def set_secret(body: SetSecret) -> dict:
    """Tell the simulator which signing secret to verify against."""
    state["secret"] = body.secret
    return {"ok": True}


@app.get("/received")
async def received() -> dict:
    return {"count": len(state["received"]), "items": state["received"][-50:]}


@app.post("/reset")
async def reset() -> dict:
    state["received"].clear()
    state["mode"] = "healthy"
    state["param"] = None
    return {"ok": True}


@app.post("/hook/{mode_override}")
async def hook_with_mode(mode_override: Mode, request: Request) -> Response:
    """Same as /hook but pins the behavior for this request only.

    Lets concurrent tests target different behaviors without fighting over the
    global mode — a `timeout` test would otherwise make every other endpoint's
    deliveries sleep too, starving the workers.
    """
    return await _handle(request, mode_override)


@app.post("/hook")
async def hook(request: Request) -> Response:
    return await _handle(request, state["mode"])


async def _handle(request: Request, mode: str) -> Response:
    raw_body = await request.body()

    # --- reference receiver verification ---
    timestamp_header = request.headers.get("Relay-Timestamp", "0")
    signature = request.headers.get("Relay-Signature", "")
    valid = False
    if state["secret"]:
        try:
            valid = verify_signature(
                state["secret"], int(timestamp_header), raw_body, signature
            )
        except ValueError:
            valid = False

    state["received"].append(
        {
            "delivery_id": request.headers.get("Relay-Id"),
            "event_id": request.headers.get("Relay-Event-Id"),
            "signature_valid": valid,
            "body": raw_body.decode(errors="replace")[:512],
        }
    )

    if mode == "healthy":
        return Response(status_code=200, content='{"ok":true}', media_type="application/json")
    if mode == "http_500":
        return Response(status_code=500, content="internal error")
    if mode == "http_429":
        retry_after = str(int(state["param"] or 1))
        return Response(status_code=429, content="slow down", headers={"Retry-After": retry_after})
    if mode == "auth_401":
        return Response(status_code=401, content="unauthorized")
    if mode == "timeout":
        await asyncio.sleep(state["param"] or 30)
        return Response(status_code=200, content="late")
    if mode == "slow":
        await asyncio.sleep(state["param"] or 8)
        return Response(status_code=200, content='{"ok":true}', media_type="application/json")
    if mode == "conn_reset":
        return _TruncatedResponse()
    if mode == "flaky":
        import random

        if random.random() < (state["param"] or 0.3):
            return Response(status_code=500, content="flaky failure")
        return Response(status_code=200, content='{"ok":true}', media_type="application/json")

    return Response(status_code=200, content='{"ok":true}', media_type="application/json")


@app.get("/healthz")
async def healthz() -> dict:
    return {"status": "ok", "mode": state["mode"]}
