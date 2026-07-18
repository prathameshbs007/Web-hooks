"""Unit tests for the sender: headers, classification, snippet truncation."""

import uuid

import httpx

from relay.delivery.sender import (
    RESPONSE_SNIPPET_LIMIT,
    build_headers,
    classify_exception,
    classify_status,
    send_delivery,
    serialize_payload,
)
from relay.signing import verify_signature

SECRET = "whsec_x"


def test_classify_status():
    assert classify_status(200) is None
    assert classify_status(204) is None
    assert classify_status(400) == "http_4xx"
    assert classify_status(429) == "http_4xx"
    assert classify_status(500) == "http_5xx"
    assert classify_status(503) == "http_5xx"


def test_classify_exception():
    assert classify_exception(httpx.ReadTimeout("slow")) == "timeout"
    assert classify_exception(httpx.ConnectTimeout("slow")) == "timeout"
    assert classify_exception(httpx.ConnectError("connection refused")) == "conn_refused"


def test_build_headers_are_verifiable():
    delivery_id, event_id = uuid.uuid4(), uuid.uuid4()
    body = serialize_payload({"amount": 42})
    headers = build_headers(delivery_id, event_id, SECRET, body, 1700000000)

    assert headers["Relay-Id"] == str(delivery_id)
    assert headers["Relay-Event-Id"] == str(event_id)
    assert headers["Relay-Timestamp"] == "1700000000"
    assert headers["Relay-Signature"].startswith("v1=")
    assert verify_signature(
        SECRET, 1700000000, body, headers["Relay-Signature"], now=1700000000
    )


async def test_send_delivery_success():
    def handler(request: httpx.Request) -> httpx.Response:
        assert verify_signature(
            SECRET,
            int(request.headers["Relay-Timestamp"]),
            request.content,
            request.headers["Relay-Signature"],
        )
        return httpx.Response(200, text="ok")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        result = await send_delivery(
            client,
            url="http://receiver.test/hook",
            secret=SECRET,
            delivery_id=uuid.uuid4(),
            event_id=uuid.uuid4(),
            payload={"amount": 42},
        )
    assert result.succeeded
    assert result.http_status == 200
    assert result.error_class is None


async def test_send_delivery_records_5xx_as_failure():
    transport = httpx.MockTransport(lambda r: httpx.Response(503, text="down"))
    async with httpx.AsyncClient(transport=transport) as client:
        result = await send_delivery(
            client,
            url="http://receiver.test/hook",
            secret=SECRET,
            delivery_id=uuid.uuid4(),
            event_id=uuid.uuid4(),
            payload={},
        )
    assert not result.succeeded
    assert result.error_class == "http_5xx"
    assert result.http_status == 503


async def test_send_delivery_converts_transport_error_to_result():
    def boom(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("timed out")

    async with httpx.AsyncClient(transport=httpx.MockTransport(boom)) as client:
        result = await send_delivery(
            client,
            url="http://receiver.test/hook",
            secret=SECRET,
            delivery_id=uuid.uuid4(),
            event_id=uuid.uuid4(),
            payload={},
        )
    assert not result.succeeded
    assert result.error_class == "timeout"
    assert result.http_status is None


async def test_response_snippet_truncated_to_1kb():
    transport = httpx.MockTransport(lambda r: httpx.Response(500, text="x" * 5000))
    async with httpx.AsyncClient(transport=transport) as client:
        result = await send_delivery(
            client,
            url="http://receiver.test/hook",
            secret=SECRET,
            delivery_id=uuid.uuid4(),
            event_id=uuid.uuid4(),
            payload={},
        )
    assert len(result.response_snippet) == RESPONSE_SNIPPET_LIMIT
