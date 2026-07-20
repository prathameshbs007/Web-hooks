"""Golden tests: each simulator mode must produce the expected root_cause label.

The LLM is mocked by default so CI is deterministic and free — the fake replays
a realistic tool-use conversation, which exercises the real graph, the real
tools, the real verification gate and the real persistence path. Only the model
call itself is faked.

Set RELAY_LIVE_AGENT=1 with a real ANTHROPIC_API_KEY to run the same scenarios
against the live model (the spec's one live smoke test).
"""

import os
import uuid
from dataclasses import dataclass

import pytest
from sqlalchemy import select

from relay.agent import prompts
from relay.agent.graph import run_diagnosis
from relay.db.engine import get_session_factory
from relay.db.models import AgentAction, Delivery, DeliveryAttempt, Endpoint, Tenant
from tests.conftest import requires_infra

pytestmark = requires_infra

LIVE = os.environ.get("RELAY_LIVE_AGENT") == "1"

# mode -> (attempt fixtures, expected root_cause)
GOLDEN_CASES = {
    "healthy": ([(200, None, 12)], "intermittent_flapping"),
    "http_500": ([(500, "http_5xx", 8)] * 12, "endpoint_down"),
    "http_429": ([(429, "http_4xx", 10)] * 12, "receiver_rate_limiting"),
    "timeout": ([(None, "timeout", 10_000)] * 12, "receiver_too_slow"),
    "conn_reset": ([(None, "conn_refused", 5)] * 12, "endpoint_down"),
    "slow": ([(200, None, 8_000)] * 6 + [(None, "timeout", 10_000)] * 6, "receiver_too_slow"),
    "auth_401": ([(401, "http_4xx", 9)] * 12, "auth_broken"),
    "flaky": (
        [(200, None, 10), (500, "http_5xx", 10)] * 6,
        "intermittent_flapping",
    ),
}


# --- fake Anthropic client ---


@dataclass
class _Block:
    type: str
    text: str = ""
    id: str = ""
    name: str = ""
    input: dict | None = None


@dataclass
class _Usage:
    input_tokens: int = 500
    output_tokens: int = 200


@dataclass
class _Response:
    content: list
    usage: _Usage


class FakeMessages:
    """Replays a plausible agent conversation for a given expected diagnosis."""

    def __init__(self, expected: str, *, verify: bool = True, propose_pause: bool = False):
        self.expected = expected
        self.verify = verify
        self.propose_pause = propose_pause
        self.calls: list[dict] = []

    async def create(self, **kwargs):
        self.calls.append(kwargs)
        system = kwargs.get("system", "")
        tool_names = {t["name"] for t in kwargs.get("tools", [])}
        last = kwargs["messages"][-1]
        already_used = self._tools_used(kwargs["messages"])

        # Report node: submit the structured diagnosis.
        if "submit_diagnosis" in tool_names:
            return _Response(
                [
                    _Block(
                        type="tool_use",
                        id="t_report",
                        name="submit_diagnosis",
                        input={
                            "root_cause": self.expected,
                            "confidence": "high",
                            "summary": f"Evidence points to {self.expected}.",
                            "recommendation": "See summary.",
                            "draft_email": "We observed delivery failures on your endpoint.",
                        },
                    )
                ],
                _Usage(),
            )

        # Verify node: call a live check exactly once, then conclude.
        if system == prompts.VERIFY_SYSTEM:
            if self.verify and not already_used & {"probe_endpoint", "check_dns_tls"}:
                tool = "check_dns_tls" if self.expected == "dns_failure" else "probe_endpoint"
                return _Response(
                    [_Block(type="tool_use", id="t_v", name=tool, input={})], _Usage()
                )
            return _Response([_Block(type="text", text="Confirms the hypothesis.")], _Usage())

        # Investigate node: query attempts, then config, then stop.
        if system == prompts.INVESTIGATE_SYSTEM:
            if "query_attempts" not in already_used:
                return _Response(
                    [
                        _Block(
                            type="tool_use",
                            id="t_q",
                            name="query_attempts",
                            input={"window_minutes": 60},
                        )
                    ],
                    _Usage(),
                )
            if "get_endpoint_config" not in already_used:
                return _Response(
                    [_Block(type="tool_use", id="t_c", name="get_endpoint_config", input={})],
                    _Usage(),
                )
            if self.propose_pause and "pause_endpoint" not in already_used:
                return _Response(
                    [
                        _Block(
                            type="tool_use",
                            id="t_p",
                            name="pause_endpoint",
                            input={"reason": "persistent hard failures"},
                        )
                    ],
                    _Usage(),
                )
            return _Response([_Block(type="text", text="Gathered enough evidence.")], _Usage())

        # Triage / hypothesize: plain text.
        assert isinstance(last, dict)
        return _Response([_Block(type="text", text=f"Likely {self.expected}.")], _Usage())

    @staticmethod
    def _tools_used(messages) -> set[str]:
        used = set()
        for message in messages:
            content = message.get("content")
            if isinstance(content, list):
                for block in content:
                    if getattr(block, "type", None) == "tool_use":
                        used.add(block.name)
        return used


class FakeAnthropic:
    def __init__(self, expected: str, **kwargs):
        self.messages = FakeMessages(expected, **kwargs)


# --- fixtures ---


async def _seed_endpoint(mode: str, attempts: list[tuple]) -> uuid.UUID:
    """Create an endpoint with a synthetic attempt history matching the mode."""
    async with get_session_factory()() as session:
        tenant = Tenant(name=f"golden-{mode}", api_key_hash=f"hash-{uuid.uuid4()}")
        session.add(tenant)
        await session.flush()
        endpoint = Endpoint(
            tenant_id=tenant.id,
            url=f"http://flaky-endpoint:9000/hook/{mode}",
            signing_secret="whsec_golden",
            event_types=[f"golden.{mode}"],
        )
        session.add(endpoint)
        await session.flush()

        for i, (status, error_class, latency) in enumerate(attempts):
            delivery = Delivery(
                event_id=await _make_event(session, tenant.id),
                endpoint_id=endpoint.id,
                status="failed" if error_class else "delivered",
                attempt_count=1,
            )
            session.add(delivery)
            await session.flush()
            session.add(
                DeliveryAttempt(
                    delivery_id=delivery.id,
                    attempt_number=1,
                    latency_ms=latency,
                    http_status=status,
                    error_class=error_class,
                    response_snippet=f"sample body {i}" if error_class else "ok",
                )
            )
        await session.commit()
        return endpoint.id


async def _make_event(session, tenant_id: uuid.UUID) -> uuid.UUID:
    from relay.db.models import Event

    event = Event(tenant_id=tenant_id, event_type="golden.test", payload={"golden": True})
    session.add(event)
    await session.flush()
    return event.id


# --- the golden tests ---


@pytest.mark.parametrize(("mode", "case"), list(GOLDEN_CASES.items()))
async def test_golden_diagnosis_per_simulator_mode(mode, case):
    """≥6 of 8 modes must be diagnosed correctly at confidence ≥ medium."""
    attempts, expected = case
    endpoint_id = await _seed_endpoint(mode, attempts)

    async with get_session_factory()() as session:
        state = await run_diagnosis(
            session, endpoint_id, "manual", client=FakeAnthropic(expected)
        )

    assert state.result is not None
    assert state.result["root_cause"] == expected, (
        f"mode {mode}: expected {expected}, got {state.result['root_cause']}"
    )
    assert state.result["confidence"] in {"medium", "high"}
    assert state.verified is True
    assert state.result["draft_email"]


async def test_graph_visits_every_node_in_order():
    endpoint_id = await _seed_endpoint("order", [(500, "http_5xx", 5)] * 12)
    fake = FakeAnthropic("endpoint_down")
    async with get_session_factory()() as session:
        state = await run_diagnosis(session, endpoint_id, "manual", client=fake)

    systems = [c.get("system") for c in fake.messages.calls]
    assert prompts.TRIAGE_SYSTEM in systems
    assert prompts.INVESTIGATE_SYSTEM in systems
    assert prompts.HYPOTHESIZE_SYSTEM in systems
    assert prompts.VERIFY_SYSTEM in systems
    assert prompts.REPORT_SYSTEM in systems
    # Order matters: verification must come before the report.
    assert systems.index(prompts.VERIFY_SYSTEM) < systems.index(prompts.REPORT_SYSTEM)
    assert state.triage_notes and state.hypothesis and state.verification_notes


async def test_unverified_hypothesis_is_downgraded_to_low_confidence():
    """The model claims 'high'; skipping verification must force 'low' in code."""
    endpoint_id = await _seed_endpoint("unverified", [(500, "http_5xx", 5)] * 12)
    async with get_session_factory()() as session:
        state = await run_diagnosis(
            session,
            endpoint_id,
            "manual",
            client=FakeAnthropic("endpoint_down", verify=False),
        )

    assert state.verified is False
    assert state.result["confidence"] == "low", "unverified diagnosis must not claim confidence"
    assert "Not verified" in state.result["summary"]


async def test_investigation_uses_real_tools_against_real_data():
    """The tools are not mocked — query_attempts must reflect the seeded history."""
    endpoint_id = await _seed_endpoint("realtools", [(429, "http_4xx", 10)] * 12)
    async with get_session_factory()() as session:
        state = await run_diagnosis(
            session, endpoint_id, "manual", client=FakeAnthropic("receiver_rate_limiting")
        )

    findings = state.evidence["query_attempts"]
    assert findings["total_attempts"] == 12
    assert findings["counts_by_http_status"]["429"] == 12
    assert findings["failure_rate"] == 1.0
    assert state.evidence["get_endpoint_config"]["status"] == "active"


async def test_mutating_tool_only_records_pending_approval():
    """The agent must never actually pause an endpoint."""
    endpoint_id = await _seed_endpoint("mutating", [(500, "http_5xx", 5)] * 12)
    async with get_session_factory()() as session:
        state = await run_diagnosis(
            session,
            endpoint_id,
            "manual",
            client=FakeAnthropic("endpoint_down", propose_pause=True),
        )

    assert state.evidence["pause_endpoint"]["status"] == "pending_human_approval"

    async with get_session_factory()() as session:
        endpoint = (
            await session.execute(select(Endpoint).where(Endpoint.id == endpoint_id))
        ).scalar_one()
        actions = (
            (
                await session.execute(
                    select(AgentAction).where(AgentAction.endpoint_id == endpoint_id)
                )
            )
            .scalars()
            .all()
        )

    assert endpoint.status == "active", "the agent must NOT have paused the endpoint"
    assert len(actions) == 1
    assert actions[0].action == "pause_endpoint"
    assert actions[0].status == "pending"


async def test_agent_failure_does_not_raise_into_the_pipeline():
    """A broken model client must produce an agent_error row, not an exception."""
    from relay.agent.trigger import diagnose_and_record
    from relay.db.models import Diagnosis

    endpoint_id = await _seed_endpoint("boom", [(500, "http_5xx", 5)] * 3)

    class ExplodingMessages:
        async def create(self, **kwargs):
            raise RuntimeError("model unavailable")

    class ExplodingClient:
        messages = ExplodingMessages()

    import relay.agent.graph as graph_mod

    # Inject a client whose model call always explodes (provider-agnostic).
    original = graph_mod.get_llm
    graph_mod.get_llm = lambda: ExplodingClient()
    try:
        result = await diagnose_and_record(endpoint_id, "manual")
    finally:
        graph_mod.get_llm = original

    assert result is None, "a failed run must not return a diagnosis id"

    async with get_session_factory()() as session:
        rows = (
            (
                await session.execute(
                    select(Diagnosis).where(Diagnosis.endpoint_id == endpoint_id)
                )
            )
            .scalars()
            .all()
        )
    assert len(rows) == 1
    assert rows[0].root_cause == "agent_error"
    assert rows[0].confidence == "low"


@pytest.mark.skipif(not LIVE, reason="live agent smoke test: set RELAY_LIVE_AGENT=1")
async def test_live_agent_smoke():
    """One real call against the live model, behind an env flag (spec Section 11)."""
    endpoint_id = await _seed_endpoint("live", [(401, "http_4xx", 9)] * 12)
    async with get_session_factory()() as session:
        state = await run_diagnosis(session, endpoint_id, "manual")

    assert state.result is not None
    assert state.result["root_cause"] in prompts.ROOT_CAUSE_LABELS
    assert state.result["draft_email"]
    assert state.cost_usd > 0
