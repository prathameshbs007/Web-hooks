"""Unit tests for the provider-neutral LLM adapter (relay/agent/llm.py).

These need no provider package and no network — they exercise the translation
and the rate-limit retry against a fake LangChain model.
"""

from dataclasses import dataclass, field

import pytest

from relay.agent import llm as llm_mod
from relay.agent.llm import (
    PRICING,
    LLMClient,
    _from_lc_response,
    _is_rate_limit,
    _to_lc_messages,
    _to_lc_tools,
    estimate_cost,
)


class ResourceExhausted(Exception):
    """Stand-in for google.api_core.exceptions.ResourceExhausted."""


@dataclass
class FakeAI:
    content: str = ""
    tool_calls: list = field(default_factory=list)
    usage_metadata: dict = field(default_factory=lambda: {"input_tokens": 10, "output_tokens": 5})


class FakeModel:
    def __init__(self, responses=None, fail_times=0, exc=None):
        self.responses = responses or [FakeAI(content="ok")]
        self.fail_times = fail_times
        self.exc = exc or ResourceExhausted("429 quota exceeded")
        self.calls = 0
        self.bound_tools = None

    def bind_tools(self, tools):
        self.bound_tools = tools
        return self

    async def ainvoke(self, messages):
        self.calls += 1
        if self.fail_times > 0:
            self.fail_times -= 1
            raise self.exc
        return self.responses[min(self.calls - 1, len(self.responses) - 1)]


# --- rate-limit detection ---


@pytest.mark.parametrize(
    "exc",
    [
        ResourceExhausted("boom"),
        RuntimeError("429 Too Many Requests"),
        RuntimeError("Resource has been exhausted (e.g. check quota)."),
        RuntimeError("rate limit reached"),
    ],
)
def test_is_rate_limit_detects_429s(exc):
    assert _is_rate_limit(exc) is True


@pytest.mark.parametrize("exc", [ValueError("bad schema"), RuntimeError("500 server error")])
def test_is_rate_limit_ignores_other_errors(exc):
    assert _is_rate_limit(exc) is False


# --- retry behaviour ---


async def test_adapter_retries_then_succeeds(monkeypatch):
    monkeypatch.setattr(llm_mod, "RETRY_BASE_DELAY_S", 0.001)
    model = FakeModel(responses=[FakeAI(content="recovered")], fail_times=2)
    client = LLMClient(model)

    resp = await client.messages.create(system="s", messages=[{"role": "user", "content": "hi"}])

    assert model.calls == 3, "should retry twice before succeeding"
    assert resp.content[0].text == "recovered"


async def test_adapter_gives_up_after_two_retries(monkeypatch):
    monkeypatch.setattr(llm_mod, "RETRY_BASE_DELAY_S", 0.001)
    model = FakeModel(fail_times=99)  # always rate-limited
    client = LLMClient(model)

    with pytest.raises(ResourceExhausted):
        await client.messages.create(system="s", messages=[{"role": "user", "content": "hi"}])
    assert model.calls == 3, "one initial attempt + two retries, then propagate"


async def test_non_rate_limit_error_is_not_retried(monkeypatch):
    monkeypatch.setattr(llm_mod, "RETRY_BASE_DELAY_S", 0.001)
    model = FakeModel(fail_times=99, exc=ValueError("bad request"))
    client = LLMClient(model)

    with pytest.raises(ValueError):
        await client.messages.create(system="s", messages=[{"role": "user", "content": "hi"}])
    assert model.calls == 1, "a non-429 error must fail immediately"


# --- response translation ---


async def test_tool_call_translates_to_anthropic_block():
    model = FakeModel(
        responses=[
            FakeAI(
                content="",
                tool_calls=[{"name": "query_attempts", "args": {"window_minutes": 60}, "id": "c1"}],
            )
        ]
    )
    client = LLMClient(model)
    resp = await client.messages.create(
        system="s",
        messages=[{"role": "user", "content": "go"}],
        tools=[{"name": "query_attempts", "description": "d", "input_schema": {"type": "object"}}],
    )

    block = resp.content[0]
    assert block.type == "tool_use"
    assert block.name == "query_attempts"
    assert block.input == {"window_minutes": 60}
    assert block.id == "c1"
    assert resp.usage.input_tokens == 10 and resp.usage.output_tokens == 5
    # tools were bound in OpenAI function shape
    assert model.bound_tools[0]["function"]["name"] == "query_attempts"


def test_message_translation_round_trips_tool_use():
    """An assistant tool_use + a following tool_result must map to AIMessage+ToolMessage."""
    from relay.agent.llm import _Block

    messages = [
        {"role": "user", "content": "investigate"},
        {
            "role": "assistant",
            "content": [_Block(type="tool_use", id="c1", name="probe", input={})],
        },
        {
            "role": "user",
            "content": [{"type": "tool_result", "tool_use_id": "c1", "content": "ok"}],
        },
    ]
    lc = _to_lc_messages("system prompt", messages)
    types = [type(m).__name__ for m in lc]
    assert types == ["SystemMessage", "HumanMessage", "AIMessage", "ToolMessage"]
    assert lc[2].tool_calls[0]["id"] == "c1"
    assert lc[3].tool_call_id == "c1"


def test_from_lc_response_handles_list_content():
    ai = FakeAI(content=[{"type": "text", "text": "hello "}, {"type": "text", "text": "world"}])
    resp = _from_lc_response(ai)
    assert resp.content[0].text == "hello world"


def test_tool_schema_conversion():
    out = _to_lc_tools([{"name": "t", "description": "d", "input_schema": {"type": "object"}}])
    assert out[0] == {
        "type": "function",
        "function": {"name": "t", "description": "d", "parameters": {"type": "object"}},
    }


def test_cost_is_non_zero_for_both_providers(monkeypatch):
    for provider in ("gemini", "anthropic"):
        monkeypatch.setattr(
            llm_mod, "get_settings", lambda p=provider: type("S", (), {"llm_provider": p})()
        )
        assert estimate_cost(1000, 1000) > 0
    assert set(PRICING) == {"gemini", "anthropic"}
