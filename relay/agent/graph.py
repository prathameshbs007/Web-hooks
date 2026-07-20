"""LangGraph workflow: triage → investigate → hypothesize → verify → report.

The loop is the point of the project, so it is not collapsed into one LLM call.
`verify` is mandatory: a hypothesis that was never tested against a live probe
or DNS/TLS check has its confidence forced down to 'low' in code, regardless of
what the model claims — the model does not get to grade its own rigour.
"""

import uuid
from dataclasses import dataclass, field
from typing import Any

from langgraph.graph import END, StateGraph
from sqlalchemy.ext.asyncio import AsyncSession

from relay.agent import prompts, tools
from relay.agent.llm import estimate_cost, get_llm
from relay.config import get_settings
from relay.db.models import Diagnosis
from relay.observability import get_logger

log = get_logger(__name__)

MAX_INVESTIGATE_TOOL_CALLS = 6
MAX_TOKENS = 4096

# Hard cost ceiling per run, independent of provider. On a free tier this never
# binds; on a paid provider it stops a runaway loop.
MAX_COST_USD_PER_RUN = 0.50


@dataclass
class AgentState:
    """Carried through every node."""

    endpoint_id: uuid.UUID
    diagnosis_id: uuid.UUID
    triggered_by: str
    session: Any = None
    client: Any = None

    triage_notes: str = ""
    hypothesis: str = ""
    verification_notes: str = ""
    evidence: dict = field(default_factory=dict)
    tool_calls_made: list[str] = field(default_factory=list)
    verified: bool = False
    result: dict | None = None
    input_tokens: int = 0
    output_tokens: int = 0

    @property
    def cost_usd(self) -> float:
        return estimate_cost(self.input_tokens, self.output_tokens)

    def over_budget(self) -> bool:
        return self.cost_usd >= MAX_COST_USD_PER_RUN


def _text_of(message) -> str:
    return "\n".join(b.text for b in message.content if b.type == "text").strip()


async def _call_model(state: AgentState, *, system: str, messages: list, tool_defs=None):
    kwargs: dict = {
        "model": get_settings().llm_model,
        "max_tokens": MAX_TOKENS,
        "system": system,
        "messages": messages,
    }
    if tool_defs:
        kwargs["tools"] = tool_defs
    response = await state.client.messages.create(**kwargs)
    state.input_tokens += response.usage.input_tokens
    state.output_tokens += response.usage.output_tokens
    return response


def _context_block(state: AgentState) -> str:
    lines = [f"Endpoint: {state.endpoint_id}", f"Trigger: {state.triggered_by}"]
    if state.triage_notes:
        lines.append(f"\nTriage notes:\n{state.triage_notes}")
    if state.evidence:
        lines.append("\nEvidence gathered so far:")
        for name, value in state.evidence.items():
            lines.append(f"- {name}: {value}")
    if state.hypothesis:
        lines.append(f"\nHypothesis:\n{state.hypothesis}")
    if state.verification_notes:
        lines.append(f"\nVerification:\n{state.verification_notes}")
    return "\n".join(lines)


async def triage_node(state: AgentState) -> AgentState:
    response = await _call_model(
        state,
        system=prompts.TRIAGE_SYSTEM,
        messages=[
            {
                "role": "user",
                "content": (
                    f"Endpoint {state.endpoint_id} triggered a diagnosis "
                    f"({state.triggered_by}). What will you check?"
                ),
            }
        ],
    )
    state.triage_notes = _text_of(response)
    log.info("agent_triage_done", endpoint_id=str(state.endpoint_id))
    return state


async def _run_tool_loop(
    state: AgentState, *, system: str, opening: str, tool_defs: list, max_calls: int
) -> str:
    """Shared tool-use loop. Returns the model's closing text."""
    messages: list = [{"role": "user", "content": opening}]
    calls = 0
    while calls < max_calls and not state.over_budget():
        response = await _call_model(
            state, system=system, messages=messages, tool_defs=tool_defs
        )
        # Echo the assistant turn back verbatim — required for tool-use continuity.
        messages.append({"role": "assistant", "content": response.content})
        tool_uses = [b for b in response.content if b.type == "tool_use"]
        if not tool_uses:
            return _text_of(response)

        results = []
        for block in tool_uses:
            calls += 1
            result = await tools.dispatch(
                block.name,
                block.input or {},
                state.session,
                state.endpoint_id,
                state.diagnosis_id,
            )
            state.tool_calls_made.append(block.name)
            state.evidence[block.name] = result
            if block.name in tools.VERIFICATION_TOOLS:
                state.verified = True
            results.append(
                {"type": "tool_result", "tool_use_id": block.id, "content": str(result)}
            )
        messages.append({"role": "user", "content": results})

    # Budget or call cap hit — ask for a wrap-up without tools.
    response = await _call_model(
        state,
        system=system,
        messages=messages + [{"role": "user", "content": "Summarize what you found."}],
    )
    return _text_of(response)


async def investigate_node(state: AgentState) -> AgentState:
    await _run_tool_loop(
        state,
        system=prompts.INVESTIGATE_SYSTEM,
        opening=f"Investigate this endpoint.\n\n{_context_block(state)}",
        tool_defs=tools.TOOL_SCHEMAS,
        max_calls=MAX_INVESTIGATE_TOOL_CALLS,
    )
    log.info(
        "agent_investigate_done",
        endpoint_id=str(state.endpoint_id),
        tools_used=state.tool_calls_made,
    )
    return state


async def hypothesize_node(state: AgentState) -> AgentState:
    response = await _call_model(
        state,
        system=prompts.HYPOTHESIZE_SYSTEM,
        messages=[{"role": "user", "content": _context_block(state)}],
    )
    state.hypothesis = _text_of(response)
    return state


async def verify_node(state: AgentState) -> AgentState:
    """Mandatory verification step — must exercise a live check."""
    verification_tools = [
        schema for schema in tools.TOOL_SCHEMAS if schema["name"] in tools.VERIFICATION_TOOLS
    ]
    state.verification_notes = await _run_tool_loop(
        state,
        system=prompts.VERIFY_SYSTEM,
        opening=(
            f"Test this hypothesis against live behavior now.\n\n{_context_block(state)}"
        ),
        tool_defs=verification_tools,
        max_calls=2,
    )
    log.info(
        "agent_verify_done", endpoint_id=str(state.endpoint_id), verified=state.verified
    )
    return state


async def report_node(state: AgentState) -> AgentState:
    response = await _call_model(
        state,
        system=prompts.REPORT_SYSTEM,
        messages=[
            {
                "role": "user",
                "content": (
                    f"Write up the diagnosis using submit_diagnosis.\n\n"
                    f"{_context_block(state)}"
                ),
            }
        ],
        tool_defs=[prompts.REPORT_TOOL],
    )
    submission = next(
        (b for b in response.content if b.type == "tool_use" and b.name == "submit_diagnosis"),
        None,
    )
    if submission is None:
        state.result = {
            "root_cause": "agent_error",
            "confidence": "low",
            "summary": "The agent did not produce a structured diagnosis.",
            "recommendation": "Re-run the diagnosis; no conclusion was reached.",
            "draft_email": "",
        }
        return state

    result = dict(submission.input)
    if not state.verified:
        # Enforced in code, not left to the model: an untested hypothesis is low
        # confidence by definition, whatever the model asserted.
        if result.get("confidence") != "low":
            log.info(
                "agent_confidence_downgraded",
                endpoint_id=str(state.endpoint_id),
                claimed=result.get("confidence"),
            )
        result["confidence"] = "low"
        result["summary"] = (
            result.get("summary", "") + " [Not verified against a live probe.]"
        ).strip()
    state.result = result
    return state


def build_graph():
    graph = StateGraph(AgentState)
    graph.add_node("triage", triage_node)
    graph.add_node("investigate", investigate_node)
    graph.add_node("hypothesize", hypothesize_node)
    graph.add_node("verify", verify_node)
    graph.add_node("report", report_node)

    graph.set_entry_point("triage")
    graph.add_edge("triage", "investigate")
    graph.add_edge("investigate", "hypothesize")
    graph.add_edge("hypothesize", "verify")
    graph.add_edge("verify", "report")
    graph.add_edge("report", END)
    return graph.compile()


async def run_diagnosis(
    session: AsyncSession,
    endpoint_id: uuid.UUID,
    triggered_by: str,
    client: Any = None,
) -> AgentState:
    """Execute one full diagnosis. Raises nothing the caller must handle.

    `client` is any object exposing `.messages.create(...)` — the tests inject a
    fake; production gets the configured provider's adapter from get_llm().
    """
    state = AgentState(
        endpoint_id=endpoint_id,
        diagnosis_id=uuid.uuid4(),
        triggered_by=triggered_by,
        session=session,
    )

    # Persist a stub before anything else — including client construction, which
    # can fail (e.g. a missing/invalid provider key). It starts as
    # 'agent_error'/'low' so any failure from here on leaves an honest record,
    # and it gives a mid-run mutation a diagnosis row to reference (agent_actions
    # FKs to diagnoses). Overwritten with the real result once the graph runs.
    stub = Diagnosis(
        id=state.diagnosis_id,
        endpoint_id=endpoint_id,
        triggered_by=triggered_by,
        root_cause="agent_error",
        confidence="low",
        evidence={},
        recommendation="",
        draft_email="",
    )
    session.add(stub)
    await session.commit()

    # Build the provider client only after the stub exists.
    state.client = client or get_llm()
    compiled = build_graph()
    # LangGraph reconstructs the dataclass per node from its channels, so the
    # final values live in ainvoke's return, not the object we passed in. Merge
    # them back so the caller reads a fully-populated state.
    final = await compiled.ainvoke(state)
    values = final if isinstance(final, dict) else vars(final)
    for key, value in values.items():
        setattr(state, key, value)

    await _persist_result(session, stub, state)
    return state


async def _persist_result(session: AsyncSession, stub: Diagnosis, state: AgentState) -> None:
    result = state.result or {}
    stub.root_cause = result.get("root_cause", "agent_error")
    stub.confidence = result.get("confidence", "low")
    stub.recommendation = result.get("recommendation", "")
    stub.draft_email = result.get("draft_email", "")
    stub.cost_usd = round(state.cost_usd, 6)
    stub.evidence = {
        "tool_calls": state.tool_calls_made,
        "verified": state.verified,
        "findings": _jsonable(state.evidence),
        "triage": state.triage_notes,
        "hypothesis": state.hypothesis,
        "verification": state.verification_notes,
        "summary": result.get("summary", ""),
    }
    await session.commit()


def _jsonable(value):
    """Best-effort conversion so tool output always survives the JSONB round trip."""
    import json

    try:
        json.dumps(value)
        return value
    except (TypeError, ValueError):
        return {k: str(v) for k, v in value.items()} if isinstance(value, dict) else str(value)
