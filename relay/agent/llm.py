"""Provider-neutral LLM client for the diagnosis agent.

The graph speaks the Anthropic Messages API shape (system + messages + tools in,
content blocks out). `get_llm()` returns an adapter that speaks that same shape
but is backed by LangChain, so the provider — Gemini by default, Anthropic as an
alternate — is a config choice. The graph and its mocked golden tests never see
the difference: the tests inject a fake with the identical `.messages.create`
interface, so CI stays free and provider-independent (the provider packages are
imported lazily, only inside get_llm()).
"""

import asyncio
import uuid
from dataclasses import dataclass, field

from relay.config import get_settings
from relay.observability import get_logger

log = get_logger(__name__)

# Free-tier providers rate-limit aggressively; back off and retry a couple of
# times before giving up (the run then fails gracefully — see graph/trigger).
RETRY_MAX = 2
RETRY_BASE_DELAY_S = 2.0
DEFAULT_MAX_TOKENS = 4096

# List prices ($/token) so cost accounting stays non-zero even on a free tier.
# gemini-2.5-flash and claude-sonnet-4-6 published input/output rates.
PRICING = {
    "gemini": (0.30 / 1_000_000, 2.50 / 1_000_000),
    "anthropic": (3.0 / 1_000_000, 15.0 / 1_000_000),
}


# --- Anthropic-shaped response objects the graph already understands ---


@dataclass
class _Block:
    type: str
    text: str = ""
    id: str = ""
    name: str = ""
    input: dict | None = None


@dataclass
class _Usage:
    input_tokens: int = 0
    output_tokens: int = 0


@dataclass
class _Response:
    content: list
    usage: _Usage = field(default_factory=_Usage)


def estimate_cost(input_tokens: int, output_tokens: int) -> float:
    inp, out = PRICING.get(get_settings().llm_provider, PRICING["gemini"])
    return input_tokens * inp + output_tokens * out


def _is_rate_limit(exc: Exception) -> bool:
    """True for a 429 / ResourceExhausted from any provider."""
    name = type(exc).__name__.lower()
    if "resourceexhausted" in name or "ratelimit" in name:
        return True
    message = str(exc).lower()
    return any(
        marker in message
        for marker in ("429", "resource has been exhausted", "quota", "rate limit")
    )


def _to_lc_messages(system: str, messages: list, cache: dict | None = None) -> list:
    """Translate the Anthropic message shape into LangChain messages.

    When `cache` maps a tool_call id to the original LangChain AIMessage that
    produced it, an assistant turn is replayed with that exact object rather than
    rebuilt. This preserves provider-specific state that a from-scratch rebuild
    would drop — notably Gemini 3.x `thought_signature` tokens, which the API
    requires echoed back on every function call.
    """
    from langchain_core.messages import (
        AIMessage,
        HumanMessage,
        SystemMessage,
        ToolMessage,
    )

    out: list = []
    if system:
        out.append(SystemMessage(content=system))

    for message in messages:
        role = message["role"]
        content = message["content"]
        if role == "user":
            if isinstance(content, str):
                out.append(HumanMessage(content=content))
                continue
            for item in content:
                if isinstance(item, dict) and item.get("type") == "tool_result":
                    out.append(
                        ToolMessage(
                            content=str(item["content"]), tool_call_id=item["tool_use_id"]
                        )
                    )
                else:
                    out.append(HumanMessage(content=str(item)))
        else:  # assistant — content is a list of blocks from a prior _Response
            tool_ids = [_attr(b, "id") for b in content if _attr(b, "type") == "tool_use"]
            replay = None
            if cache:
                replay = next((cache[i] for i in tool_ids if i in cache), None)
            if replay is not None:
                out.append(replay)  # verbatim: keeps thought_signature etc.
                continue
            text_parts: list[str] = []
            tool_calls: list[dict] = []
            for block in content:
                btype = _attr(block, "type")
                if btype == "text":
                    text_parts.append(_attr(block, "text") or "")
                elif btype == "tool_use":
                    tool_calls.append(
                        {
                            "name": _attr(block, "name"),
                            "args": _attr(block, "input") or {},
                            "id": _attr(block, "id"),
                            "type": "tool_call",
                        }
                    )
            out.append(AIMessage(content="\n".join(text_parts), tool_calls=tool_calls))
    return out


def _attr(block, name: str):
    return block.get(name) if isinstance(block, dict) else getattr(block, name, None)


def _to_lc_tools(tools: list) -> list:
    """Anthropic tool schema → OpenAI function schema (accepted by bind_tools)."""
    return [
        {
            "type": "function",
            "function": {
                "name": t["name"],
                "description": t.get("description", ""),
                "parameters": t["input_schema"],
            },
        }
        for t in tools
    ]


def _from_lc_response(ai_message) -> _Response:
    blocks: list[_Block] = []
    text = ai_message.content
    if isinstance(text, list):
        # Some providers return content as a list of parts.
        text = "".join(
            p.get("text", "") if isinstance(p, dict) else str(p) for p in text
        )
    if text:
        blocks.append(_Block(type="text", text=text))
    for call in getattr(ai_message, "tool_calls", None) or []:
        blocks.append(
            _Block(
                type="tool_use",
                id=call.get("id") or f"call_{uuid.uuid4().hex[:8]}",
                name=call["name"],
                input=call.get("args") or {},
            )
        )
    usage = getattr(ai_message, "usage_metadata", None) or {}
    return _Response(
        content=blocks,
        usage=_Usage(
            input_tokens=int(usage.get("input_tokens", 0)),
            output_tokens=int(usage.get("output_tokens", 0)),
        ),
    )


class _Messages:
    """Mimics anthropic client's `.messages` with a single `.create`."""

    def __init__(self, model):
        self._model = model
        # tool_call id -> the raw LangChain AIMessage that produced it, so a
        # later turn can be replayed verbatim (preserves provider-specific state).
        self._ai_cache: dict = {}

    async def create(self, *, model=None, max_tokens=None, system="", messages=None, tools=None):
        llm = self._model.bind_tools(_to_lc_tools(tools)) if tools else self._model
        lc_messages = _to_lc_messages(system, messages or [], cache=self._ai_cache)

        delay = RETRY_BASE_DELAY_S
        for attempt in range(RETRY_MAX + 1):
            try:
                ai_message = await llm.ainvoke(lc_messages)
                for call in getattr(ai_message, "tool_calls", None) or []:
                    if call.get("id"):
                        self._ai_cache[call["id"]] = ai_message
                return _from_lc_response(ai_message)
            except Exception as exc:
                if _is_rate_limit(exc) and attempt < RETRY_MAX:
                    log.warning(
                        "llm_rate_limited_retrying",
                        attempt=attempt + 1,
                        retry_in_s=delay,
                        error=str(exc)[:200],
                    )
                    await asyncio.sleep(delay)
                    delay *= 2
                    continue
                raise


class LLMClient:
    """Anthropic-shaped facade over a LangChain chat model."""

    def __init__(self, model):
        self.messages = _Messages(model)


def get_llm() -> LLMClient:
    """Construct the configured provider's client. Providers imported lazily."""
    settings = get_settings()
    provider = settings.llm_provider
    api_key = settings.llm_api_key or None

    if provider == "gemini":
        from langchain_google_genai import ChatGoogleGenerativeAI

        model = ChatGoogleGenerativeAI(
            model=settings.llm_model,
            google_api_key=api_key,
            max_output_tokens=DEFAULT_MAX_TOKENS,
            temperature=0,
        )
        return LLMClient(model)

    if provider == "anthropic":
        try:
            from langchain_anthropic import ChatAnthropic
        except ImportError as exc:  # pragma: no cover - alternate path
            raise RuntimeError(
                "LLM_PROVIDER=anthropic requires `pip install langchain-anthropic`"
            ) from exc

        model = ChatAnthropic(
            model=settings.llm_model, api_key=api_key, max_tokens=DEFAULT_MAX_TOKENS
        )
        return LLMClient(model)

    raise ValueError(f"unknown LLM_PROVIDER: {provider!r} (expected 'gemini' or 'anthropic')")
