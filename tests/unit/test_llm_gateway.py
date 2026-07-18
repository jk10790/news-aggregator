"""Unit tests for newsagg.core.llm.complete (ADR-3, Phase 9).

Hermetic: respx intercepts httpx at the transport layer, so the module-level
_taut/_gemini AsyncOpenAI clients built at import time (newsagg/core/llm.py)
never make a real network call — respx patches the transport regardless of
when the client objects were constructed. asyncio.sleep is monkeypatched to
a no-op so the exponential backoff between retry attempts doesn't slow the
suite down.
"""
import json

import httpx
import pytest
import respx
from pydantic import BaseModel

from newsagg.core import llm as llm_module

TAUT_CHAT_URL = "http://localhost:8000/v1/chat/completions"
GEMINI_CHAT_URL = "https://generativelanguage.googleapis.com/v1beta/openai/chat/completions"


class _Schema(BaseModel):
    foo: str


def _openai_response(content: str) -> dict:
    return {
        "id": "chatcmpl-test",
        "object": "chat.completion",
        "created": 0,
        "model": "test-model",
        "choices": [
            {"index": 0, "message": {"role": "assistant", "content": content}, "finish_reason": "stop"}
        ],
    }


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    """The gateway sleeps 2**attempt seconds between retry rounds — skip it."""

    async def _instant(_seconds):
        return None

    monkeypatch.setattr(llm_module.asyncio, "sleep", _instant)


@pytest.mark.asyncio
@respx.mock
async def test_taut_500_falls_back_to_gemini_route():
    respx.post(TAUT_CHAT_URL).mock(return_value=httpx.Response(500, json={"error": "taut down"}))
    gemini_route = respx.post(GEMINI_CHAT_URL).mock(
        return_value=httpx.Response(200, json=_openai_response("hello from gemini"))
    )

    result = await llm_module.complete(tier="simple", system="sys", user="hi")

    assert result == "hello from gemini"
    assert gemini_route.call_count == 1


@pytest.mark.asyncio
@respx.mock
async def test_response_model_schema_injected_into_system_message():
    route = respx.post(TAUT_CHAT_URL).mock(
        return_value=httpx.Response(200, json=_openai_response(json.dumps({"foo": "bar"})))
    )

    result = await llm_module.complete(
        tier="simple", system="sys prompt", user="hi", response_model=_Schema
    )

    assert result == _Schema(foo="bar")

    sent_body = json.loads(route.calls.last.request.content)
    system_message = sent_body["messages"][0]["content"]
    assert system_message.startswith("sys prompt")
    assert "Respond with JSON matching this schema exactly" in system_message
    # The response_model's JSON schema (its field names) is inlined verbatim.
    assert '"foo"' in system_message


@pytest.mark.asyncio
@respx.mock
async def test_all_routes_failing_retries_then_raises():
    taut_route = respx.post(TAUT_CHAT_URL).mock(return_value=httpx.Response(500))
    gemini_route = respx.post(GEMINI_CHAT_URL).mock(return_value=httpx.Response(500))

    with pytest.raises(RuntimeError, match="All LLM routes failed after 2 attempts"):
        await llm_module.complete(tier="simple", system="sys", user="hi", max_retries=2)

    assert taut_route.call_count == 2
    assert gemini_route.call_count == 2
