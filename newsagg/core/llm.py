"""Single LLM gateway. Every LLM call in the codebase goes through complete()."""
import json, asyncio, logging
import httpx, openai
from pydantic import BaseModel
from newsagg import config

logger = logging.getLogger(__name__)

# max_retries=0 on both clients: retries and fallback are this module's job
# alone (ADR-3 — "one choke point for retries, fallback, metrics"). Without
# this, the openai SDK's own default (2 retries -> 3 HTTP calls per
# .create()) would silently triple the latency/cost of every attempt in the
# loop below before our own retry-then-fallback logic ever gets a turn.
_taut = openai.AsyncOpenAI(base_url=config.TAUT_URL, api_key="taut-local", max_retries=0)
_gemini = openai.AsyncOpenAI(
    base_url="https://generativelanguage.googleapis.com/v1beta/openai/",
    api_key=config.GEMINI_API_KEY,
    max_retries=0,
) if config.GEMINI_API_KEY else None

TIER_MODELS = {"simple": "ollama/llama3.1", "standard": "gemini/gemini-2.5-flash",
               "complex": "gemini/gemini-2.5-flash"}
_FALLBACK_MODEL = "gemini-2.5-flash"


async def complete(*, tier: str, system: str, user: str,
                   response_model: type[BaseModel] | None = None,
                   namespace: str = "system", context: str = "",
                   stream: bool = False, max_retries: int = 3):
    """Returns str, a parsed response_model instance, or an async chunk iterator (stream=True)."""
    messages = [{"role": "system", "content": system}, {"role": "user", "content": user}]
    headers = {"X-Taut-Namespace": namespace, "X-Taut-System": "NewsAggregator",
               "X-Taut-Context": context, "X-Taut-Tier": tier}
    kwargs = {}
    if response_model is not None:
        kwargs["response_format"] = {"type": "json_object"}
        messages[0]["content"] += ("\nRespond with JSON matching this schema exactly:\n"
                                   + json.dumps(response_model.model_json_schema()))
    last_err = None
    for attempt in range(max_retries):
        for client, model in ((_taut, TIER_MODELS[tier]), (_gemini, _FALLBACK_MODEL)):
            if client is None:
                continue
            try:
                resp = await client.chat.completions.create(
                    model=model, messages=messages, stream=stream,
                    extra_headers=headers if client is _taut else {}, **kwargs)
                if stream:
                    return resp
                text = resp.choices[0].message.content
                return response_model.model_validate_json(text) if response_model else text
            except Exception as e:           # noqa: BLE001 — gateway boundary
                last_err = e
                logger.warning("LLM call failed (%s, attempt %d): %s", model, attempt, e)
        await asyncio.sleep(2 ** attempt)
    raise RuntimeError(f"All LLM routes failed after {max_retries} attempts") from last_err
