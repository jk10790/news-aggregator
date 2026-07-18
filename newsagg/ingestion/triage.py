"""Triage consumer (ADR-11): reads raw articles, classifies them via the shared
LLM gateway (newsagg.core.llm.complete), and publishes verified articles.

Offsets are committed manually only after every message in a batch has
resolved — either published downstream or routed to the triage-dlq topic.
This is the ONLY module that owns the topic-slug mapping for triage output;
storage/vector_store.py trusts `ArticleVerified.topics` are already slugs.
"""
import asyncio
import json
import logging
import os

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from pydantic import BaseModel, Field, field_validator

from newsagg.config import REDPANDA_BROKER, TOPIC_RAW_ARTICLES, TOPIC_VERIFIED_ARTICLES
from newsagg.core.models import ArticleRaw, ArticleVerified
from newsagg.core.llm import complete
from newsagg.core.taxonomy import SLUGS

logger = logging.getLogger(__name__)

TRIAGE_DLQ_TOPIC = "triage-dlq"
GETMANY_MAX_RECORDS = 50
CONCURRENCY = 10

# =========================================================================
# Topic label -> taxonomy slug mapping
# =========================================================================
# The LLM sometimes returns a human label ("Databases", "Distributed Systems",
# "AI & ML") instead of the exact slug. Map known aliases down to slugs;
# anything that doesn't resolve is dropped rather than guessed at.
_TOPIC_ALIASES: dict[str, str] = {
    # identity (already a slug, case-insensitive)
    **{slug: slug for slug in SLUGS},
    # AI
    "ai & ml": "ai", "ai and ml": "ai", "artificial intelligence": "ai", "machine learning": "ai",
    # Cloud
    "cloud & infra": "cloud", "cloud and infra": "cloud", "cloud infrastructure": "cloud",
    # Security
    "cybersecurity": "security", "infosec": "security",
    # Startups
    "startups & vc": "startups", "startups and vc": "startups", "venture capital": "startups",
    # Programming
    "software engineering": "programming", "developer tools": "programming",
    # Distributed systems
    "distributed systems": "distsys",
    # Databases
    "database": "databases",
    # Business
    "business & markets": "business", "business and markets": "business", "markets": "business",
}


def _map_topic_to_slug(raw: str) -> str | None:
    return _TOPIC_ALIASES.get(str(raw).strip().lower())


class TriageOutput(BaseModel):
    """Structured triage decision returned by the LLM via core.llm.complete."""

    relevant: bool
    reasoning: str
    topics: list[str] = Field(default_factory=list)
    entities: list[str] = Field(default_factory=list)
    importance_score: int = Field(ge=1, le=10)
    key_insights: list[str] = Field(default_factory=list)

    @field_validator("topics", mode="before")
    @classmethod
    def _normalize_topics(cls, value):
        if not value:
            return []
        mapped: list[str] = []
        for raw in value:
            slug = _map_topic_to_slug(raw)
            if slug and slug not in mapped:
                mapped.append(slug)
        return mapped


# =========================================================================
# Prompts
# =========================================================================
SYSTEM_PROMPT_PATH = os.path.join(os.path.dirname(__file__), "prompts", "triage_system_prompt.txt")
USER_PROMPT_PATH = os.path.join(os.path.dirname(__file__), "prompts", "triage_user_prompt.txt")

with open(SYSTEM_PROMPT_PATH, "r") as f:
    TRIAGE_SYSTEM_PROMPT = f.read()
with open(USER_PROMPT_PATH, "r") as f:
    TRIAGE_USER_PROMPT = f.read()


async def triage_article(article: ArticleRaw) -> TriageOutput:
    """Classifies a single article via the shared LLM gateway (ADR-3).

    core.llm.complete already implements the Taut -> Gemini fallback and
    retries internally; this function makes exactly one call and lets
    failures propagate to the caller, which routes them to the DLQ.
    """
    user_prompt = TRIAGE_USER_PROMPT.format(
        title=article.title,
        source=article.source,
        summary=article.summary,
    )
    return await complete(
        tier="simple",
        system=TRIAGE_SYSTEM_PROMPT,
        user=user_prompt,
        response_model=TriageOutput,
        context="Triage",
        namespace="system",
    )


async def _publish_dlq(producer: AIOKafkaProducer, raw_value: bytes, error: Exception) -> None:
    try:
        payload = json.loads(raw_value.decode("utf-8"))
        if not isinstance(payload, dict):
            payload = {"raw": payload}
    except Exception:
        payload = {"raw": raw_value.decode("utf-8", errors="replace")}
    payload["error"] = str(error)
    payload["stage"] = "triage"
    await producer.send_and_wait(TRIAGE_DLQ_TOPIC, value=json.dumps(payload).encode("utf-8"))


async def _process_one(raw_value: bytes, producer: AIOKafkaProducer, sem: asyncio.Semaphore) -> None:
    async with sem:
        try:
            article = ArticleRaw.model_validate_json(raw_value.decode("utf-8"))
        except Exception as e:
            logger.error("Failed to parse raw article: %s", e)
            await _publish_dlq(producer, raw_value, e)
            return

        try:
            result = await triage_article(article)
        except Exception as e:
            logger.error("Triage failed for '%s': %s", article.title, e)
            await _publish_dlq(producer, raw_value, e)
            return

        if not result.relevant:
            logger.info("REJECTED: '%s' — %s", article.title, result.reasoning)
            return

        logger.info("ACCEPTED: '%s' — %s", article.title, result.reasoning)
        verified = ArticleVerified(
            **article.model_dump(),
            triage_reason=result.reasoning,
            topics=result.topics,
            entities=result.entities,
            importance_score=result.importance_score,
            key_insights=result.key_insights,
        )
        try:
            await producer.send_and_wait(
                TOPIC_VERIFIED_ARTICLES, value=verified.model_dump_json().encode("utf-8")
            )
        except Exception as e:
            logger.error("Failed to publish verified article '%s': %s", article.title, e)
            await _publish_dlq(producer, raw_value, e)


async def process_batch(raw_values: list[bytes], producer: AIOKafkaProducer) -> None:
    """Processes every message in a batch concurrently (bounded by a semaphore).

    Returns only after ALL messages have resolved — either published
    downstream or routed to the DLQ — so the caller can safely commit offsets.
    """
    if not raw_values:
        return
    sem = asyncio.Semaphore(CONCURRENCY)
    await asyncio.gather(*(_process_one(raw, producer, sem) for raw in raw_values))


async def _run_once(consumer: AIOKafkaConsumer, producer: AIOKafkaProducer) -> int:
    """Fetches one batch, processes it fully, and commits offsets. Returns
    the number of messages processed (0 if the poll timed out empty)."""
    batches = await consumer.getmany(timeout_ms=5000, max_records=GETMANY_MAX_RECORDS)
    if not batches:
        return 0
    raw_values = [msg.value for messages in batches.values() for msg in messages]
    await process_batch(raw_values, producer)
    await consumer.commit()
    return len(raw_values)


async def main():
    logger.info("Starting Triage Consumer...")

    consumer = AIOKafkaConsumer(
        TOPIC_RAW_ARTICLES,
        bootstrap_servers=REDPANDA_BROKER,
        group_id="triage-group",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    producer = AIOKafkaProducer(bootstrap_servers=REDPANDA_BROKER)

    await consumer.start()
    await producer.start()
    logger.info("Connected to Redpanda. Consuming from '%s'...", TOPIC_RAW_ARTICLES)

    try:
        while True:
            await _run_once(consumer, producer)
    finally:
        await consumer.stop()
        await producer.stop()
        logger.info("Triage consumer stopped.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    asyncio.run(main())
