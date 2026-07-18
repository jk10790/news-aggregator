"""Unit tests for newsagg.ingestion.triage (Phase 4).

Hermetic: no live Kafka/LLM. The Kafka consumer/producer and
newsagg.core.llm.complete are mocked at the boundary.
"""
import json
from unittest.mock import AsyncMock

import pytest
from pydantic import ValidationError

from newsagg.core.models import ArticleRaw
from newsagg.ingestion import triage
from newsagg.ingestion.triage import TriageOutput, _run_once


class FakeRecord:
    def __init__(self, value: bytes):
        self.value = value


def _raw_article_bytes(title="Some article", source="tech_meme") -> bytes:
    article = ArticleRaw(
        source=source,
        title=title,
        link="https://example.com/article",
        summary="A summary of the article.",
        published="Wed, 17 Jun 2026 15:09:20 +0000",
    )
    return article.model_dump_json().encode("utf-8")


# =========================================================================
# TriageOutput validation / topic mapping
# =========================================================================

def test_topic_label_mapped_to_slug():
    out = TriageOutput(
        relevant=True,
        reasoning="db internals",
        topics=["Databases"],
        entities=[],
        importance_score=5,
        key_insights=[],
    )
    assert out.topics == ["databases"]


def test_unknown_topic_label_is_dropped():
    out = TriageOutput(
        relevant=True,
        reasoning="nonsense",
        topics=["Quantum Basketweaving"],
        entities=[],
        importance_score=5,
        key_insights=[],
    )
    assert out.topics == []


def test_mixed_known_and_unknown_topics():
    out = TriageOutput(
        relevant=True,
        reasoning="mixed",
        topics=["Databases", "Quantum Basketweaving", "Distributed Systems", "AI"],
        entities=[],
        importance_score=5,
        key_insights=[],
    )
    assert out.topics == ["databases", "distsys", "ai"]


def test_ai_and_ml_label_maps_to_ai_slug():
    out = TriageOutput(
        relevant=True, reasoning="", topics=["AI & ML"], entities=[],
        importance_score=5, key_insights=[],
    )
    assert out.topics == ["ai"]


@pytest.mark.parametrize("bad_score", [0, 11])
def test_importance_score_out_of_range_rejected(bad_score):
    with pytest.raises(ValidationError):
        TriageOutput(
            relevant=True,
            reasoning="x",
            topics=[],
            entities=[],
            importance_score=bad_score,
            key_insights=[],
        )


@pytest.mark.parametrize("good_score", [1, 5, 10])
def test_importance_score_in_range_accepted(good_score):
    out = TriageOutput(
        relevant=True, reasoning="x", topics=[], entities=[],
        importance_score=good_score, key_insights=[],
    )
    assert out.importance_score == good_score


# =========================================================================
# DLQ path
# =========================================================================

@pytest.mark.asyncio
async def test_triage_failure_routes_to_dlq_and_commits(monkeypatch):
    monkeypatch.setattr(
        triage, "complete", AsyncMock(side_effect=RuntimeError("all LLM routes failed"))
    )

    raw_value = _raw_article_bytes(title="Breaking: something happened")

    fake_consumer = AsyncMock()
    fake_consumer.getmany = AsyncMock(
        return_value={("raw-articles", 0): [FakeRecord(raw_value)]}
    )
    fake_consumer.commit = AsyncMock()

    fake_producer = AsyncMock()
    fake_producer.send_and_wait = AsyncMock()

    processed = await _run_once(fake_consumer, fake_producer)

    assert processed == 1
    fake_consumer.commit.assert_awaited_once()

    fake_producer.send_and_wait.assert_awaited_once()
    call_args = fake_producer.send_and_wait.call_args
    topic_arg = call_args.args[0] if call_args.args else call_args.kwargs.get("topic")
    assert topic_arg == triage.TRIAGE_DLQ_TOPIC

    published_value = call_args.kwargs.get("value") or call_args.args[1]
    payload = json.loads(published_value.decode("utf-8"))
    assert payload["stage"] == "triage"
    assert "error" in payload
    assert payload["title"] == "Breaking: something happened"


@pytest.mark.asyncio
async def test_successful_triage_publishes_verified_article_and_commits(monkeypatch):
    canned = TriageOutput(
        relevant=True,
        reasoning="high quality AI news",
        topics=["AI"],
        entities=["OpenAI"],
        importance_score=8,
        key_insights=["Something important happened."],
    )
    monkeypatch.setattr(triage, "complete", AsyncMock(return_value=canned))

    raw_value = _raw_article_bytes(title="OpenAI ships something")

    fake_consumer = AsyncMock()
    fake_consumer.getmany = AsyncMock(
        return_value={("raw-articles", 0): [FakeRecord(raw_value)]}
    )
    fake_consumer.commit = AsyncMock()

    fake_producer = AsyncMock()
    fake_producer.send_and_wait = AsyncMock()

    processed = await _run_once(fake_consumer, fake_producer)

    assert processed == 1
    fake_consumer.commit.assert_awaited_once()
    fake_producer.send_and_wait.assert_awaited_once()

    call_args = fake_producer.send_and_wait.call_args
    topic_arg = call_args.args[0] if call_args.args else call_args.kwargs.get("topic")
    assert topic_arg == triage.TOPIC_VERIFIED_ARTICLES

    published_value = call_args.kwargs.get("value") or call_args.args[1]
    payload = json.loads(published_value.decode("utf-8"))
    assert payload["topics"] == ["ai"]
    assert payload["importance_score"] == 8
    assert payload["key_insights"] == ["Something important happened."]
    assert payload["triage_reason"] == "high quality AI news"


@pytest.mark.asyncio
async def test_irrelevant_article_neither_published_nor_dlqd(monkeypatch):
    canned = TriageOutput(
        relevant=False, reasoning="spam", topics=[], entities=[],
        importance_score=1, key_insights=[],
    )
    monkeypatch.setattr(triage, "complete", AsyncMock(return_value=canned))

    raw_value = _raw_article_bytes()

    fake_consumer = AsyncMock()
    fake_consumer.getmany = AsyncMock(
        return_value={("raw-articles", 0): [FakeRecord(raw_value)]}
    )
    fake_consumer.commit = AsyncMock()

    fake_producer = AsyncMock()
    fake_producer.send_and_wait = AsyncMock()

    await _run_once(fake_consumer, fake_producer)

    fake_consumer.commit.assert_awaited_once()
    fake_producer.send_and_wait.assert_not_awaited()


@pytest.mark.asyncio
async def test_empty_batch_does_not_commit():
    fake_consumer = AsyncMock()
    fake_consumer.getmany = AsyncMock(return_value={})
    fake_consumer.commit = AsyncMock()
    fake_producer = AsyncMock()

    processed = await _run_once(fake_consumer, fake_producer)

    assert processed == 0
    fake_consumer.commit.assert_not_awaited()
