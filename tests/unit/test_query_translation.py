"""Unit tests for newsagg.api.query_engine (Phase 7): query translation,
retrieval where-clause construction, and context grading.

Hermetic: newsagg.core.llm.complete is mocked directly on the query_engine
module. Chroma/embeddings are mocked/monkeypatched so no docker/network is
required.
"""
import datetime

import pytest

import newsagg.api.query_engine as qe
from newsagg.api.query_engine import (
    TranslatedQuery,
    ContextGrade,
    translate_query,
    execute_hybrid_search,
    evaluator_node,
)


def _mock_complete(return_value):
    async def _fake(**kwargs):
        return return_value
    return _fake


class FakeCollection:
    """Records the `where` clause passed to .query() and returns no hits."""

    def __init__(self):
        self.last_where = None

    def query(self, query_embeddings, n_results, where):
        self.last_where = where
        return {"metadatas": [[]]}

    def get(self, ids):
        return {"documents": [], "metadatas": [{}]}


# =========================================================================
# translate_query
# =========================================================================
@pytest.mark.asyncio
async def test_translate_query_null_offsets_no_date_filter(monkeypatch):
    monkeypatch.setattr(
        qe, "complete",
        _mock_complete(TranslatedQuery(semantic_query="kubernetes", days_offset_start=None, days_offset_end=None)),
    )
    translated = await translate_query("tell me about kubernetes", chat_id="42")
    assert translated.semantic_query == "kubernetes"
    assert translated.days_offset_start is None
    assert translated.days_offset_end is None

    # Retrieval: no published_int constraint should be present.
    fake_collection = FakeCollection()
    monkeypatch.setattr(qe, "get_chroma_collection", lambda: fake_collection)
    monkeypatch.setattr(qe, "embed", lambda texts: [[0.1, 0.2, 0.3]])

    await execute_hybrid_search(translated)

    where = fake_collection.last_where
    # Only the type=child clause should be present — no $and, no published_int.
    assert where == {"type": {"$eq": "child"}}


@pytest.mark.asyncio
async def test_translate_query_with_offsets_builds_correct_date_range(monkeypatch):
    monkeypatch.setattr(
        qe, "complete",
        _mock_complete(TranslatedQuery(semantic_query="AI", days_offset_start=7, days_offset_end=0)),
    )
    translated = await translate_query("AI news from the last week", chat_id="42")
    assert translated.days_offset_start == 7
    assert translated.days_offset_end == 0

    fake_collection = FakeCollection()
    monkeypatch.setattr(qe, "get_chroma_collection", lambda: fake_collection)
    monkeypatch.setattr(qe, "embed", lambda texts: [[0.1, 0.2, 0.3]])

    await execute_hybrid_search(translated)

    where = fake_collection.last_where
    assert "$and" in where
    clauses = where["$and"]
    assert {"type": {"$eq": "child"}} in clauses

    today = datetime.date.today()
    expected_start = int((today - datetime.timedelta(days=7)).strftime("%Y%m%d"))
    expected_end = int((today - datetime.timedelta(days=0)).strftime("%Y%m%d"))

    assert {"published_int": {"$gte": expected_start}} in clauses
    assert {"published_int": {"$lte": expected_end}} in clauses


@pytest.mark.asyncio
async def test_translate_query_falls_back_on_llm_error(monkeypatch):
    async def _raise(**kwargs):
        raise RuntimeError("llm gateway down")

    monkeypatch.setattr(qe, "complete", _raise)

    translated = await translate_query("what's new with SpaceX", chat_id="42")
    assert translated.semantic_query == "what's new with SpaceX"
    assert translated.days_offset_start is None
    assert translated.days_offset_end is None


# =========================================================================
# evaluator_node / ContextGrade
# =========================================================================
@pytest.mark.asyncio
async def test_evaluator_sufficient_at_0_8(monkeypatch):
    monkeypatch.setattr(
        qe, "complete",
        _mock_complete(ContextGrade(score=0.8, reasoning="Context directly answers the query.")),
    )
    state = {
        "query": "what happened with SpaceX",
        "chat_id": "42",
        "context_articles": [{"title": "t", "source": "s", "url": "u", "content": "c"}],
        "context_text": "Document 1 | Source: s | URL: u\nContent: c",
    }
    result = await evaluator_node(state)
    assert result["grade"] == "sufficient"


@pytest.mark.asyncio
async def test_evaluator_insufficient_at_0_3(monkeypatch):
    monkeypatch.setattr(
        qe, "complete",
        _mock_complete(ContextGrade(score=0.3, reasoning="Context is unrelated to the query.")),
    )
    state = {
        "query": "what happened with SpaceX",
        "chat_id": "42",
        "context_articles": [{"title": "t", "source": "s", "url": "u", "content": "c"}],
        "context_text": "Document 1 | Source: s | URL: u\nContent: c",
    }
    result = await evaluator_node(state)
    assert result["grade"] == "insufficient"


@pytest.mark.asyncio
async def test_evaluator_insufficient_when_no_context(monkeypatch):
    called = False

    async def _fake(**kwargs):
        nonlocal called
        called = True
        return ContextGrade(score=0.9, reasoning="n/a")

    monkeypatch.setattr(qe, "complete", _fake)
    state = {
        "query": "what happened with SpaceX",
        "chat_id": "42",
        "context_articles": [],
        "context_text": "",
    }
    result = await evaluator_node(state)
    assert result["grade"] == "insufficient"
    assert called is False  # short-circuits before calling the LLM
