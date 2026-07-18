"""Unit tests for newsagg.storage.vector_store.topic_filter (Phase 5, ADR-4).

Hermetic: topic_filter is pure — no Chroma/network/LLM involved. Importing
newsagg.storage.vector_store must not touch any infra (lazy client fix).
"""
from newsagg.storage.vector_store import topic_filter


def test_multiple_slugs_use_or_clause():
    assert topic_filter(["ai", "databases"]) == {
        "$and": [
            {"type": {"$eq": "parent"}},
            {"$or": [
                {"topic_ai": {"$eq": True}},
                {"topic_databases": {"$eq": True}},
            ]},
        ]
    }


def test_single_slug_has_no_or_clause():
    assert topic_filter(["ai"]) == {
        "$and": [
            {"type": {"$eq": "parent"}},
            {"topic_ai": {"$eq": True}},
        ]
    }


def test_top_pseudo_topic_excluded_from_or_clause():
    assert topic_filter(["top"]) == {"type": {"$eq": "parent"}}


def test_empty_slugs_returns_bare_parent_filter():
    assert topic_filter([]) == {"type": {"$eq": "parent"}}


def test_unknown_slugs_are_dropped():
    assert topic_filter(["not-a-real-slug"]) == {"type": {"$eq": "parent"}}
    assert topic_filter(["ai", "not-a-real-slug"]) == {
        "$and": [
            {"type": {"$eq": "parent"}},
            {"topic_ai": {"$eq": True}},
        ]
    }


def test_top_mixed_with_real_slug_excludes_top_from_or():
    assert topic_filter(["top", "security"]) == {
        "$and": [
            {"type": {"$eq": "parent"}},
            {"topic_security": {"$eq": True}},
        ]
    }
