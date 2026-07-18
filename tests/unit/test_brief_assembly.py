"""Unit tests for newsagg.processor.brief_engine.assemble_brief (Phase 6).

Hermetic: in-memory sqlite (Base.metadata.create_all) for the Brief-row
persistence side effect. assemble_brief makes ZERO LLM/Chroma/Telegram
calls, so those boundaries need no mocking here. No conftest.py
dependency; this file owns its own fixtures per the Phase 6 task split
(bot agent owns tests/unit/conftest.py).
"""
import datetime

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

import newsagg.processor.brief_engine as brief_engine
from newsagg.processor.brief_engine import (
    ModuleItem,
    TopicModuleContent,
    QUIET_DAY_MESSAGE,
    assemble_brief,
)
from newsagg.db.schema import Base, User, Brief


@pytest.fixture
def session_factory(monkeypatch):
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    testing_session_local = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    monkeypatch.setattr(brief_engine, "SessionLocal", testing_session_local)
    yield testing_session_local
    engine.dispose()


@pytest.fixture
def make_user(session_factory):
    def _make(chat_id="chat-1", first_name="Ada"):
        db = session_factory()
        try:
            user = User(telegram_chat_id=chat_id, first_name=first_name)
            db.add(user)
            db.commit()
            db.refresh(user)
            return user
        finally:
            db.close()
    return _make


def _today():
    return datetime.datetime.now(datetime.timezone.utc).date()


def test_two_module_brief_has_both_topic_headers_and_escapes_titles(session_factory, make_user):
    user = make_user()
    modules = {
        "ai": TopicModuleContent(
            topic="ai",
            headline="AI headline",
            items=[
                ModuleItem(
                    title="<script>alert(1)</script>",
                    url="https://example.com/a",
                    summary_line="Why it matters.",
                )
            ],
        ),
        "security": TopicModuleContent(
            topic="security",
            headline="Security headline",
            items=[
                ModuleItem(
                    title="Patch released",
                    url="https://example.com/b",
                    summary_line="Fixes a bug.",
                )
            ],
        ),
    }

    html_text = assemble_brief(user, modules)

    # <b> topic headers for both topics (taxonomy labels).
    assert "<b>AI &amp; ML</b>" in html_text
    assert "<b>Security</b>" in html_text
    # Title is HTML-escaped, not passed through raw.
    assert "&lt;script&gt;alert(1)&lt;/script&gt;" in html_text
    assert "<script>" not in html_text
    # Both link targets present.
    assert 'href="https://example.com/a"' in html_text
    assert 'href="https://example.com/b"' in html_text

    db = session_factory()
    try:
        brief = db.query(Brief).filter(Brief.user_id == user.id, Brief.brief_date == _today()).first()
        assert brief is not None
        assert brief.content["html"] == html_text
    finally:
        db.close()


def test_quiet_day_all_modules_none_produces_quiet_message_and_brief_row(session_factory, make_user):
    user = make_user(chat_id="chat-2")
    modules = {"ai": None, "security": None}

    html_text = assemble_brief(user, modules)

    assert html_text == QUIET_DAY_MESSAGE

    db = session_factory()
    try:
        brief = db.query(Brief).filter(Brief.user_id == user.id, Brief.brief_date == _today()).first()
        assert brief is not None
        assert brief.content["html"] == QUIET_DAY_MESSAGE
    finally:
        db.close()


def test_assemble_brief_does_not_duplicate_brief_row_same_day(session_factory, make_user):
    user = make_user(chat_id="chat-3")
    modules = {"ai": None}

    assemble_brief(user, modules)
    assemble_brief(user, modules)  # simulate a second assembly call the same day

    db = session_factory()
    try:
        briefs = db.query(Brief).filter(Brief.user_id == user.id, Brief.brief_date == _today()).all()
        assert len(briefs) == 1
    finally:
        db.close()


def test_empty_modules_dict_is_quiet_day(session_factory, make_user):
    user = make_user(chat_id="chat-4")

    html_text = assemble_brief(user, {})

    assert html_text == QUIET_DAY_MESSAGE
