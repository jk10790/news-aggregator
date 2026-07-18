"""Unit tests for newsagg.processor.brief_engine.run_hour (Phase 6 scheduler
due-matrix).

Hermetic: in-memory sqlite (Base.metadata.create_all) for Users/Interests/
TopicModules/Briefs; newsagg.core.llm.complete, the Chroma boundary
(fetch_topic_articles), and newsagg.bot.telegram_api.TelegramAPI are all
mocked directly on the modules brief_engine imports them from/into. No
conftest.py dependency; this file owns its own fixtures per the Phase 6
task split (bot agent owns tests/unit/conftest.py).
"""
import datetime

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

import newsagg.bot.telegram_api as telegram_api_module
import newsagg.processor.brief_engine as brief_engine
from newsagg.processor.brief_engine import ModuleItem, TopicModuleContent, run_hour
from newsagg.db.schema import Base, User, Interest, Brief

UTC = datetime.timezone.utc
MONDAY = datetime.datetime(2024, 1, 1, 7, 0, tzinfo=UTC)   # 2024-01-01 is a Monday
TUESDAY = datetime.datetime(2024, 1, 2, 7, 0, tzinfo=UTC)  # 2024-01-02 is a Tuesday


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
def fake_telegram(monkeypatch):
    """Patches newsagg.bot.telegram_api.TelegramAPI (which brief_engine.deliver
    imports lazily by name each call) with an in-memory recorder.
    """
    sent = []

    class _FakeTelegramAPI:
        def __init__(self, token):
            self.token = token

        async def send_message(self, chat_id, text, reply_markup=None):
            sent.append({"chat_id": chat_id, "text": text})
            return {"ok": True}

    monkeypatch.setattr(telegram_api_module, "TelegramAPI", _FakeTelegramAPI)
    return sent


@pytest.fixture
def llm_calls(monkeypatch):
    """Patches brief_engine.complete (the core.llm.complete gateway) so
    build_topic_module never makes a real network call. Records every call.
    """
    calls = []

    async def _fake_complete(**kwargs):
        calls.append(kwargs)
        return TopicModuleContent(
            topic="ai",
            headline="AI headline",
            items=[ModuleItem(title="Some title", url="https://example.com/x", summary_line="Summary.")],
        )

    monkeypatch.setattr(brief_engine, "complete", _fake_complete)
    return calls


@pytest.fixture
def fake_articles(monkeypatch):
    """Patches brief_engine.fetch_topic_articles (the Chroma boundary) so
    build_topic_module sees non-empty candidates without touching Chroma.
    """
    def _fake(slug, now):
        return [{"title": "T", "url": "https://example.com/x", "summary": "S", "importance_score": 9}]

    monkeypatch.setattr(brief_engine, "fetch_topic_articles", _fake)
    return _fake


def _make_user(Session, chat_id, cadence="daily", hour=7):
    db = Session()
    try:
        user = User(telegram_chat_id=chat_id, first_name="U", delivery_cadence=cadence, delivery_hour_utc=hour)
        db.add(user)
        db.commit()
        db.refresh(user)
        return user.id
    finally:
        db.close()


def _add_interest(Session, user_id, topic, source="explicit"):
    db = Session()
    try:
        db.add(Interest(user_id=user_id, topic=topic, source=source,
                         engagement_score=1.0, last_interacted_at=datetime.datetime.now(UTC)))
        db.commit()
    finally:
        db.close()


def _brief_row(Session, user_id, brief_date):
    db = Session()
    try:
        return db.query(Brief).filter(Brief.user_id == user_id, Brief.brief_date == brief_date).first()
    finally:
        db.close()


@pytest.mark.asyncio
async def test_daily_user_at_matching_hour_is_delivered(session_factory, fake_telegram):
    user_id = _make_user(session_factory, "chat-daily", cadence="daily", hour=7)

    await run_hour(MONDAY)

    assert len(fake_telegram) == 1
    assert fake_telegram[0]["chat_id"] == "chat-daily"
    brief = _brief_row(session_factory, user_id, MONDAY.date())
    assert brief is not None
    assert brief.delivered_at is not None


@pytest.mark.asyncio
async def test_daily_user_at_non_matching_hour_is_not_delivered(session_factory, fake_telegram):
    user_id = _make_user(session_factory, "chat-daily-off", cadence="daily", hour=7)

    off_hour_now = MONDAY.replace(hour=8)
    await run_hour(off_hour_now)

    assert fake_telegram == []
    assert _brief_row(session_factory, user_id, off_hour_now.date()) is None


@pytest.mark.asyncio
async def test_paused_user_is_not_delivered(session_factory, fake_telegram):
    user_id = _make_user(session_factory, "chat-paused", cadence="paused", hour=7)

    await run_hour(MONDAY)

    assert fake_telegram == []
    assert _brief_row(session_factory, user_id, MONDAY.date()) is None


@pytest.mark.asyncio
async def test_weekly_user_only_delivered_on_monday_matching_hour(session_factory, fake_telegram):
    user_id = _make_user(session_factory, "chat-weekly", cadence="weekly", hour=7)

    # Tuesday: not due even though the hour matches.
    await run_hour(TUESDAY)
    assert fake_telegram == []
    assert _brief_row(session_factory, user_id, TUESDAY.date()) is None

    # Monday: due.
    await run_hour(MONDAY)
    assert len(fake_telegram) == 1
    assert fake_telegram[0]["chat_id"] == "chat-weekly"
    assert _brief_row(session_factory, user_id, MONDAY.date()) is not None


@pytest.mark.asyncio
async def test_already_briefed_today_user_is_skipped(session_factory, fake_telegram):
    user_id = _make_user(session_factory, "chat-already", cadence="daily", hour=7)
    db = session_factory()
    try:
        db.add(Brief(user_id=user_id, brief_date=MONDAY.date(), content={"html": "prior brief"}))
        db.commit()
    finally:
        db.close()

    await run_hour(MONDAY)

    assert fake_telegram == []
    briefs = [b for b in [_brief_row(session_factory, user_id, MONDAY.date())] if b is not None]
    assert len(briefs) == 1  # still just the pre-existing row, not duplicated


@pytest.mark.asyncio
async def test_two_users_sharing_topic_builds_module_exactly_once(
    session_factory, fake_telegram, llm_calls, fake_articles,
):
    user_a = _make_user(session_factory, "chat-a", cadence="daily", hour=7)
    user_b = _make_user(session_factory, "chat-b", cadence="daily", hour=7)
    _add_interest(session_factory, user_a, "ai")
    _add_interest(session_factory, user_b, "ai")

    await run_hour(MONDAY)

    assert len(llm_calls) == 1
    assert len(fake_telegram) == 2
    assert _brief_row(session_factory, user_a, MONDAY.date()) is not None
    assert _brief_row(session_factory, user_b, MONDAY.date()) is not None


@pytest.mark.asyncio
async def test_second_run_hour_same_hour_sends_nothing(
    session_factory, fake_telegram, llm_calls, fake_articles,
):
    user_id = _make_user(session_factory, "chat-repeat", cadence="daily", hour=7)
    _add_interest(session_factory, user_id, "ai")

    await run_hour(MONDAY)
    assert len(fake_telegram) == 1
    assert len(llm_calls) == 1

    await run_hour(MONDAY)  # same hour, same day -> nothing new

    assert len(fake_telegram) == 1
    assert len(llm_calls) == 1
