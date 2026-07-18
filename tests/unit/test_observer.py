"""Unit tests for newsagg.api.observer (Phase 7).

Hermetic: in-memory sqlite (Base.metadata.create_all), newsagg.core.llm.complete
mocked directly on the observer module. No conftest.py dependency — this
file owns its own fixtures per the Phase 7 task split (bot agent owns
tests/unit/conftest.py).
"""
import datetime

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

import newsagg.api.observer as observer_module
from newsagg.api.observer import InterestExtraction, observe_conversation
from newsagg.db.schema import Base, User, Interest


@pytest.fixture
def session_factory(monkeypatch):
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    testing_session_local = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    monkeypatch.setattr(observer_module, "SessionLocal", testing_session_local)
    yield testing_session_local
    engine.dispose()


@pytest.fixture
def make_user(session_factory):
    def _make(chat_id="123", first_name="Ada"):
        db = session_factory()
        try:
            user = User(telegram_chat_id=chat_id, first_name=first_name)
            db.add(user)
            db.commit()
            db.refresh(user)
            return user.id
        finally:
            db.close()
    return _make


def _mock_complete(return_value=None, side_effect=None):
    async def _fake(**kwargs):
        if side_effect is not None:
            raise side_effect
        return return_value
    return _fake


@pytest.mark.asyncio
async def test_high_confidence_new_topic_creates_implicit_interest(session_factory, make_user, monkeypatch):
    chat_id = "123"
    make_user(chat_id=chat_id)
    monkeypatch.setattr(
        observer_module, "complete",
        _mock_complete(InterestExtraction(topic="cloud", confidence=0.95)),
    )

    await observe_conversation(chat_id, "tell me more about kubernetes and cloud infra")

    db = session_factory()
    try:
        user = db.query(User).filter(User.telegram_chat_id == chat_id).first()
        interests = db.query(Interest).filter(Interest.user_id == user.id).all()
        assert len(interests) == 1
        assert interests[0].topic == "cloud"
        assert interests[0].source == "implicit"
        assert interests[0].engagement_score == pytest.approx(0.95)
    finally:
        db.close()


@pytest.mark.asyncio
async def test_low_confidence_creates_nothing(session_factory, make_user, monkeypatch):
    chat_id = "124"
    make_user(chat_id=chat_id)
    monkeypatch.setattr(
        observer_module, "complete",
        _mock_complete(InterestExtraction(topic="cloud", confidence=0.5)),
    )

    await observe_conversation(chat_id, "meh, whatever")

    db = session_factory()
    try:
        user = db.query(User).filter(User.telegram_chat_id == chat_id).first()
        interests = db.query(Interest).filter(Interest.user_id == user.id).all()
        assert interests == []
    finally:
        db.close()


@pytest.mark.asyncio
async def test_existing_explicit_interest_refreshed_and_bumped(session_factory, make_user, monkeypatch):
    chat_id = "125"
    user_id = make_user(chat_id=chat_id)

    db = session_factory()
    try:
        old_time = datetime.datetime(2020, 1, 1)
        existing = Interest(
            user_id=user_id, topic="cloud", source="explicit",
            engagement_score=0.95, last_interacted_at=old_time,
        )
        db.add(existing)
        db.commit()
    finally:
        db.close()

    monkeypatch.setattr(
        observer_module, "complete",
        _mock_complete(InterestExtraction(topic="cloud", confidence=0.95)),
    )

    await observe_conversation(chat_id, "more cloud news please")

    db = session_factory()
    try:
        user = db.query(User).filter(User.telegram_chat_id == chat_id).first()
        interest = db.query(Interest).filter(Interest.user_id == user.id, Interest.topic == "cloud").first()
        assert interest.source == "explicit"  # source is not overwritten
        assert interest.engagement_score == pytest.approx(1.0)  # 0.95 + 0.1 capped at 1.0
        assert interest.last_interacted_at > old_time
    finally:
        db.close()


@pytest.mark.asyncio
async def test_invalid_topic_slug_rejected_nothing_written(session_factory, make_user, monkeypatch):
    chat_id = "126"
    make_user(chat_id=chat_id)

    async def _fake(**kwargs):
        # Simulate exactly what newsagg.core.llm.complete would do for an
        # out-of-taxonomy slug: model_validate raises pydantic.ValidationError.
        return InterestExtraction.model_validate({"topic": "not_a_real_topic", "confidence": 0.95})

    monkeypatch.setattr(observer_module, "complete", _fake)

    await observe_conversation(chat_id, "tell me about crypto")

    db = session_factory()
    try:
        user = db.query(User).filter(User.telegram_chat_id == chat_id).first()
        interests = db.query(Interest).filter(Interest.user_id == user.id).all()
        assert interests == []
    finally:
        db.close()
