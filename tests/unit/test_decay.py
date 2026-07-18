"""Unit tests for interest decay math (ADR-13) in newsagg.processor.brief_engine.

active_interests() is a pure DB-read + math function — no LLM, no Chroma,
no Telegram involved — but we still build the User/Interest rows through
an in-memory sqlite session (Base.metadata.create_all) since active_interests
expects a real `User` ORM object with a working `.interests` relationship.
No conftest.py dependency; this file owns its own fixtures per the Phase 6
task split (bot agent owns tests/unit/conftest.py).
"""
import datetime

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from newsagg.db.schema import Base, User, Interest
from newsagg.processor.brief_engine import active_interests, _decayed_implicit_score

UTC = datetime.timezone.utc


@pytest.fixture
def session_factory():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    testing_session_local = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    yield testing_session_local
    engine.dispose()


def _make_user_with_interests(session_factory, interest_specs):
    """Returns (db, user) with the session left OPEN so the caller can
    still exercise the lazy-loaded `user.interests` relationship.
    """
    db = session_factory()
    user = User(telegram_chat_id="chat-1", first_name="Ada")
    db.add(user)
    db.flush()
    for spec in interest_specs:
        db.add(Interest(user_id=user.id, **spec))
    db.commit()
    db.refresh(user)
    return db, user


def test_explicit_interest_100_days_stale_still_included(session_factory):
    db, user = _make_user_with_interests(session_factory, [
        dict(topic="ai", source="explicit", engagement_score=1.0,
             last_interacted_at=datetime.datetime(2020, 1, 1, tzinfo=UTC)),
    ])
    try:
        now = datetime.datetime(2020, 1, 1, tzinfo=UTC) + datetime.timedelta(days=100)
        assert active_interests(user, now) == ["ai"]
    finally:
        db.close()


def test_decayed_implicit_score_halves_at_one_half_life():
    # Formula check: at exactly 14 days (one half-life), score is halved.
    assert _decayed_implicit_score(1.0, 14) == pytest.approx(0.5)
    assert _decayed_implicit_score(0.8, 14) == pytest.approx(0.4)


def test_implicit_interest_included_at_28_days_score_one(session_factory):
    db, user = _make_user_with_interests(session_factory, [
        dict(topic="cloud", source="implicit", engagement_score=1.0,
             last_interacted_at=datetime.datetime(2020, 1, 1, tzinfo=UTC)),
    ])
    try:
        now = datetime.datetime(2020, 1, 1, tzinfo=UTC) + datetime.timedelta(days=28)
        # 1.0 * 0.5 ** (28/14) == 0.25 >= 0.2 -> still included
        assert active_interests(user, now) == ["cloud"]
    finally:
        db.close()


def test_implicit_interest_excluded_at_50_days_score_one(session_factory):
    db, user = _make_user_with_interests(session_factory, [
        dict(topic="cloud", source="implicit", engagement_score=1.0,
             last_interacted_at=datetime.datetime(2020, 1, 1, tzinfo=UTC)),
    ])
    try:
        now = datetime.datetime(2020, 1, 1, tzinfo=UTC) + datetime.timedelta(days=50)
        # 1.0 * 0.5 ** (50/14) ~= 0.084 < 0.2 -> excluded
        assert active_interests(user, now) == []
    finally:
        db.close()


def test_explicit_and_decayed_implicit_mixed_dedup_preserved(session_factory):
    db, user = _make_user_with_interests(session_factory, [
        dict(topic="ai", source="explicit", engagement_score=1.0,
             last_interacted_at=datetime.datetime(2020, 1, 1, tzinfo=UTC)),
        dict(topic="cloud", source="implicit", engagement_score=1.0,
             last_interacted_at=datetime.datetime(2020, 1, 1, tzinfo=UTC)),
        dict(topic="security", source="implicit", engagement_score=1.0,
             last_interacted_at=datetime.datetime(2020, 1, 1, tzinfo=UTC)),
    ])
    try:
        now = datetime.datetime(2020, 1, 1, tzinfo=UTC) + datetime.timedelta(days=50)
        # "ai" is explicit -> always in. "cloud"/"security" implicit at 50
        # days have decayed below 0.2 -> excluded.
        assert active_interests(user, now) == ["ai"]
    finally:
        db.close()
