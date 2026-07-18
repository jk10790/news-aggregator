"""Shared fixtures for newsagg unit tests (hermetic — no infra).

DB: an in-memory SQLite engine, schema created via
newsagg.db.schema.Base.metadata.create_all() (a bare create_all() is
fine in tests — the plan's "Postgres via Alembic only" rule targets the
real app/migrations, not test fixtures). newsagg.bot.handlers opens
sessions via a module-level `SessionLocal` name (imported from
newsagg.db.database); we monkeypatch that name directly on the handlers
module so every handler call goes through the sqlite session factory.

A StaticPool is required: plain sqlite:///:memory: hands each new
connection a *fresh*, empty database, and handlers.py opens a new
SessionLocal() per call — without StaticPool, the second call would see
none of the first call's writes.
"""
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

import newsagg.bot.handlers as handlers_module
from newsagg.db.schema import Base


@pytest.fixture
def db_session_factory(monkeypatch):
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    testing_session_local = sessionmaker(autocommit=False, autoflush=False, bind=engine)

    monkeypatch.setattr(handlers_module, "SessionLocal", testing_session_local)

    yield testing_session_local

    engine.dispose()


class FakeTelegramAPI:
    """Records calls instead of hitting the real Telegram Bot API."""

    def __init__(self):
        self.sent_messages: list[dict] = []
        self.edited_markups: list[dict] = []
        self.answered_callbacks: list[dict] = []
        self._next_message_id = 1

    async def send_message(self, chat_id, text, reply_markup=None):
        record = {"chat_id": chat_id, "text": text, "reply_markup": reply_markup}
        self.sent_messages.append(record)
        message_id = self._next_message_id
        self._next_message_id += 1
        return {"ok": True, "result": {"message_id": message_id, "chat": {"id": chat_id}}}

    async def edit_reply_markup(self, chat_id, message_id, reply_markup):
        record = {"chat_id": chat_id, "message_id": message_id, "reply_markup": reply_markup}
        self.edited_markups.append(record)
        return {"ok": True}

    async def answer_callback(self, callback_query_id, text=""):
        record = {"callback_query_id": callback_query_id, "text": text}
        self.answered_callbacks.append(record)
        return {"ok": True}

    async def get_updates(self, offset):
        return []


@pytest.fixture
def fake_api():
    return FakeTelegramAPI()
