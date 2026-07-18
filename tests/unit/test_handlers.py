"""Unit tests for newsagg.bot.handlers (Phase 3).

All DB access is a sqlite in-memory session (see conftest.py); Telegram
API calls are recorded by FakeTelegramAPI instead of hitting the network.
Free-text/RAG dispatch is intentionally not exercised here — it lazily
imports newsagg.api.query_engine / newsagg.api.observer, which pull in
chromadb/langgraph and are out of scope for these hermetic bot tests.
"""
import pytest

from newsagg.bot import handlers
from newsagg.db.schema import User

CHAT_ID = "555"


def _message_update(text, chat_id=CHAT_ID, first_name="Ada", update_id=1):
    return {
        "update_id": update_id,
        "message": {
            "message_id": 10,
            "chat": {"id": int(chat_id)},
            "from": {"id": int(chat_id), "first_name": first_name},
            "text": text,
        },
    }


def _callback_update(data, chat_id=CHAT_ID, message_id=10, update_id=2, cq_id="cq1"):
    return {
        "update_id": update_id,
        "callback_query": {
            "id": cq_id,
            "from": {"id": int(chat_id), "first_name": "Ada"},
            "message": {"message_id": message_id, "chat": {"id": int(chat_id)}},
            "data": data,
        },
    }


@pytest.mark.asyncio
async def test_start_creates_user_and_sends_keyboard(db_session_factory, fake_api):
    await handlers.handle_update(fake_api, _message_update("/start"))

    session = db_session_factory()
    try:
        user = session.query(User).filter(User.telegram_chat_id == CHAT_ID).one()
        assert user.telegram_chat_id == CHAT_ID
        assert user.first_name == "Ada"
        assert user.interests == []  # no default interest on signup
    finally:
        session.close()

    assert len(fake_api.sent_messages) == 1
    sent = fake_api.sent_messages[0]
    assert sent["chat_id"] == CHAT_ID
    assert sent["reply_markup"] is not None
    assert "inline_keyboard" in sent["reply_markup"]


@pytest.mark.asyncio
async def test_toggle_interest_add_then_remove(db_session_factory, fake_api):
    await handlers.handle_update(fake_api, _message_update("/start"))
    await handlers.handle_update(fake_api, _callback_update("t:ai", update_id=2))

    session = db_session_factory()
    try:
        user = session.query(User).filter(User.telegram_chat_id == CHAT_ID).one()
        topics = [i.topic for i in user.interests]
        assert topics == ["ai"]
        assert user.interests[0].source == "explicit"
    finally:
        session.close()

    await handlers.handle_update(fake_api, _callback_update("t:ai", update_id=3))

    session = db_session_factory()
    try:
        user = session.query(User).filter(User.telegram_chat_id == CHAT_ID).one()
        assert user.interests == []
    finally:
        session.close()

    # keyboard re-rendered in place both times, both toggles acknowledged
    assert len(fake_api.edited_markups) == 2
    assert len(fake_api.answered_callbacks) == 2
    assert fake_api.answered_callbacks[0]["text"] == "Added ✅"
    assert fake_api.answered_callbacks[1]["text"] == "Removed"


@pytest.mark.asyncio
async def test_done_with_zero_interests_keeps_picker_open(db_session_factory, fake_api):
    await handlers.handle_update(fake_api, _message_update("/start"))
    await handlers.handle_update(fake_api, _callback_update("t:done", update_id=2))

    # picker was re-rendered (edit_reply_markup), no confirmation sendMessage
    assert len(fake_api.edited_markups) == 1
    assert len(fake_api.sent_messages) == 1  # only the /start welcome
    assert len(fake_api.answered_callbacks) == 1
    nudge = fake_api.answered_callbacks[0]["text"]
    assert nudge  # non-empty nudge text telling the user to pick something


@pytest.mark.asyncio
async def test_cadence_and_hour_update_user_row(db_session_factory, fake_api):
    await handlers.handle_update(fake_api, _message_update("/start"))
    await handlers.handle_update(fake_api, _callback_update("c:weekly", update_id=2))
    await handlers.handle_update(fake_api, _callback_update("h:9", update_id=3))

    session = db_session_factory()
    try:
        user = session.query(User).filter(User.telegram_chat_id == CHAT_ID).one()
        assert user.delivery_cadence == "weekly"
        assert user.delivery_hour_utc == 9
    finally:
        session.close()


@pytest.mark.asyncio
async def test_brief_with_no_rows_sends_friendly_message(db_session_factory, fake_api):
    await handlers.handle_update(fake_api, _message_update("/start"))
    await handlers.handle_update(fake_api, _callback_update("t:ai", update_id=2))

    await handlers.handle_update(fake_api, _message_update("/brief", update_id=3))

    last = fake_api.sent_messages[-1]
    assert "no brief yet" in last["text"].lower()


@pytest.mark.asyncio
async def test_brief_with_no_interests_prompts_topics(db_session_factory, fake_api):
    await handlers.handle_update(fake_api, _message_update("/start"))
    await handlers.handle_update(fake_api, _message_update("/brief", update_id=2))

    last = fake_api.sent_messages[-1]
    assert "/topics" in last["text"]
