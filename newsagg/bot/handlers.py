"""Transport-agnostic Telegram command/callback/text handlers (Phase 3).

Both the long-poll loop (bot/poller.py) and the webhook endpoint
(api/main.py) call handle_update(api, update) with a TelegramAPI-shaped
object and the raw Telegram update dict — same logic, different
transport (ADR-2).

Callback-data protocol (Telegram caps callback_data at 64 bytes):
    t:<slug>            toggle interest <slug>
    t:done              close picker, confirm (or nudge if 0 interests)
    c:daily/weekly/paused   set delivery cadence
    h:<0-23>            set delivery hour (UTC)
"""
import html
import logging

from newsagg.core.taxonomy import BY_SLUG, TAXONOMY
from newsagg.db.database import SessionLocal
from newsagg.db.schema import Brief, Interest, User

logger = logging.getLogger(__name__)

CADENCES = ("daily", "weekly", "paused")
HOURS_PER_ROW = 6

HELP_TEXT = (
    "<b>Commands</b>\n"
    "/start - onboard / re-show the welcome message\n"
    "/topics - pick the topics you want news about\n"
    "/schedule - set your delivery cadence and hour (UTC)\n"
    "/brief - view your latest brief\n"
    "/help - this message\n\n"
    "Or just send me a message and ask about the news — I'll look it up."
)


# ---------------------------------------------------------------------------
# Keyboards
# ---------------------------------------------------------------------------

def interest_keyboard(selected: set[str]) -> dict:
    rows, row = [], []
    for t in TAXONOMY:
        mark = "✅ " if t.slug in selected else ""
        row.append({"text": f"{mark}{t.emoji} {t.label}", "callback_data": f"t:{t.slug}"})
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([{"text": "Done ✔️", "callback_data": "t:done"}])
    return {"inline_keyboard": rows}


def schedule_keyboard(cadence: str, hour: int) -> dict:
    cadence_row = [
        {"text": ("✅ " if cadence == "daily" else "") + "Daily", "callback_data": "c:daily"},
        {"text": ("✅ " if cadence == "weekly" else "") + "Weekly", "callback_data": "c:weekly"},
        {"text": ("✅ " if cadence == "paused" else "") + "Pause", "callback_data": "c:paused"},
    ]
    rows = [cadence_row]
    row = []
    for h in range(24):
        label = f"✅ {h:02d}" if h == hour else f"{h:02d}"
        row.append({"text": label, "callback_data": f"h:{h}"})
        if len(row) == HOURS_PER_ROW:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    return {"inline_keyboard": rows}


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def get_or_create_user(db, chat_id: str, first_name: str | None) -> User:
    user = db.query(User).filter(User.telegram_chat_id == chat_id).first()
    if user is None:
        user = User(telegram_chat_id=chat_id, first_name=first_name)
        db.add(user)
        db.commit()
        db.refresh(user)
    return user


def _render_brief_content(content) -> str:
    """Render a Brief.content JSON blob for chat display.

    processor/brief_engine.py's assemble_brief() persists
    `Brief.content = {"html": html_text}` where html_text is already
    fully escaped/ready for parse_mode="HTML" (see
    brief_engine._persist_brief). Render it as-is; fall back to a
    defensively-escaped str() only for unexpected/legacy shapes.
    """
    if isinstance(content, dict) and content.get("html"):
        return content["html"]
    return html.escape(str(content))


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

async def _handle_start(api, chat_id: str, message: dict) -> None:
    from_user = message.get("from") or {}
    first_name = from_user.get("first_name")

    db = SessionLocal()
    try:
        user = get_or_create_user(db, chat_id, first_name)
        selected = {i.topic for i in user.interests}
    finally:
        db.close()

    greeting = f", {html.escape(first_name)}" if first_name else ""
    text = (
        f"\U0001f44b Welcome{greeting}! I curate a personalized news brief just for you.\n\n"
        "Pick the topics you care about below — tap Done when you're finished."
    )
    await api.send_message(chat_id, text, reply_markup=interest_keyboard(selected))


async def _handle_topics(api, chat_id: str) -> None:
    db = SessionLocal()
    try:
        user = db.query(User).filter(User.telegram_chat_id == chat_id).first()
        selected = {i.topic for i in user.interests} if user else set()
    finally:
        db.close()
    await api.send_message(chat_id, "Your topics:", reply_markup=interest_keyboard(selected))


async def _handle_schedule(api, chat_id: str) -> None:
    db = SessionLocal()
    try:
        user = db.query(User).filter(User.telegram_chat_id == chat_id).first()
        cadence = user.delivery_cadence if user else "daily"
        hour = user.delivery_hour_utc if user else 7
    finally:
        db.close()
    await api.send_message(
        chat_id,
        "Choose your delivery cadence and hour. All times are UTC.",
        reply_markup=schedule_keyboard(cadence, hour),
    )


async def _handle_brief(api, chat_id: str) -> None:
    db = SessionLocal()
    try:
        user = db.query(User).filter(User.telegram_chat_id == chat_id).first()
        if user is None:
            await api.send_message(chat_id, "Send /start first to get set up.")
            return

        has_interests = (
            db.query(Interest).filter(Interest.user_id == user.id).first() is not None
        )
        if not has_interests:
            await api.send_message(
                chat_id,
                "You haven't picked any topics yet — use /topics to choose what "
                "you'd like to hear about.",
            )
            return

        brief = (
            db.query(Brief)
            .filter(Brief.user_id == user.id)
            .order_by(Brief.brief_date.desc())
            .first()
        )
        if brief is None:
            await api.send_message(
                chat_id,
                f"No brief yet — you'll get your first one at {user.delivery_hour_utc:02d}:00 UTC.",
            )
            return

        await api.send_message(chat_id, _render_brief_content(brief.content))
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Callback handling
# ---------------------------------------------------------------------------

async def handle_callback(api, callback_query: dict) -> None:
    cq_id = callback_query["id"]
    data = callback_query.get("data", "")
    message = callback_query["message"]
    chat_id = str(message["chat"]["id"])
    message_id = message["message_id"]
    from_user = callback_query.get("from") or {}

    db = SessionLocal()
    try:
        user = get_or_create_user(db, chat_id, from_user.get("first_name"))

        if data.startswith("t:") and data != "t:done":
            slug = data[2:]
            if slug not in BY_SLUG:
                await api.answer_callback(cq_id, "Unknown topic")
                return
            existing = (
                db.query(Interest)
                .filter(Interest.user_id == user.id, Interest.topic == slug)
                .first()
            )
            if existing:
                db.delete(existing)
                db.commit()
                answer_text = "Removed"
            else:
                db.add(
                    Interest(user_id=user.id, topic=slug, source="explicit", engagement_score=1.0)
                )
                db.commit()
                answer_text = "Added ✅"

            selected = {
                i.topic for i in db.query(Interest).filter(Interest.user_id == user.id).all()
            }
            await api.edit_reply_markup(chat_id, message_id, interest_keyboard(selected))
            await api.answer_callback(cq_id, answer_text)

        elif data == "t:done":
            interests = db.query(Interest).filter(Interest.user_id == user.id).all()
            if interests:
                await api.answer_callback(cq_id, "All set!")
                hour = user.delivery_hour_utc
                await api.send_message(
                    chat_id,
                    f"You're set! First brief at {hour:02d}:00 UTC. "
                    "Change anytime with /topics or /schedule.",
                )
            else:
                await api.answer_callback(cq_id, "Pick at least one topic first")
                await api.edit_reply_markup(chat_id, message_id, interest_keyboard(set()))

        elif data.startswith("c:"):
            cadence = data[2:]
            if cadence not in CADENCES:
                await api.answer_callback(cq_id, "Unknown option")
                return
            user.delivery_cadence = cadence
            db.commit()
            await api.edit_reply_markup(
                chat_id, message_id, schedule_keyboard(cadence, user.delivery_hour_utc)
            )
            await api.answer_callback(cq_id, f"Cadence set: {cadence}")

        elif data.startswith("h:"):
            try:
                hour = int(data[2:])
            except ValueError:
                hour = -1
            if not (0 <= hour <= 23):
                await api.answer_callback(cq_id, "Unknown hour")
                return
            user.delivery_hour_utc = hour
            db.commit()
            await api.edit_reply_markup(
                chat_id, message_id, schedule_keyboard(user.delivery_cadence, hour)
            )
            await api.answer_callback(cq_id, f"Delivery hour set: {hour:02d}:00 UTC")

        else:
            logger.warning("Unrecognized callback_data: %r", data)
            await api.answer_callback(cq_id, "")
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Free text -> RAG
# ---------------------------------------------------------------------------

async def handle_free_text(api, chat_id: str, text: str) -> None:
    db = SessionLocal()
    try:
        get_or_create_user(db, chat_id, None)
    finally:
        db.close()

    # Imported lazily so importing newsagg.bot.handlers (and running bot
    # unit tests) never drags in chromadb/langgraph — those live behind
    # newsagg.api.query_engine / newsagg.api.observer.
    import asyncio

    from newsagg.api.observer import observe_conversation
    from newsagg.api.query_engine import query_news_rag

    # observer.observe_conversation looks the user up by telegram_chat_id
    # (a string), not the integer User.id primary key — pass the chat id.
    asyncio.create_task(observe_conversation(str(chat_id), text))

    try:
        answer = await query_news_rag(text, str(chat_id))
    except Exception:
        logger.exception("query_news_rag failed for chat %s", chat_id)
        answer = "Sorry, I hit a snag looking that up — please try again in a moment."

    await api.send_message(chat_id, html.escape(answer))


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------

COMMAND_HANDLERS = {
    "/topics": lambda api, chat_id, message: _handle_topics(api, chat_id),
    "/schedule": lambda api, chat_id, message: _handle_schedule(api, chat_id),
    "/brief": lambda api, chat_id, message: _handle_brief(api, chat_id),
}


async def handle_update(api, update: dict) -> None:
    if "callback_query" in update:
        await handle_callback(api, update["callback_query"])
        return

    message = update.get("message")
    if not message or "text" not in message:
        return

    chat_id = str(message["chat"]["id"])
    text = message["text"].strip()

    if not text.startswith("/"):
        await handle_free_text(api, chat_id, text)
        return

    command = text.split()[0].split("@")[0].lower()
    if command == "/start":
        await _handle_start(api, chat_id, message)
    elif command == "/help":
        await api.send_message(chat_id, HELP_TEXT)
    elif command in COMMAND_HANDLERS:
        await COMMAND_HANDLERS[command](api, chat_id, message)
    else:
        await api.send_message(chat_id, "Unknown command.\n\n" + HELP_TEXT)
