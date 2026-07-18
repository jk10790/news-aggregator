"""Topic-centric brief engine (ADR-5 — replaces processor/daily_brief.py).

One "topic module" per active topic per day, cached in Postgres
(TopicModule); per-user briefs are template-stitched from modules with
zero per-user LLM calls (ADR-7 — Brief rows, not JSON files).
"""
import datetime
import html
import json
import logging
import os

from pydantic import BaseModel, Field

from newsagg import config
from newsagg.core import taxonomy
from newsagg.core.llm import complete
from newsagg.db.database import SessionLocal
from newsagg.db.schema import Brief, TopicModule, User

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Contracts
# ---------------------------------------------------------------------------


class ModuleItem(BaseModel):
    title: str
    url: str
    summary_line: str


class TopicModuleContent(BaseModel):
    topic: str
    headline: str
    items: list[ModuleItem] = Field(min_length=1, max_length=5)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

IMPLICIT_HALF_LIFE_DAYS = 14.0
IMPLICIT_MIN_SCORE = 0.2
QUIET_DAY_MESSAGE = (
    "It's a quiet day across your topics — nothing cleared the bar for "
    "today's brief. We'll be back next time something worth reading shows up."
)

_PROMPT_PATH = os.path.join(os.path.dirname(__file__), "prompts", "topic_module_prompt.txt")
try:
    with open(_PROMPT_PATH, "r") as _f:
        TOPIC_MODULE_SYSTEM_PROMPT = _f.read()
except FileNotFoundError:
    logger.error("topic_module_prompt.txt not found at %s", _PROMPT_PATH)
    TOPIC_MODULE_SYSTEM_PROMPT = ""


# ---------------------------------------------------------------------------
# Interest decay (ADR-13)
# ---------------------------------------------------------------------------


def _as_utc(value: datetime.datetime) -> datetime.datetime:
    """Normalizes a datetime to tz-aware UTC. Sqlite/in-memory test fixtures
    frequently hand back naive datetimes even though the Postgres schema
    declares DateTime(timezone=True), so both sides of any comparison in
    this module get normalized through here rather than assuming either
    shape.
    """
    if value.tzinfo is None:
        return value.replace(tzinfo=datetime.timezone.utc)
    return value.astimezone(datetime.timezone.utc)


def _decayed_implicit_score(engagement_score: float, days_since: float) -> float:
    """Exponential decay with a 14-day half-life: at exactly one half-life
    (14 days) the score is halved; at two half-lives (28 days) it's a
    quarter, etc.
    """
    return engagement_score * (0.5 ** (days_since / IMPLICIT_HALF_LIFE_DAYS))


def active_interests(user, now: datetime.datetime) -> list[str]:
    """Topic slugs this user's brief should draw on right now.

    Explicit interests (source == "explicit") are ALWAYS included, no
    matter how stale (ADR-13) — the user opted in directly.

    Implicit interests (source == "implicit", inferred by the Observer)
    decay per `_decayed_implicit_score` and are only included while the
    decayed score is >= 0.2.
    """
    now_utc = _as_utc(now)
    slugs: list[str] = []
    seen: set[str] = set()

    for interest in user.interests:
        if interest.source == "explicit":
            include = True
        else:
            last = _as_utc(interest.last_interacted_at)
            days_since = (now_utc - last).total_seconds() / 86400.0
            decayed = _decayed_implicit_score(interest.engagement_score, days_since)
            include = decayed >= IMPLICIT_MIN_SCORE

        if include and interest.topic not in seen:
            seen.add(interest.topic)
            slugs.append(interest.topic)

    return slugs


# ---------------------------------------------------------------------------
# Chroma boundary
#
# NOTE (Phase 6 concurrency): newsagg.storage.vector_store is being rewritten
# concurrently by another agent to expose `topic_filter(slugs)` and a lazy
# `get_collection()` accessor. At the time this file was written the rewrite
# had not landed yet (vector_store.py only exposed `get_or_create_collection`
# and stored topics as a comma-joined string, not the per-topic boolean keys
# `taxonomy.chroma_key()` implies). The helpers below prefer the new names
# and fall back to a local equivalent so this module stays importable and
# testable either way; unit tests mock `_get_collection` directly so the
# fallback path is never exercised by the test suite.
# ---------------------------------------------------------------------------


def _get_collection():
    from newsagg.storage import vector_store

    if hasattr(vector_store, "get_collection"):
        return vector_store.get_collection()
    return vector_store.get_or_create_collection()


def _topic_where(slug: str, cutoff_int: int) -> dict:
    from newsagg.storage import vector_store

    date_clause = {"published_int": {"$gte": cutoff_int}}

    if slug == "top":
        return {"$and": [{"type": "parent"}, {"importance_score": {"$gte": 8}}, date_clause]}

    if hasattr(vector_store, "topic_filter"):
        topic_clause = vector_store.topic_filter([slug])
    else:
        # Pre-rewrite fallback: the plan's per-topic boolean metadata key.
        topic_clause = {taxonomy.chroma_key(slug): True}

    return {"$and": [{"type": "parent"}, topic_clause, date_clause]}


def fetch_topic_articles(slug: str, now: datetime.datetime) -> list[dict]:
    """Fetches candidate articles for one topic module via Chroma get()
    (metadata filter, not a similarity query) restricted to the last
    BRIEF_LOOKBACK_HOURS, sorted by importance_score descending and capped
    at TOPIC_MODULE_MAX_ARTICLES.
    """
    now_utc = _as_utc(now)
    cutoff = now_utc - datetime.timedelta(hours=config.BRIEF_LOOKBACK_HOURS)
    cutoff_int = int(cutoff.strftime("%Y%m%d"))

    where = _topic_where(slug, cutoff_int)
    collection = _get_collection()
    result = collection.get(where=where, include=["metadatas", "documents"])

    metadatas = result.get("metadatas") or []
    documents = result.get("documents") or []

    articles: list[dict] = []
    for i, meta in enumerate(metadatas):
        if not meta:
            continue
        doc_text = documents[i] if i < len(documents) else ""
        articles.append({
            "title": meta.get("title", ""),
            "url": meta.get("url", ""),
            "summary": meta.get("summary", doc_text),
            "importance_score": meta.get("importance_score", 0),
        })

    articles.sort(key=lambda a: a["importance_score"], reverse=True)
    return articles[: config.TOPIC_MODULE_MAX_ARTICLES]


# ---------------------------------------------------------------------------
# Topic module build (one LLM call per topic per day, cached)
# ---------------------------------------------------------------------------


def _build_user_prompt(topic_label: str, articles: list[dict]) -> str:
    lines = [
        f"Topic: {topic_label}",
        "",
        "Articles (use ONLY these; copy title and url verbatim):",
    ]
    for a in articles:
        lines.append(json.dumps({
            "title": a["title"],
            "url": a["url"],
            "summary": a.get("summary", ""),
        }))
    return "\n".join(lines)


async def build_topic_module(slug: str, date) -> TopicModuleContent | None:
    """Idempotent: a TopicModule row already existing for (slug, date) is
    returned as-is with zero LLM calls. Otherwise fetches candidate
    articles; if none, returns None (and nothing is persisted, so a later
    call can retry once articles show up). Otherwise makes exactly ONE
    core.llm.complete() call and persists the result.
    """
    db = SessionLocal()
    try:
        existing = (
            db.query(TopicModule)
            .filter(TopicModule.topic == slug, TopicModule.module_date == date)
            .first()
        )
        if existing is not None:
            return TopicModuleContent.model_validate(existing.content)

        # `date` is a calendar date; anchor the lookback window at the end
        # of that day so fetch_topic_articles' rolling BRIEF_LOOKBACK_HOURS
        # window covers the module's full day.
        now = datetime.datetime.combine(date, datetime.time.max, tzinfo=datetime.timezone.utc)
        articles = fetch_topic_articles(slug, now)
        if not articles:
            return None

        topic_meta = taxonomy.BY_SLUG.get(slug)
        topic_label = topic_meta.label if topic_meta else slug
        user_prompt = _build_user_prompt(topic_label, articles)

        content: TopicModuleContent = await complete(
            tier="standard",
            system=TOPIC_MODULE_SYSTEM_PROMPT,
            user=user_prompt,
            response_model=TopicModuleContent,
            context="TopicModule",
        )

        db.add(TopicModule(topic=slug, module_date=date, content=content.model_dump()))
        db.commit()
        return content
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Per-user assembly (zero LLM calls)
# ---------------------------------------------------------------------------


def _render_module_html(slug: str, content: TopicModuleContent) -> str:
    topic_meta = taxonomy.BY_SLUG.get(slug)
    label = topic_meta.label if topic_meta else slug
    emoji = f"{topic_meta.emoji} " if topic_meta else ""

    lines = [f"{emoji}<b>{html.escape(label)}</b>", html.escape(content.headline), ""]
    for item in content.items:
        lines.append(
            f'• <a href="{html.escape(item.url)}">{html.escape(item.title)}</a> '
            f"— {html.escape(item.summary_line)}"
        )
    return "\n".join(lines)


def assemble_brief(user, modules: dict, now: datetime.datetime | None = None) -> str:
    """Stitches cached TopicModuleContent objects into one HTML message.
    ZERO LLM calls — pure template assembly, html.escape()'d throughout,
    only <b> and <a href> tags. Always records a Brief row for today
    (idempotent), even on a quiet day where every module is None, so the
    scheduler's "already briefed today" check works whether or not the
    brief actually had content.

    `now` is optional and defaults to wall-clock UTC now; run_hour passes
    its own `now` through explicitly so the persisted brief_date always
    matches the hour being processed rather than wall-clock time (this
    matters for tests driving run_hour with a fixed `now`).
    """
    sections = []
    for slug, content in modules.items():
        if content is None:
            continue
        sections.append(_render_module_html(slug, content))

    if sections:
        first_name = getattr(user, "first_name", None)
        greeting = f"Good morning, {html.escape(first_name)}!" if first_name else "Good morning!"
        html_text = greeting + "\n\n" + "\n\n".join(sections)
    else:
        html_text = QUIET_DAY_MESSAGE

    _persist_brief(user, html_text, now)
    return html_text


def _persist_brief(user, html_text: str, now: datetime.datetime | None = None) -> None:
    today = _as_utc(now).date() if now is not None else datetime.datetime.now(datetime.timezone.utc).date()
    db = SessionLocal()
    try:
        existing = (
            db.query(Brief)
            .filter(Brief.user_id == user.id, Brief.brief_date == today)
            .first()
        )
        if existing is None:
            db.add(Brief(user_id=user.id, brief_date=today, content={"html": html_text}))
            db.commit()
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Delivery
# ---------------------------------------------------------------------------


async def deliver(user, html_text: str, now: datetime.datetime | None = None) -> bool:
    """Sends the assembled brief over Telegram (ADR-2/ADR-9 — the one
    product bot). Imports TelegramAPI lazily so this module stays importable
    before/without the bot package's real network dependencies. On success,
    marks today's Brief row delivered_at.

    `now` is optional (see assemble_brief) so run_hour can pin the
    delivered-brief lookup to the hour being processed instead of
    wall-clock time.
    """
    from newsagg.bot.telegram_api import TelegramAPI

    api = TelegramAPI(config.TELEGRAM_BOT_TOKEN)
    try:
        await api.send_message(user.telegram_chat_id, html_text)
    except Exception as e:  # noqa: BLE001 — delivery boundary, must never raise
        logger.error("Failed to deliver brief to user %s: %s", user.id, e)
        return False

    _mark_delivered(user, now)
    return True


def _mark_delivered(user, now: datetime.datetime | None = None) -> None:
    today = _as_utc(now).date() if now is not None else datetime.datetime.now(datetime.timezone.utc).date()
    db = SessionLocal()
    try:
        brief = (
            db.query(Brief)
            .filter(Brief.user_id == user.id, Brief.brief_date == today)
            .first()
        )
        if brief is not None:
            brief.delivered_at = datetime.datetime.now(datetime.timezone.utc)
            db.commit()
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Scheduler entry point
# ---------------------------------------------------------------------------


def _is_due(user, now: datetime.datetime) -> bool:
    if user.delivery_cadence == "paused":
        return False
    if user.delivery_hour_utc != now.hour:
        return False
    if user.delivery_cadence == "weekly":
        return now.weekday() == 0  # Monday
    return user.delivery_cadence == "daily"


async def run_hour(now: datetime.datetime) -> None:
    """Runs once per scheduler tick where now.minute == 0. Finds users due
    this hour (daily every day, weekly only Monday), skips paused users and
    anyone already holding a Brief row for today (idempotent against
    scheduler restarts/re-ticks), unions their active-interest topic slugs,
    builds each topic module at most once, then assembles + delivers per
    user.
    """
    db = SessionLocal()
    try:
        today = now.date()
        all_users = db.query(User).filter(User.delivery_hour_utc == now.hour).all()

        due_users = []
        for user in all_users:
            if not _is_due(user, now):
                continue
            already = (
                db.query(Brief)
                .filter(Brief.user_id == user.id, Brief.brief_date == today)
                .first()
            )
            if already is not None:
                continue
            due_users.append(user)

        if not due_users:
            return

        user_topics: dict[int, list[str]] = {}
        topic_slugs: set[str] = set()
        for user in due_users:
            slugs = active_interests(user, now)
            user_topics[user.id] = slugs
            topic_slugs.update(slugs)

        modules: dict[str, TopicModuleContent | None] = {}
        for slug in topic_slugs:
            try:
                modules[slug] = await build_topic_module(slug, today)
            except Exception as e:  # noqa: BLE001 — one bad topic must not sink the run
                logger.error("build_topic_module failed for topic %s: %s", slug, e)
                modules[slug] = None

        for user in due_users:
            user_modules = {slug: modules.get(slug) for slug in user_topics[user.id]}
            html_text = assemble_brief(user, user_modules, now=now)
            await deliver(user, html_text, now=now)
    finally:
        db.close()
