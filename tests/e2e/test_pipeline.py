"""End-to-end pipeline test (Phase 9 — the single most important test in the
overhaul). Requires `docker compose up` (real Postgres, Redpanda, Chroma);
only the LLM (newsagg.core.llm.complete, patched at each call site's own
module) and the outbound Telegram sendMessage call (TelegramAPI.send_message,
monkeypatched — see the note at step 5 for why this isn't respx) are mocked.
Everything else — Postgres writes, Kafka produce/consume, Chroma storage +
real sentence-transformer embeddings — is real infra.

Scenario (mirrors docs/OVERHAUL_PLAN.md Phase 9):
  1. Seed a user via newsagg.bot.handlers.handle_update: /start, t:ai, t:done.
  2. Publish one AI-topic ArticleRaw fixture to the real `raw-articles` topic.
  3. Run one triage batch (newsagg.ingestion.triage.process_batch) with
     core.llm.complete canned to a fixed TriageOutput.
  4. Run one storage batch (newsagg.storage.consumer.process_batch) against
     real Chroma with real embeddings.
  5. Run newsagg.processor.brief_engine.run_hour at the user's delivery hour
     with the module LLM canned to a fixed TopicModuleContent and Telegram
     sendMessage mocked.
  6. Assert exactly one sendMessage to the seeded chat id containing the
     fixture article's title + url; a Brief row exists with delivered_at
     set; a second run_hour at the same hour sends nothing more.

Uses a fixed chat id + a per-run UUID-suffixed article title/url so reruns
(same day, same process or a fresh one) are idempotent: the `clean_db`
fixture deletes any leftover User/Interest/Brief rows for that chat id, any
cached TopicModule row for today's "ai" module, and any Chroma docs left
over from a previous run of this test (source="e2e-fixture-feed") before
each run — the last of these matters because ChromaDB's semantic-dedup
check (newsagg.storage.vector_store.DEDUP_DISTANCE_THRESHOLD) would
otherwise mistake this run's fixture for a near-duplicate of a previous
run's similarly-worded "AI benchmark breakthrough" fixture and silently
skip the insert, even though the title/url/RUN_ID differ.
"""
import datetime
import json
import uuid

import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from newsagg import config
from newsagg.bot import handlers
from newsagg.bot.telegram_api import TelegramAPI
from newsagg.core.models import ArticleRaw
from newsagg.db.database import SessionLocal
from newsagg.db.schema import Brief, TopicModule, User
from newsagg.ingestion.triage import TriageOutput
import newsagg.ingestion.triage as triage_module
from newsagg.processor.brief_engine import ModuleItem, TopicModuleContent, run_hour
import newsagg.processor.brief_engine as brief_engine_module
from newsagg.storage import consumer as storage_consumer_module
from newsagg.storage.vector_store import generate_url_hash, get_or_create_collection

pytestmark = pytest.mark.e2e

CHAT_ID = "999000111"
RUN_ID = uuid.uuid4().hex[:8]
FIXTURE_TITLE = f"E2E fixture {RUN_ID}: researchers unveil new AI benchmark model"
FIXTURE_URL = f"https://example-e2e-fixture.invalid/{RUN_ID}"
FIXTURE_SUMMARY = (
    "A research lab announced a new large language model that sets a new "
    "state-of-the-art on several public reasoning benchmarks."
)


def _today_utc() -> datetime.date:
    return datetime.datetime.now(datetime.timezone.utc).date()


def _delivery_now() -> datetime.datetime:
    """The scheduler 'now' for run_hour — matches User.delivery_hour_utc's
    schema default (7 UTC), so no schedule callback is needed on top of the
    plan's /start + t:ai + t:done seed sequence."""
    return datetime.datetime.combine(_today_utc(), datetime.time(7, 0), tzinfo=datetime.timezone.utc)


class _RecordingTelegramAPI:
    """Stands in for the transport during user seeding (step 1) — the real
    TelegramAPI is only exercised later, inside brief_engine.deliver, where
    respx intercepts it at the HTTP layer instead."""

    def __init__(self):
        self.sent = []

    async def send_message(self, chat_id, text, reply_markup=None):
        self.sent.append({"chat_id": chat_id, "text": text, "reply_markup": reply_markup})
        return {"ok": True, "result": {"message_id": len(self.sent)}}

    async def edit_reply_markup(self, chat_id, message_id, reply_markup):
        return {"ok": True}

    async def answer_callback(self, callback_query_id, text=""):
        return {"ok": True}


def _msg_update(text: str, update_id: int) -> dict:
    return {
        "update_id": update_id,
        "message": {
            "message_id": update_id,
            "chat": {"id": int(CHAT_ID)},
            "from": {"id": int(CHAT_ID), "first_name": "E2E"},
            "text": text,
        },
    }


def _cb_update(data: str, update_id: int) -> dict:
    return {
        "update_id": update_id,
        "callback_query": {
            "id": f"cq{update_id}",
            "from": {"id": int(CHAT_ID), "first_name": "E2E"},
            "message": {"message_id": update_id, "chat": {"id": int(CHAT_ID)}},
            "data": data,
        },
    }


@pytest.fixture
def clean_db():
    """Real-infra cleanup so reruns of this test are idempotent:

    - Postgres: removes any leftover User/Interest/Brief rows for the fixed
      e2e chat id (cascade deletes Interest via the ORM relationship) and
      any TopicModule row already cached for today's 'ai' module, so
      build_topic_module is guaranteed to invoke the (canned) LLM fresh
      every run rather than silently returning a stale cached row.
    - Chroma: removes any parent/child docs left over from a *previous* run
      of this test (tagged `source="e2e-fixture-feed"`). Without this, a
      prior run's fixture article — semantically similar "AI benchmark
      breakthrough" wording — sits in the collection and
      vector_store.store_article's semantic-dedup check (distance <
      DEDUP_DISTANCE_THRESHOLD) mistakes this run's fresh fixture for a
      near-duplicate of it and silently skips the insert, even though the
      title/url/RUN_ID are different.
    """
    db = SessionLocal()
    try:
        user = db.query(User).filter(User.telegram_chat_id == CHAT_ID).first()
        if user is not None:
            db.query(Brief).filter(Brief.user_id == user.id).delete()
            db.delete(user)
        db.query(TopicModule).filter(
            TopicModule.topic == "ai", TopicModule.module_date == _today_utc()
        ).delete()
        db.commit()
    finally:
        db.close()

    collection = get_or_create_collection()
    stray = collection.get(where={"source": {"$eq": "e2e-fixture-feed"}}, include=[])
    stray_ids = stray.get("ids") or []
    if stray_ids:
        collection.delete(ids=stray_ids)

    yield


@pytest.mark.asyncio
async def test_full_pipeline_seed_ingest_triage_store_deliver(clean_db):
    # ------------------------------------------------------------------
    # 1. Seed user via real handlers against real Postgres.
    # ------------------------------------------------------------------
    seed_api = _RecordingTelegramAPI()
    await handlers.handle_update(seed_api, _msg_update("/start", 1))
    await handlers.handle_update(seed_api, _cb_update("t:ai", 2))
    await handlers.handle_update(seed_api, _cb_update("t:done", 3))

    db = SessionLocal()
    try:
        user = db.query(User).filter(User.telegram_chat_id == CHAT_ID).one()
        assert {i.topic for i in user.interests} == {"ai"}
        assert user.delivery_cadence == "daily"
        assert user.delivery_hour_utc == 7
    finally:
        db.close()

    # ------------------------------------------------------------------
    # 2. Publish one AI-topic ArticleRaw fixture to the real raw-articles
    #    topic (real Redpanda).
    # ------------------------------------------------------------------
    raw_article = ArticleRaw(
        source="e2e-fixture-feed",
        title=FIXTURE_TITLE,
        link=FIXTURE_URL,
        summary=FIXTURE_SUMMARY,
        published=datetime.datetime.now(datetime.timezone.utc).isoformat(),
        author="E2E",
    )
    producer = AIOKafkaProducer(bootstrap_servers=config.REDPANDA_BROKER)
    await producer.start()
    try:
        await producer.send_and_wait(
            config.TOPIC_RAW_ARTICLES, value=raw_article.model_dump_json().encode("utf-8")
        )
    finally:
        await producer.stop()

    # ------------------------------------------------------------------
    # 3. Run one triage batch with core.llm.complete canned to a fixed
    #    TriageOutput (real Redpanda consume + publish; LLM mocked).
    # ------------------------------------------------------------------
    canned_triage = TriageOutput(
        relevant=True,
        reasoning="High-impact AI research announcement.",
        topics=["ai"],
        entities=["Test Lab"],
        importance_score=9,
        key_insights=["New model beats prior public benchmarks."],
    )

    async def _fake_triage_complete(**kwargs):
        return canned_triage

    triage_consumer = AIOKafkaConsumer(
        config.TOPIC_RAW_ARTICLES,
        bootstrap_servers=config.REDPANDA_BROKER,
        group_id=f"e2e-triage-{RUN_ID}",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    triage_producer = AIOKafkaProducer(bootstrap_servers=config.REDPANDA_BROKER)
    await triage_consumer.start()
    await triage_producer.start()
    try:
        batches = await triage_consumer.getmany(timeout_ms=15000, max_records=50)
        raw_values = [
            msg.value
            for msgs in batches.values()
            for msg in msgs
            if json.loads(msg.value)["link"] == FIXTURE_URL
        ]
        assert raw_values, "fixture raw article was not consumed from raw-articles"

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(triage_module, "complete", _fake_triage_complete)
            await triage_module.process_batch(raw_values, triage_producer)
        await triage_consumer.commit()
    finally:
        await triage_consumer.stop()
        await triage_producer.stop()

    # ------------------------------------------------------------------
    # 4. Run one storage batch against real Chroma with real embeddings.
    # ------------------------------------------------------------------
    storage_consumer = AIOKafkaConsumer(
        config.TOPIC_VERIFIED_ARTICLES,
        bootstrap_servers=config.REDPANDA_BROKER,
        group_id=f"e2e-storage-{RUN_ID}",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    storage_dlq_producer = AIOKafkaProducer(bootstrap_servers=config.REDPANDA_BROKER)
    await storage_consumer.start()
    await storage_dlq_producer.start()
    try:
        batches = await storage_consumer.getmany(timeout_ms=15000, max_records=50)
        raw_values = [
            msg.value
            for msgs in batches.values()
            for msg in msgs
            if json.loads(msg.value)["link"] == FIXTURE_URL
        ]
        assert raw_values, "fixture verified article was not consumed from verified-articles"
        await storage_consumer_module.process_batch(raw_values, storage_dlq_producer)
        await storage_consumer.commit()
    finally:
        await storage_consumer.stop()
        await storage_dlq_producer.stop()

    # Verify the storage stage genuinely landed the fixture in Chroma with
    # the right taxonomy metadata (real importance_score, real topic_ai
    # boolean) *before* trusting the brief step below, whose module content
    # is itself LLM-mocked and therefore wouldn't otherwise prove storage
    # worked.
    collection = get_or_create_collection()
    parent_id = f"parent_{generate_url_hash(FIXTURE_URL)}"
    stored = collection.get(ids=[parent_id], include=["metadatas"])
    assert stored["ids"] == [parent_id], "fixture article parent doc missing from Chroma"
    assert stored["metadatas"][0]["topic_ai"] is True
    assert stored["metadatas"][0]["importance_score"] == 9

    # ------------------------------------------------------------------
    # 5. run_hour at the user's delivery hour: module LLM canned, Telegram
    #    sendMessage mocked.
    #
    #    NOTE / deviation from the plan's literal "respx against
    #    api.telegram.org": run_hour's fetch_topic_articles also makes real
    #    (unmocked) HTTP calls to Chroma in this same call. In this repo's
    #    installed respx==0.23.1 + httpx==0.28.1, respx.mock(...,
    #    assert_all_mocked=False) does NOT actually forward unmatched
    #    requests to the real network as "pass-through" — verified directly
    #    (a bare httpx GET/POST to the real Chroma container inside that
    #    context returns a synthetic empty 200 body instead of hitting the
    #    network), which makes fetch_topic_articles fail with a JSON decode
    #    error. Since respx intercepts at the global transport level, there
    #    is no way to scope it to only the Telegram client without also
    #    breaking the real Chroma calls in the same test. Per the task's
    #    documented fallback, we monkeypatch TelegramAPI.send_message
    #    directly instead — respx itself is still exercised end-to-end in
    #    tests/unit/test_llm_gateway.py, where nothing else needs the real
    #    network at the same time.
    # ------------------------------------------------------------------
    canned_module = TopicModuleContent(
        topic="ai",
        headline="Big AI news today",
        items=[ModuleItem(title=FIXTURE_TITLE, url=FIXTURE_URL, summary_line="Big breakthrough.")],
    )

    async def _fake_module_complete(**kwargs):
        return canned_module

    sent_messages: list[dict] = []

    async def _fake_send_message(self, chat_id, text, reply_markup=None):
        sent_messages.append({"chat_id": chat_id, "text": text})
        return {"ok": True, "result": {"message_id": len(sent_messages)}}

    now = _delivery_now()
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(brief_engine_module, "complete", _fake_module_complete)
        mp.setattr(TelegramAPI, "send_message", _fake_send_message)
        await run_hour(now)

    # ------------------------------------------------------------------
    # 6. Assertions: exactly one sendMessage, containing the fixture
    #    title + url; Brief row delivered; second run_hour sends nothing.
    # ------------------------------------------------------------------
    assert len(sent_messages) == 1
    assert sent_messages[0]["chat_id"] == CHAT_ID
    assert FIXTURE_TITLE in sent_messages[0]["text"]
    assert FIXTURE_URL in sent_messages[0]["text"]

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(brief_engine_module, "complete", _fake_module_complete)
        mp.setattr(TelegramAPI, "send_message", _fake_send_message)
        await run_hour(now)  # same hour again -> idempotent, nothing new sent

    assert len(sent_messages) == 1

    db = SessionLocal()
    try:
        user = db.query(User).filter(User.telegram_chat_id == CHAT_ID).one()
        brief = (
            db.query(Brief)
            .filter(Brief.user_id == user.id, Brief.brief_date == now.date())
            .one()
        )
        assert brief.delivered_at is not None
    finally:
        db.close()
