# News-Aggregator Overhaul Plan (v1 — Final Architecture)

**Status:** Approved for implementation
**Written:** 2026-07-17
**Executor:** Any competent coding agent (written for Claude Sonnet). Follow phases in order. Do not skip acceptance criteria.

---

## 1. Context — why this overhaul

The product promise: **a user finds the Telegram bot, signs up seamlessly, picks interests from presented options, and receives a personalized daily news brief at their chosen hour — zero operator involvement.**

Today that promise is broken:

- Telegram webhook stores the chat id into `User.phone_number`; delivery keys on `User.telegram_chat_id` (always NULL) → **no self-onboarded user ever receives a brief**.
- There is **no interest picker at all** — interests come only from a hardcoded `"Top News"` seed, low-volume implicit extraction, or an operator script.
- Scheduling is one server-local cron for everyone; `delivery_time_utc`, `timezone`, `delivery_cadence` columns exist but are never read.
- The main RAG path bypasses the Taut middleware whenever `GEMINI_API_KEY` is set; demo hacks (a fabricated "agentic" web result, a `"(CACHE BUST 3)"` prompt suffix) ship in prod code.
- `importance_score` is computed by the triage LLM then discarded (always defaults to 5 in storage).
- The e2e test suite asserts stale artifacts and makes live network calls — green tests, broken product.
- Development happened via string-replace patch scripts (`patch.py`, `patch2.py`, `patch_format.py`, `fix_brief.py`) leaving the tree half-patched.

Previous review iterations (see `~/.gemini/antigravity-cli/brain/7f09619b…/architecture_review.md` and `7263b72e…/architecture_ui_cadence.md`) diagnosed most of this correctly but were each half-implemented. This plan is the consolidation: **implement it fully, verify each phase, and the architecture question is closed.**

---

## 2. Locked architecture decisions (ADRs — do not relitigate)

| # | Decision | Rationale |
|---|----------|-----------|
| ADR-1 | **Telegram is the only delivery channel in v1.** Twilio/WhatsApp code is deleted (git history preserves it). Delivery goes through a small interface so a second channel is a new module, not a rewrite. | Half-maintained channels rot. Product intent is Telegram. |
| ADR-2 | **One product bot, long-polling by default.** `getUpdates` loop — works out-of-box on a laptop, no public URL/ngrok. The FastAPI webhook endpoint is kept as a deploy-time alternative; both call the same handler functions. **Drop the per-user `telegram_bot_token` column** — users never bring their own bots. | Seamless local out-of-box experience was the whole point. |
| ADR-3 | **All LLM calls go through the Taut proxy in OpenAI-compatible mode** (`AsyncOpenAI(base_url=TAUT_URL)`) via one shared client module. The Taut Python SDK import in `daily_brief.py` is removed (also fixes the undeclared-dependency problem — proxy mode needs no `taut` import). Tier selection via `X-Taut-Tier` header. If Taut is down, the client falls back to direct Gemini **inside the shared client only** — never at call sites. | Ends the proxy-vs-SDK split (FINDING-01) and the prod-path bypass. One choke point for retries, fallback, metrics. |
| ADR-4 | **Fixed topic taxonomy is the single source of truth**, defined once in `newsagg/core/taxonomy.py`. Triage classifies into it, Chroma stores per-topic boolean metadata, the interest picker renders it, the Observer maps free text into it. No free-form topic strings anywhere in metadata or interests. | Kills the `$eq`/`$contains`/substring-match thrash permanently. Booleans filter natively in ChromaDB 0.6.x. |
| ADR-5 | **Topic-centric brief generation, O(T) LLM calls.** One "topic module" per active topic per day, cached in Postgres; per-user briefs are template-stitched from modules with **zero** per-user LLM calls. | Per-user map-reduce scales cost O(users). This was the Jul-11 proposal's best idea. |
| ADR-6 | **Drop Prefect.** Scheduling is one long-running asyncio scheduler service (checks every minute, hour-gated work). Retries are plain `tenacity`-style loops in code. | Prefect was used as a decorator with no deployments/workers/UI — cost without benefit. |
| ADR-7 | **Postgres via Alembic only** (no `create_all`). Briefs and topic modules are stored in Postgres tables, not JSON files — fixes the dead `/brief` endpoint class of bugs. | Files-on-disk as API surface caused two shipped 404 bugs. |
| ADR-8 | **Explicit embeddings everywhere** (parents AND children) using the one `SentenceTransformer` instance from `newsagg/core/embeddings.py`. Never rely on Chroma's default embedder. | Mixed embedding sources currently work only because both happen to be 384-dim MiniLM. |
| ADR-9 | **Telegram messages use `parse_mode="HTML"`** with `html.escape()` on every piece of dynamic text. Never `Markdown` mode. | Three prior formatting hotfixes all fought Markdown-mode parse failures. |
| ADR-10 | **Proper Python package** (`newsagg/`), absolute imports, `pyproject.toml`, no `sys.path.append`. | Import hacks caused double module loads and CWD-dependent breakage. |
| ADR-11 | **Kafka consumers use manual offset commit + DLQ topic.** Commit only after a batch is fully processed; poison messages go to `triage-dlq` / `storage-dlq`. | Current auto-commit silently drops articles on crash/LLM failure. |
| ADR-12 | **No interest filter on conversational RAG retrieval.** Users may ask about anything; interests drive briefs only. RAG uses vector similarity + date + `type=parent` filters. | Removes the last "personalized retrieval returns nothing" failure mode; simpler and matches user expectation of a chatbot. |
| ADR-13 | **Explicit interests never decay; implicit interests decay** `0.5 ** (days_since_last_interaction / 14)`, dropped below 0.2. | A user who tapped a topic button said so explicitly; only AI guesses should fade. |
| ADR-14 | AWS migration, web dashboard, real-time alerts, WhatsApp: **out of scope for v1.** Hook points are noted (§12) so they need no re-architecture later. | Ship the promise first. |

---

## 3. Target architecture

```
                      ┌─────────────────────────────────────────────┐
                      │ docker-compose: redpanda, chroma, postgres, │
                      │                 taut, (redpanda-console)    │
                      └─────────────────────────────────────────────┘

 RSS feeds ──> ingestion/producer ──> [raw-articles] ──> ingestion/triage ──> [verified-articles]
   (cron/loop)      (Kafka)                (LLM via core/llm, tier=simple)        │
                                            failures -> [triage-dlq]              ▼
                                                                        storage/consumer
                                                                        (embed via core/embeddings,
                                                                         taxonomy bools, importance,
                                                                         semantic dedup, upsert)
                                                                                  │
                                                                                  ▼
                                                                              ChromaDB
                                                                                  │
 ┌────────────────────────── scheduler service (asyncio, 1-min tick) ────────────┤
 │  hourly: users due this hour -> union of their topics                         │
 │          -> topic modules (1 LLM call/topic, cached in Postgres)              │
 │          -> stitch per-user brief (0 LLM calls) -> deliver -> record          │
 │  daily 03:00 UTC: chroma retention cleanup                                    │
 └───────────────────────────────────────────────────────────────────────────────┘
                                                                                  │
 Telegram user <──> bot service (long-poll getUpdates)                            │
   /start  -> create user (telegram_chat_id), interest picker (inline keyboard)   │
   /topics -> picker    /schedule -> cadence+hour picker    /brief -> latest brief│
   free text -> RAG (LangGraph CRAG via core/llm) <───────────────────────────────┘
               └─> observer (implicit interests, taxonomy-constrained)

 FastAPI (api/) : /query (NDJSON stream), /brief/{chat_id}, /webhook/telegram (deploy mode), /health
```

---

## 4. Final repo layout

```
news-aggregator/
├── pyproject.toml                  # NEW — package def, deps, entry points
├── docker-compose.yml
├── alembic/                        # migrations (env.py updated for new metadata import)
├── newsagg/
│   ├── __init__.py
│   ├── config.py                   # moved from root; env parsing only
│   ├── core/
│   │   ├── __init__.py
│   │   ├── taxonomy.py             # NEW — ADR-4 single source of truth
│   │   ├── llm.py                  # NEW — ADR-3 single LLM client
│   │   ├── embeddings.py           # NEW — ADR-8 single embedder
│   │   └── models.py               # Pydantic contracts (from root models.py)
│   ├── db/
│   │   ├── __init__.py
│   │   ├── database.py             # engine, SessionLocal (from root database.py)
│   │   └── schema.py               # SQLAlchemy models (User, Interest, Brief, TopicModule)
│   ├── ingestion/
│   │   ├── __init__.py
│   │   ├── producer.py
│   │   └── triage.py               # from consumer_triage.py
│   ├── storage/
│   │   ├── __init__.py
│   │   ├── consumer.py             # from consumer_storage.py
│   │   ├── vector_store.py
│   │   └── cleanup.py
│   ├── processor/
│   │   ├── __init__.py
│   │   ├── brief_engine.py         # NEW — replaces daily_brief.py
│   │   └── prompts/topic_module_prompt.txt   # NEW (reduce_prompt.txt retired)
│   ├── bot/
│   │   ├── __init__.py
│   │   ├── telegram_api.py         # NEW — thin httpx wrapper for Bot API
│   │   ├── handlers.py             # NEW — command/callback/text handlers (transport-agnostic)
│   │   └── poller.py               # NEW — long-poll loop entry point
│   ├── scheduler/
│   │   ├── __init__.py
│   │   └── main.py                 # NEW — ADR-6 scheduler service
│   └── api/
│       ├── __init__.py
│       ├── main.py                 # FastAPI app
│       ├── query_engine.py         # CRAG graph
│       ├── observer.py
│       └── prompts/rag_prompt.txt
├── tests/
│   ├── conftest.py
│   ├── unit/                       # hermetic, no infra
│   └── e2e/                        # needs docker-compose, LLM+Telegram mocked
├── scripts/
│   └── backfill_reingest.py        # NEW — §11 backfill
└── docs/
```

**Deleted files** (Phase 0): `patch.py`, `patch2.py`, `patch_format.py`, `fix_brief.py`, `create_test_user.py`, `daily_brief_+1234567890.json`, root `outputs/` artifacts, `start_services.sh` (replaced §10), `processor/daily_brief.py`, `processor/prompts/map_prompt.txt`, `processor/prompts/reduce_prompt.txt` (content reused in new prompt), all Twilio code paths, `ingestion/prompts/triage_system_prompt.txt` v1 (replaced by v2 below).

---

## 5. Phase plan

Execute phases in order. Each phase = one commit (or a few logical commits). **Run `pytest tests/unit` after every phase; run the Phase 9 e2e before declaring done.** Never edit files via generated patch scripts — direct edits only.

---

### Phase 0 — Repo hygiene (blocking everything)

1. Current working tree has ~10 modified files + untracked migration from the abandoned "chatbot-integration" iteration. **Commit the current tree as-is** to a branch `wip-snapshot` (safety), then on `main` create branch `overhaul/v1` where all following work lands.
2. Delete the files listed in §4 "Deleted files". Delete the untracked migration `alembic/versions/786187746bcc_*.py` (superseded by Phase 2's migration).
3. Remove from `requirements.txt`/deps: `groq`, `prefect`, `twilio`, `ollama` (unused directly — Taut talks to Ollama), `opentelemetry-api` (currently a no-op; re-add properly later per §12).
4. Add `.gitignore` entries: `outputs/`, `*.db`, already-present data dirs.

**Acceptance:** `git status` clean on `overhaul/v1`; `grep -r "patch\|CACHE BUST\|twilio" newsagg/` (post-Phase-1 layout) returns nothing.

---

### Phase 1 — Packaging & core modules

#### 1a. `pyproject.toml`

```toml
[project]
name = "newsagg"
version = "1.0.0"
requires-python = ">=3.11"
dependencies = [
    "aiokafka==0.12.0",
    "chromadb==0.6.3",
    "fastapi==0.115.8",
    "uvicorn[standard]==0.34.0",
    "openai==1.61.0",
    "google-genai==1.2.0",
    "sentence-transformers==3.3.1",
    "pydantic==2.10.6",
    "python-dotenv==1.0.1",
    "httpx==0.28.1",
    "feedparser==6.0.11",
    "beautifulsoup4==4.12.3",
    "langgraph==0.2.60",
    "langchain-core==0.3.29",
    "sqlalchemy==2.0.36",
    "psycopg2-binary==2.9.10",
    "alembic==1.14.0",
    "duckduckgo-search==5.3.0",
]

[project.optional-dependencies]
test = ["pytest", "pytest-asyncio", "pytest-mock", "respx"]

[project.scripts]
newsagg-producer  = "newsagg.ingestion.producer:main"
newsagg-triage    = "newsagg.ingestion.triage:main"
newsagg-storage   = "newsagg.storage.consumer:main"
newsagg-bot       = "newsagg.bot.poller:main"
newsagg-scheduler = "newsagg.scheduler.main:main"
newsagg-api       = "newsagg.api.main:main"        # wraps uvicorn.run

[tool.setuptools.packages.find]
include = ["newsagg*"]
```

Pin `langgraph`/`langchain-core`/`sqlalchemy`/`alembic` to whatever versions are currently installed in `.venv` (check with `pip show`) if different from above — the point is pinning, not these exact numbers.

#### 1b. Move files per §4 layout. Convert every import to absolute (`from newsagg.core.llm import llm_client`). Delete every `sys.path.append`. Install editable: `pip install -e ".[test]"`.

#### 1c. `newsagg/core/taxonomy.py` (ADR-4)

```python
from dataclasses import dataclass

@dataclass(frozen=True)
class Topic:
    slug: str      # stable id: used in DB, Chroma metadata key suffix, callback data
    label: str     # shown to users
    emoji: str

TAXONOMY: list[Topic] = [
    Topic("ai",          "AI & ML",             "🤖"),
    Topic("cloud",       "Cloud & Infra",       "☁️"),
    Topic("security",    "Security",            "🔐"),
    Topic("startups",    "Startups & VC",       "🚀"),
    Topic("programming", "Programming",         "💻"),
    Topic("distsys",     "Distributed Systems", "🕸️"),
    Topic("databases",   "Databases",           "🗄️"),
    Topic("business",    "Business & Markets",  "📈"),
    Topic("science",     "Science",             "🔬"),
    Topic("sports",      "Sports",              "🏟️"),
    Topic("top",         "Top News",            "🌍"),  # pseudo-topic: importance >= 8, any category
]

SLUGS = {t.slug for t in TAXONOMY}
BY_SLUG = {t.slug: t for t in TAXONOMY}
# Slugs the triage LLM may assign (everything except the pseudo-topic):
CLASSIFIABLE = [t for t in TAXONOMY if t.slug != "top"]

def chroma_key(slug: str) -> str:
    return f"topic_{slug}"
```

Note taxonomy now covers the sports/business/science feeds in `config.RSS_FEEDS` — the current tech-only taxonomy silently discards them.

#### 1d. `newsagg/core/llm.py` (ADR-3) — the ONLY module that constructs LLM clients

```python
"""Single LLM gateway. Every LLM call in the codebase goes through complete()."""
import json, asyncio, logging
import httpx, openai
from pydantic import BaseModel
from newsagg import config

logger = logging.getLogger(__name__)

_taut = openai.AsyncOpenAI(base_url=config.TAUT_URL, api_key="taut-local")
_gemini = openai.AsyncOpenAI(
    base_url="https://generativelanguage.googleapis.com/v1beta/openai/",
    api_key=config.GEMINI_API_KEY,
) if config.GEMINI_API_KEY else None

TIER_MODELS = {"simple": "ollama/llama3.1", "standard": "gemini/gemini-2.5-flash",
               "complex": "gemini/gemini-2.5-flash"}
_FALLBACK_MODEL = "gemini-2.5-flash"

async def complete(*, tier: str, system: str, user: str,
                   response_model: type[BaseModel] | None = None,
                   namespace: str = "system", context: str = "",
                   stream: bool = False, max_retries: int = 3):
    """Returns str, a parsed response_model instance, or an async chunk iterator (stream=True)."""
    messages = [{"role": "system", "content": system}, {"role": "user", "content": user}]
    headers = {"X-Taut-Namespace": namespace, "X-Taut-System": "NewsAggregator",
               "X-Taut-Context": context, "X-Taut-Tier": tier}
    kwargs = {}
    if response_model is not None:
        kwargs["response_format"] = {"type": "json_object"}
        messages[0]["content"] += ("\nRespond with JSON matching this schema exactly:\n"
                                   + json.dumps(response_model.model_json_schema()))
    last_err = None
    for attempt in range(max_retries):
        for client, model in ((_taut, TIER_MODELS[tier]), (_gemini, _FALLBACK_MODEL)):
            if client is None:
                continue
            try:
                resp = await client.chat.completions.create(
                    model=model, messages=messages, stream=stream,
                    extra_headers=headers if client is _taut else {}, **kwargs)
                if stream:
                    return resp
                text = resp.choices[0].message.content
                return response_model.model_validate_json(text) if response_model else text
            except Exception as e:           # noqa: BLE001 — gateway boundary
                last_err = e
                logger.warning("LLM call failed (%s, attempt %d): %s", model, attempt, e)
        await asyncio.sleep(2 ** attempt)
    raise RuntimeError(f"All LLM routes failed after {max_retries} attempts") from last_err
```

Rules for the implementer: **no other file may import `openai` or construct an LLM client.** `grep -rn "AsyncOpenAI\|import openai" newsagg/ | grep -v core/llm.py` must return nothing at the end of every phase.

#### 1e. `newsagg/core/embeddings.py` (ADR-8)

```python
from functools import lru_cache
from sentence_transformers import SentenceTransformer
from newsagg import config

@lru_cache(maxsize=1)
def get_model() -> SentenceTransformer:
    return SentenceTransformer(config.EMBEDDING_MODEL)   # "all-MiniLM-L6-v2", dim 384

def embed(texts: list[str]) -> list[list[float]]:
    return get_model().encode(texts, normalize_embeddings=True).tolist()

EXPECTED_DIM = 384
```

Lazy via `lru_cache` — nothing heavy at import time (fixes pytest-collection model downloads).

#### 1f. `newsagg/config.py`

Move from root. Keep RSS_FEEDS, brokers, Chroma, `TAUT_URL`, `DATABASE_URL`, `EMBEDDING_MODEL`, `TELEGRAM_BOT_TOKEN`, `GEMINI_API_KEY`, `CHROMA_RETENTION_DAYS`. **Delete:** `LLM_PROVIDER` branching, `RATE_DELAY_SECONDS`/backoff constants (retry lives in `core/llm.py`; Taut does rate limiting), `MESSAGING_PROVIDER`, all `TWILIO_*`, `OLLAMA_*`. Add: `TELEGRAM_POLL_TIMEOUT=50`, `BRIEF_LOOKBACK_HOURS=24`, `TOPIC_MODULE_MAX_ARTICLES=5`.

Final `.env.example`:

```
GEMINI_API_KEY=
TELEGRAM_BOT_TOKEN=          # from @BotFather — the ONE product bot
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/news_aggregator
REDPANDA_BROKER=localhost:9092
CHROMA_SERVER_HOST=localhost
CHROMA_SERVER_PORT=8002
CHROMA_RETENTION_DAYS=7
TAUT_URL=http://localhost:8000/v1
```

**Acceptance Phase 1:** `pip install -e ".[test]"` succeeds in a fresh venv with no sibling `../taut` checkout; `python -c "import newsagg.api.main, newsagg.processor.brief_engine"` works (stubs OK at this point); the `grep` rule in 1d passes.

---

### Phase 2 — Database schema (final)

`newsagg/db/schema.py`:

```python
class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    telegram_chat_id = Column(String, unique=True, nullable=False)  # sole identity in v1
    first_name = Column(String, nullable=True)          # from Telegram, for greeting
    timezone = Column(String, default="UTC")            # IANA name; v1 stores, doesn't convert
    delivery_cadence = Column(String, default="daily")  # 'daily' | 'weekly' | 'paused'
    delivery_hour_utc = Column(Integer, default=7)      # 0-23
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    interests = relationship("Interest", back_populates="user", cascade="all, delete-orphan")

class Interest(Base):
    __tablename__ = "interests"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    topic = Column(String, nullable=False)              # taxonomy slug — enforce in code
    source = Column(String, nullable=False, default="explicit")  # 'explicit' | 'implicit'
    engagement_score = Column(Float, default=1.0)
    last_interacted_at = Column(DateTime(timezone=True), server_default=func.now())
    __table_args__ = (UniqueConstraint("user_id", "topic", name="uq_interest_user_topic"),)

class TopicModule(Base):
    __tablename__ = "topic_modules"
    id = Column(Integer, primary_key=True)
    topic = Column(String, nullable=False)              # taxonomy slug
    module_date = Column(Date, nullable=False)          # UTC date
    content = Column(JSON, nullable=False)              # TopicModuleContent JSON (§Phase 6)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    __table_args__ = (UniqueConstraint("topic", "module_date", name="uq_module_topic_date"),)

class Brief(Base):
    __tablename__ = "briefs"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    brief_date = Column(Date, nullable=False)
    content = Column(JSON, nullable=False)              # assembled brief (topics + text)
    delivered_at = Column(DateTime(timezone=True), nullable=True)
    __table_args__ = (UniqueConstraint("user_id", "brief_date", name="uq_brief_user_date"),)
```

Notes:
- `phone_number`, `telegram_bot_token`, `is_premium` are **gone** (ADR-1/2; `is_premium` returns with monetization, not before).
- Migration: single new Alembic revision from current committed head (`cebd6dce1828`). Name every constraint (no `op.create_unique_constraint(None, ...)`). Since prod data is throwaway dev data, the migration may drop and recreate `users`/`interests`.
- `alembic/env.py`: import `newsagg.db.schema` metadata.

**Acceptance:** `alembic upgrade head` on a fresh Postgres container succeeds; `alembic downgrade -1` succeeds; no `create_all` anywhere.

---

### Phase 3 — Telegram bot: seamless signup + interest picker (the product core)

#### 3a. `newsagg/bot/telegram_api.py` — thin async wrapper

```python
class TelegramAPI:
    def __init__(self, token: str):
        self.base = f"https://api.telegram.org/bot{token}"
        self.client = httpx.AsyncClient(timeout=60)

    async def get_updates(self, offset: int) -> list[dict]:
        r = await self.client.get(f"{self.base}/getUpdates",
                                  params={"offset": offset, "timeout": config.TELEGRAM_POLL_TIMEOUT,
                                          "allowed_updates": '["message","callback_query"]'})
        r.raise_for_status()
        return r.json()["result"]

    async def send_message(self, chat_id: str, text: str,
                           reply_markup: dict | None = None) -> dict: ...
        # POST /sendMessage {"chat_id":…, "text":…, "parse_mode":"HTML",
        #                    "reply_markup": reply_markup, "disable_web_page_preview": true}
    async def edit_reply_markup(self, chat_id: str, message_id: int, reply_markup: dict): ...
    async def answer_callback(self, callback_query_id: str, text: str = ""): ...
```

Every dynamic string inserted into message text passes through `html.escape()` first (ADR-9). Allowed tags: `<b>`, `<i>`, `<a href>`.

#### 3b. `newsagg/bot/handlers.py` — transport-agnostic logic

Callback-data protocol (Telegram limits callback_data to 64 bytes — slugs keep it tiny):

| callback_data | Meaning |
|---|---|
| `t:<slug>` | toggle interest `<slug>` |
| `t:done` | close picker, confirm |
| `c:daily` / `c:weekly` / `c:paused` | set cadence |
| `h:<0-23>` | set delivery hour (UTC) |

Handlers:

- **`handle_update(update: dict)`** — dispatcher: `message.text` starting with `/` → command; other text → `handle_free_text`; `callback_query` → `handle_callback`.
- **`/start`** → `get_or_create_user(chat_id, first_name)` (writes `telegram_chat_id` — THE fix for the delivery gap). Reply: welcome text + interest keyboard. New users get **no** default interest — the picker is the onboarding.
- **`/topics`** → interest keyboard reflecting current selections.
- **`/schedule`** → cadence row (`Daily/Weekly/Pause`) + hour grid (buttons `00`–`23`, callback `h:<n>`, current hour marked ✅). Caption states times are UTC.
- **`/brief`** → latest `Brief` row for user; if none: "No brief yet — you'll get your first one at HH:00 UTC." If user has no interests: prompt `/topics`.
- **`/help`** → command list.
- **`handle_callback`**: for `t:<slug>` — insert or delete `Interest(source='explicit', engagement_score=1.0)`; then `edit_reply_markup` re-rendering the keyboard and `answer_callback("Added ✅"/"Removed")`. For `t:done` — if ≥1 interest: confirm "You're set! First brief at HH:00 UTC. Change anytime with /topics /schedule"; else keep picker open with a nudge. For `c:*`/`h:*` — update user row, `answer_callback` confirmation.
- **`handle_free_text`** → fire-and-forget `observe_conversation(user_id, text)`; run CRAG `query_news_rag(text, str(chat_id))`; send answer. Wrap in try/except with friendly failure message.

Interest keyboard builder (2 columns, ✅ prefix when selected):

```python
def interest_keyboard(selected: set[str]) -> dict:
    rows, row = [], []
    for t in TAXONOMY:
        mark = "✅ " if t.slug in selected else ""
        row.append({"text": f"{mark}{t.emoji} {t.label}", "callback_data": f"t:{t.slug}"})
        if len(row) == 2:
            rows.append(row); row = []
    if row: rows.append(row)
    rows.append([{"text": "Done ✔️", "callback_data": "t:done"}])
    return {"inline_keyboard": rows}
```

#### 3c. `newsagg/bot/poller.py`

```python
async def run():
    api = TelegramAPI(config.TELEGRAM_BOT_TOKEN)
    offset = 0
    while True:
        try:
            for upd in await api.get_updates(offset):
                offset = upd["update_id"] + 1
                try:
                    await handlers.handle_update(api, upd)
                except Exception:
                    logger.exception("update %s failed", upd["update_id"])  # never kill the loop
        except (httpx.HTTPError, asyncio.TimeoutError):
            await asyncio.sleep(3)
```

#### 3d. `api/main.py` webhook mode: `/webhook/telegram` becomes `await handlers.handle_update(api, await request.json()); return {"ok": True}` — same handlers, different transport. Delete the old inline user-creation/RAG code and the Twilio endpoint.

**Acceptance Phase 3 (manual smoke, real bot token):** fresh chat → `/start` → picker appears → tap 2 topics → checkmarks update in place → Done → `/schedule` → pick hour → user row in Postgres has correct `telegram_chat_id`, 2 explicit interests, chosen hour. Free text returns a RAG answer (or graceful failure if Chroma empty).

---

### Phase 4 — Ingestion fixes

`newsagg/ingestion/triage.py` (from `consumer_triage.py`):

1. `TriageOutput` (Pydantic): `relevant: bool`, `reasoning: str`, `topics: list[str]` (validator: keep only slugs in `taxonomy.SLUGS`, lowercase/map labels→slugs), `entities: list[str]`, `importance_score: int` (ge=1, le=10), `key_insights: list[str]`.
2. Triage call → `core.llm.complete(tier="simple", response_model=TriageOutput, context="Triage", ...)`. Delete the module-local OpenAI client and XML compressor remnants.
3. **Plumb `importance_score` and `key_insights` into `ArticleVerified`** (currently discarded — the bug that flattens everything to 5).
4. Prompt v2 (`newsagg/ingestion/prompts/triage_system_prompt.txt`): update the topic instruction to enumerate slugs: `Classify into one or more of: ai, cloud, security, startups, programming, distsys, databases, business, science, sports.` Broaden the relevance definition beyond tech ("high-quality news in any of the listed categories; spam/marketing/noise is irrelevant") since feeds now include sports/business/science. Update the few-shot examples' `topics` values to slugs. Bump version header to 2.0.0.
5. Consumer loop (ADR-11): `enable_auto_commit=False`; process batch via `getmany(max_records=50)` + `Semaphore(10)` + `gather`; after ALL messages in the batch resolve (success, or published to `triage-dlq` after `MAX_RETRIES`), `await consumer.commit()`. DLQ message = original raw JSON + `{"error": str(e), "stage": "triage"}`.
6. `main()` entry point for the console script.

`newsagg/ingestion/producer.py`: keep; add the boilerplate scrubber — after BeautifulSoup strip, drop sentences matching `r"(?i)(subscribe|sign up|newsletter|click here|read more|share (this|on)|follow us)"`.

**Acceptance:** unit tests — TriageOutput validator maps `"Databases"`→`databases` and drops unknown topics; a triage failure lands the message on `triage-dlq` (mock Kafka producer) and the offset still commits.

---

### Phase 5 — Storage fixes

`newsagg/storage/vector_store.py`:

1. Parent metadata (final shape):

```python
meta = {
    "type": "parent",
    "title": article.title, "url": url, "source": article.source,
    "published": article.published, "published_int": published_int,
    "importance_score": article.importance_score,          # real value now
    "key_insights": " | ".join(article.key_insights),
    "topics": ",".join(article.topics),                    # display only
    **{chroma_key(t.slug): (t.slug in article.topics) for t in CLASSIFIABLE},  # ADR-4 booleans
}
```

2. Children inherit `published_int` + the topic booleans (so child-level filtered search stays possible).
3. **Parents embedded explicitly** via `core.embeddings.embed([title + " " + summary])` (ADR-8). Semantic-dedup query switches from `query_texts` to `query_embeddings` with the same embedder. Keep dedup threshold 0.15 and md5(url) ids + `upsert`.
4. Filter helper (used by brief engine; exported here):

```python
def topic_filter(slugs: list[str]) -> dict:
    ors = [{chroma_key(s): {"$eq": True}} for s in slugs if s in SLUGS and s != "top"]
    if not ors:
        return {"type": {"$eq": "parent"}}
    clause = ors[0] if len(ors) == 1 else {"$or": ors}
    return {"$and": [{"type": {"$eq": "parent"}}, clause]}
```

5. Consumer (`storage/consumer.py`): manual commit after batch; storage failures → `storage-dlq`.

**Acceptance:** unit test builds `topic_filter(["ai","databases"])` and asserts exact dict; integration (against docker Chroma): store 2 articles with different topics, `collection.get(where=topic_filter(["ai"]))` returns only the AI one.

---

### Phase 6 — Brief engine + scheduler (replaces `daily_brief.py`, Prefect, cron)

#### 6a. `newsagg/processor/brief_engine.py`

```python
class ModuleItem(BaseModel):
    title: str; url: str; summary_line: str          # ≤ 25 words, plain text
class TopicModuleContent(BaseModel):
    topic: str; headline: str                        # ≤ 12 words
    items: list[ModuleItem]                          # 1-5
```

- `active_interests(user, now) -> list[str]`: explicit → always included (ADR-13). Implicit → `engagement_score * 0.5 ** ((now - last_interacted_at).days / 14)`, include if ≥ 0.2.
- `fetch_topic_articles(slug, now) -> list[dict]`: Chroma `collection.get(where=...)` with `topic_filter([slug])` AND `published_int >= int((now - timedelta(hours=BRIEF_LOOKBACK_HOURS)).strftime("%Y%m%d"))`; for `"top"`: filter is `type=parent AND importance_score >= 8` + date. Sort by `importance_score` desc, take `TOPIC_MODULE_MAX_ARTICLES`.
- `build_topic_module(slug, date) -> TopicModuleContent | None`: return cached `TopicModule` row if exists (idempotent). Else fetch articles; if none → None. Else ONE call: `core.llm.complete(tier="standard", response_model=TopicModuleContent, context="TopicModule", system=<topic_module_prompt>, user=<articles: title/url/key_insights/summary per article>)`. Persist row. Prompt (`topic_module_prompt.txt`, v1): "You are a sharp newsletter editor. Given articles on {topic}, produce `headline` (≤12 words, plain text, no markdown) and per-article `summary_line` (≤25 words, plain text). Use ONLY given articles. Copy `title`/`url` verbatim." (Reuse the Morning-Brew tone lines from the retired `reduce_prompt.txt`, keep its NO-MARKDOWN constraint.)
- `assemble_brief(user, modules) -> (content_json, html_text)`: zero LLM. HTML format (all dynamic parts `html.escape`d):

```
☕ Good morning{, FirstName}! Your brief for {Mon, Jul 20}:

<b>{emoji} {Topic label}</b> — {headline}
 • <a href="{url}">{title}</a>
   {summary_line}
 ...

Change topics: /topics · Schedule: /schedule
```

If every module is None → send "Quiet day in your topics — nothing major in the last 24h." (still record a Brief row so we don't respam).
- `deliver(user, html_text)`: `TelegramAPI.send_message`; on success set `Brief.delivered_at`.
- `run_hour(now)`: users where cadence=='daily' and `delivery_hour_utc == now.hour`, plus cadence=='weekly' and weekday==0 (Mon) and hour matches; skip users with an existing Brief row for today (idempotent re-runs); union their active interest slugs → `build_topic_module` per slug → assemble+deliver per user.

#### 6b. `newsagg/scheduler/main.py`

```python
async def run():
    while True:
        now = datetime.now(timezone.utc)
        if now.minute == 0:
            await brief_engine.run_hour(now)          # try/except + log, never die
            if now.hour == 3:
                cleanup.prune_expired()               # chroma retention
        await asyncio.sleep(60 - now.second)
```

`/brief` FastAPI endpoint → `GET /brief/{chat_id}` reads latest `Brief` row (ADR-7). Root `/brief` deleted.

**Acceptance:** unit — `run_hour` with 3 fake users (due/not-due/paused) calls deliver only for the due one; module built once per topic even when 2 users share it (assert 1 LLM-mock call); brief HTML contains escaped title and both topic sections; second `run_hour` same hour sends nothing (idempotent).

---

### Phase 7 — RAG cleanup (`api/query_engine.py`)

1. Delete: the hardcoded "agentic" fake web result; the `"(CACHE BUST 3)"` suffix; the `if GEMINI_API_KEY` direct-Gemini fork; the module-local clients and `SentenceTransformer` load (use `core.embeddings`).
2. All nodes (router, translate, evaluator, generate) call `core.llm.complete` — tiers: router/translate/evaluator `simple`, generate `complex`; `namespace=str(chat_id)`, `stream=True` for generate.
3. Translate + evaluator get `response_model=` schemas (fixes free-form parsing fragility flagged in the Jul-6 review; the schema is auto-injected by `core/llm.py`).
4. Retrieval: vector similarity + `published_int` range + `type` filter only (ADR-12 — remove any interest-based filtering remnants).
5. Keep: LangGraph CRAG shape, MemorySaver keyed by chat id, DDGS fallback, 1-retry cap.

`api/observer.py`: extraction schema constrained to taxonomy — `topic: Literal[<slugs>]` (build the Literal from `taxonomy.SLUGS`); confidence ≥ 0.9; writes `source='implicit'`, `engagement_score=confidence`; existing topic (explicit or implicit) → refresh `last_interacted_at` and bump `engagement_score = min(1.0, score + 0.1)`. Keep negative-sentiment guard from current prompt.

**Acceptance:** `grep -rn "CACHE BUST\|example.com\|agentic" newsagg/` → nothing. Unit: observer maps "kubernetes stuff" → `cloud` (mock LLM), rejects unknown slug via validation.

---

### Phase 8 — docker-compose & services

1. Taut service: default `TAUT_ROUTING_TIERS` uses `ollama/llama3.1` for simple, `gemini/gemini-2.5-flash` for standard+complex (match `core/llm.py TIER_MODELS`); add healthcheck `curl -f http://localhost:8000/health || exit 1` (adjust path to Taut's real health endpoint) and `depends_on: condition: service_healthy` for nothing (app runs on host) but document readiness.
2. Keep redpanda, chroma (8002→8000), postgres as-is.
3. Replace `start_services.sh` with `scripts/dev.sh`:

```bash
#!/usr/bin/env bash
set -euo pipefail
docker compose up -d
alembic upgrade head
trap 'kill 0' EXIT
newsagg-api & newsagg-triage & newsagg-storage & newsagg-bot & newsagg-scheduler &
wait
```

**Acceptance:** `docker compose up -d && ./scripts/dev.sh` starts everything on a machine with only this repo + `.env` (bot token + Gemini key). No sibling checkouts.

---

### Phase 9 — Tests (the phase that ends the failed-iteration loop)

Delete `tests/test_integration_e2e.py` (false positives). New structure:

**`tests/unit/`** (no infra, all external boundaries mocked):
- `test_taxonomy.py` — slugs unique, `chroma_key`, keyboard builder shape.
- `test_triage.py` — TriageOutput validation/mapping; DLQ path.
- `test_topic_filter.py` — exact where-clause dicts.
- `test_decay.py` — explicit never decays; implicit at 14/28 days; 0.2 cutoff.
- `test_brief_assembly.py` — HTML output, escaping (`<script>` in a title arrives escaped), empty-day path.
- `test_scheduler_due.py` — due-user selection matrix (cadence × hour × weekday × already-delivered).
- `test_handlers.py` — `/start` creates user with `telegram_chat_id`; `t:ai` toggles Interest; `t:done` with zero interests keeps picker; uses a `FakeTelegramAPI` recording calls; DB = sqlite in-memory session fixture.
- `test_llm_gateway.py` — respx: Taut 500 → falls back to Gemini route; schema injection happens; retries then raises.

**`tests/e2e/test_pipeline.py`** (marker `@pytest.mark.e2e`, requires docker-compose; LLM + Telegram mocked, infra real):
1. Seed user via `handlers.handle_update` fake `/start` + `t:ai` + `t:done` (real Postgres).
2. Publish one `ArticleRaw` (AI-topic fixture) to `raw-articles` (real Redpanda).
3. Run one triage batch with `core.llm.complete` patched → canned `TriageOutput(topics=["ai"], importance_score=9, ...)`.
4. Run one storage batch (real Chroma, real embeddings).
5. `run_hour(fake_now_at_user_hour)` with module LLM patched → canned `TopicModuleContent`; Telegram `sendMessage` mocked via respx.
6. **Assert:** respx captured exactly one sendMessage to the seeded chat_id whose text contains the fixture article title and url; `Brief` row exists with `delivered_at` set; re-running step 5 sends nothing.

This single test would have caught every shipped regression to date (identity mismatch, dead endpoint, empty briefs, discarded importance).

**Acceptance:** `pytest tests/unit` green with no network and no docker. `pytest -m e2e` green with compose up. Both documented in README.

---

### Phase 10 — Docs & cleanup

1. Rewrite `README.md`: quickstart (clone → `.env` → `docker compose up -d` → `pip install -e .` → `alembic upgrade head` → `./scripts/dev.sh` → message the bot), architecture diagram (§3), command list, test instructions. Fix/remove broken doc links (`PRODUCT_BRIEF.md`, `DEVELOPMENT_GUIDE.md` don't exist).
2. Add `docs/ROADMAP.md` seeded from §12 with checkboxes — future proposals live IN the repo, not in agent brain folders.
3. Merge `overhaul/v1` → `main` via PR.

---

## 6. Data migration / backfill (§11 referenced above)

Dev data is disposable, but the vector store must match the new metadata shape. `scripts/backfill_reingest.py`:
1. Delete Chroma collection `news_archive` (metadata shape changed: taxonomy booleans, explicit parent embeddings, real importance).
2. Re-run producer once, let triage+storage consumers drain (they now write v2 metadata).
Document in README: "after upgrading, run the backfill".

---

## 7. Execution guardrails for the implementing agent

1. **Never** generate/apply string-replace patch scripts. Direct file edits only.
2. One phase = one commit minimum; commit messages `overhaul(phaseN): <what>`.
3. After each phase: `pytest tests/unit` + the phase's acceptance checks. A phase is not done until they pass — do not proceed on red.
4. `grep` gates that must stay clean from Phase 7 on: `CACHE BUST`, `example.com`, `twilio`, `import taut`, `sys.path.append`, `AsyncOpenAI` outside `core/llm.py`, `parse_mode.*Markdown`.
5. If something in this plan contradicts reality (an API changed, a version conflict), fix forward within the phase and note the deviation in the commit body — do not silently skip.
6. Do not add features not in this plan (no dashboards, no WhatsApp, no AWS).

## 8. Definition of done (whole overhaul)

- Fresh machine, only this repo + `.env` with `TELEGRAM_BOT_TOKEN` + `GEMINI_API_KEY`: `docker compose up -d`, `pip install -e .`, `alembic upgrade head`, `./scripts/dev.sh`.
- A brand-new Telegram user: `/start` → picks topics from buttons → picks hour → at that hour receives an HTML brief containing real ingested articles for their topics. **No operator edits to `.env` or DB.**
- `/brief`, `/topics`, `/schedule`, free-text Q&A all work in-chat.
- `pytest tests/unit` and `pytest -m e2e` green.
- All grep gates (§7.4) clean.

---

## 12. Future hooks (explicitly out of scope — designed-for, not built)

| Feature | Hook point already in place after this plan |
|---|---|
| Real-time alerts (premium) | `storage/consumer.py` after upsert: if `importance_score >= 8`, look up users subscribed to the article's topic slugs with cadence `real-time`; new cadence value + delivery call. |
| Web dashboard (Jul-11 proposal) | Deep-link binding `tg://resolve?domain=<bot>&start=<token>` — add a `/start <payload>` branch in `handlers.py`; JWT + preferences API over existing tables. |
| Second channel (WhatsApp) | Implement the same `deliver()` signature in a new `bot/whatsapp_api.py`; add channel column to `users`. |
| Local timezone scheduling | `users.timezone` already stored; convert in `run_hour` due-check instead of comparing UTC hour. |
| Observability | Wrap `core/llm.complete` (single choke point) with OTel spans + token counters; add `-sdk` dependency. |
| AWS migration | Deferred until local v1 runs clean for weeks. See `~/.gemini` brain doc `3eeea4e6…/production_migration_plan.md`. |
