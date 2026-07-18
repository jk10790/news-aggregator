# News Aggregator — Personalized Telegram News Bot

A local-first, event-driven news pipeline: RSS feeds are ingested, triaged by
an LLM into a fixed topic taxonomy, embedded into ChromaDB, and delivered as
a personalized daily/weekly brief to each user's Telegram chat at the hour
they choose — with zero operator involvement after `/start`. Users can also
just ask the bot questions and get a conversational, RAG-backed answer.

See `docs/OVERHAUL_PLAN.md` for the full design rationale (ADRs) behind the
current architecture.

---

## 1. Quickstart

```bash
git clone <this-repo>
cd news-aggregator

cp .env.example .env
# edit .env: at minimum set TELEGRAM_BOT_TOKEN (from @BotFather) and
# GEMINI_API_KEY (https://aistudio.google.com/apikey)

docker compose up -d          # Redpanda, ChromaDB, Postgres, Taut proxy

python -m venv .venv && source .venv/bin/activate
pip install -e ".[test]"

alembic upgrade head           # creates users/interests/topic_modules/briefs

./scripts/dev.sh                # runs migrations again (idempotent) + starts
                                 # api, triage, storage, bot, scheduler
```

Then, on a fresh RSS ingest (see §5), message your bot on Telegram:

```
/start          -> pick your topics from the inline keyboard, tap Done
/schedule       -> pick a delivery hour (all times UTC)
```

At the chosen hour, you'll receive an HTML-formatted brief built from real
ingested articles in your topics. No `.env`/DB edits required from an
operator at any point.

---

## 2. Services & ports

| Service | Port | Purpose |
| :--- | :--- | :--- |
| Redpanda | `9092` / `29092` | Kafka-compatible broker: `raw-articles` → `verified-articles`, plus `triage-dlq` / `storage-dlq`. |
| Redpanda Console | `8080` | Web UI to inspect topics/offsets ([http://localhost:8080](http://localhost:8080)). |
| ChromaDB | `8002` (container `8000`) | Vector store: parent/child chunks, taxonomy-boolean metadata. |
| Postgres | `5432` | `users`, `interests`, `topic_modules`, `briefs` (Alembic-managed). |
| Taut proxy | `8000` | OpenAI-compatible LLM gateway (`newsagg/core/llm.py`'s only client target); routes `simple` → local Ollama, `standard`/`complex` → Gemini, with a direct-Gemini fallback baked into the gateway if Taut is unreachable. |
| FastAPI (`newsagg-api`) | `8050` | `/query` (NDJSON RAG stream), `/brief/{chat_id}`, `/webhook/telegram` (deploy-time alternative to long-polling), `/health`. |

---

## 3. Architecture

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

Key design decisions (full rationale in `docs/OVERHAUL_PLAN.md` §2):

- **Telegram only** — one product bot, long-polling by default (no public
  URL/ngrok needed); the FastAPI webhook is a deploy-time alternative that
  calls the exact same handler functions.
- **One LLM gateway** (`newsagg/core/llm.py`) — every LLM call in the
  codebase goes through `complete()`. It's the only module allowed to
  construct an `openai.AsyncOpenAI` client, and the only place retry/tier
  routing/Taut→Gemini fallback logic lives.
- **Fixed topic taxonomy** (`newsagg/core/taxonomy.py`) — triage, storage,
  the interest picker, and the Observer all classify into the same 10
  slugs (`ai`, `cloud`, `security`, `startups`, `programming`, `distsys`,
  `databases`, `business`, `science`, `sports`) plus a pseudo-topic `top`
  (importance ≥ 8, any category). No free-form topic strings anywhere.
- **Topic-centric briefs** — one LLM call per active *topic* per day
  (cached in Postgres as `TopicModule`), not one per user. Per-user briefs
  are template-stitched from cached modules with zero additional LLM calls.
- **Explicit interests never decay; implicit ones do** — a topic the user
  tapped in `/topics` stays forever; a topic the Observer inferred from
  free text decays (`0.5 ** (days_since / 14)`) and drops out below 0.2.

---

## 4. Bot commands

| Command / input | Behavior |
| :--- | :--- |
| `/start` | Onboard (creates the user keyed on `telegram_chat_id`) and show the topic picker. Re-running it re-shows the picker. |
| `/topics` | Show the interest picker reflecting current selections; tap a topic to toggle it, tap Done to close. |
| `/schedule` | Pick delivery cadence (Daily / Weekly / Pause) and delivery hour (00–23, UTC). |
| `/brief` | Show your latest delivered brief, or a friendly "nothing yet" / "pick topics first" message. |
| `/help` | List commands. |
| Any other text | Routed through a LangGraph CRAG pipeline (vector search + web-search fallback) for a conversational answer; also silently updates your implicit interests via the Observer. |

---

## 5. Ingesting news

The producer isn't a long-running daemon (it's not one of the 5 services
`scripts/dev.sh` starts) — run it manually or on a cron to pull fresh RSS
articles into the pipeline:

```bash
newsagg-producer          # or: python -m newsagg.ingestion.producer
```

The triage and storage consumers (already running via `dev.sh`) will pick
the new articles up automatically and index them into ChromaDB with real
importance scores and taxonomy metadata.

### Backfilling after a metadata-shape change

If you're upgrading from a pre-overhaul checkout, old ChromaDB entries won't
have the current per-topic boolean metadata and will silently fail to match
any brief/topic filter. Re-ingest from a clean slate:

```bash
python scripts/backfill_reingest.py     # deletes the news_archive collection
newsagg-producer                        # re-run the producer once
# let the triage/storage consumers (already running) drain the new articles
```

---

## 6. Testing

```bash
pytest tests/unit                 # hermetic — no docker, no network, no LLM calls
docker compose up -d               # required for the e2e suite below
pytest tests/e2e -m e2e            # real Postgres/Redpanda/Chroma; LLM + Telegram mocked
```

`tests/unit/` covers taxonomy, the LLM gateway (respx-mocked Taut→Gemini
fallback + retries), triage validation/DLQ routing, the Chroma topic filter,
interest decay, brief HTML assembly, scheduler due-selection, and bot
handlers (fake Telegram API + in-memory sqlite).

`tests/e2e/test_pipeline.py` is the one test that would have caught every
shipped regression to date: it seeds a real user, publishes a real article
through real Redpanda, runs one real triage + storage batch (LLM canned),
then runs the real hourly scheduler and asserts a real Telegram
`sendMessage` was made containing that article's title and url, and that a
`Brief` row was recorded and marked delivered.

---

## 7. Repo layout

```
newsagg/
├── config.py            # env parsing only
├── core/                 # taxonomy, LLM gateway, embeddings, shared models
├── db/                   # SQLAlchemy schema + session factory
├── ingestion/             # RSS producer, triage consumer
├── storage/               # ChromaDB consumer, vector store, retention cleanup
├── processor/             # topic-module + brief assembly engine
├── bot/                   # Telegram API wrapper, handlers, long-poll loop
├── scheduler/              # 1-minute-tick asyncio scheduler
└── api/                    # FastAPI app, RAG query engine, observer

alembic/                  # migrations (Postgres only — no create_all)
scripts/dev.sh             # docker compose up + migrate + launch all services
scripts/backfill_reingest.py
tests/unit/                # hermetic
tests/e2e/                  # requires docker compose
docs/OVERHAUL_PLAN.md       # architecture ADRs + phase-by-phase build log
docs/ROADMAP.md              # explicitly out-of-scope-for-v1 future hooks
```
