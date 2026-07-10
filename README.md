# Personalized AI News Aggregator & Analyst (Local-First MVP)

A local-first, event-driven news pipeline that ingests technical RSS feeds, filters articles using high-speed LLM triage, processes and indexes them into a hierarchical parent-child ChromaDB vector store, consolidates daily briefs using local Map-Reduce summarization, and exposes them via a conversational RAG API.

---

## 📚 Documentation Index

To keep this repository clean and organized, all detailed documentation has been consolidated:

**Core System Guides:**
- **[PRODUCT_BRIEF.md](PRODUCT_BRIEF.md)**: Product vision, feature requirements, and client specifications.
- **[ARCHITECTURE.md](ARCHITECTURE.md)**: Architecture, Mermaid diagrams, deployment strategies, and Taut SDK requirements.
- **[DEVELOPMENT_GUIDE.md](DEVELOPMENT_GUIDE.md)**: Guidelines for contributing and working with the codebase.
- **[TWILIO_GUIDE.md](TWILIO_GUIDE.md)**: Webhook tunneling, and pricing for the WhatsApp integration.
- **[TELEGRAM_GUIDE.md](TELEGRAM_GUIDE.md)**: Webhook configuration for the 100% free Telegram bot integration.

**In-Depth Technical Research (`docs/` folder):**
- **[Rag And Query Translation](docs/rag_and_query_translation.md)**: CRAG framework and routing details.
- **[Hybrid Search Metadata](docs/hybrid_search_metadata.md)**: ChromaDB vector metadata design.
- **[Map Reduce Summarization](docs/map_reduce_summarization.md)**: Async LLM reduce strategies.
- **[Data Contracts](docs/data_contracts.md)** & **[Structured Outputs](docs/structured_outputs.md)**: Pydantic schemas.
- **[Prompt Engineering](docs/prompt_engineering.md)** & **[Rag Best Practices](docs/rag_best_practices.md)**.
- **[Vector Databases](docs/vector_databases.md)** & **[Scaling Vector DBs](docs/scaling_vector_dbs.md)**.

---

## Technical Stack & Infrastructure

This architecture runs locally inside a Docker Compose cluster using **Taut** as a stateless LLM proxy middleware.

| Service | Port | Local Endpoint | Purpose in the System |
| :--- | :--- | :--- | :--- |
| **Redpanda** | `9092` / `29092` | `localhost:9092` | Kafka-compatible event stream broker buffering data flows. |
| **Redpanda Console** | `8080` | [http://localhost:8080](http://localhost:8080) | Web-based GUI to inspect topics, offsets, and payloads. |
| **ChromaDB** | `8002` | [http://localhost:8002](http://localhost:8002) | Vector database storing semantic text embeddings and parent-child metadata. |
| **Taut Proxy** | `8000` | [http://localhost:8000](http://localhost:8000) | AI Efficiency Middleware intercepting, caching, and routing LLM requests. |
| **Ollama** | `11434` | [http://localhost:11434](http://localhost:11434) | Local runtime hosting offline LLM models (e.g., Llama-3). |
| **FastAPI Server** | `8050` | [http://localhost:8050](http://localhost:8050) | The API gateway serving RAG queries and pre-compiled briefs. |

---

## 🚀 Quick Start Guide

This architecture is **Event-Driven**. Because of this, the system is split into two types of programs: **Always-On Daemons** and **Scheduled Batch Scripts**.

**Prerequisites:** 
1. `docker` and `docker-compose` installed.
2. A populated `.env` file (copy `.env.example`).
3. Virtual environment activated: `source .venv/bin/activate`.
4. Run `docker-compose up -d` and initialize the database: `alembic upgrade head`.

### 1. Always-On Daemons (Leave these running!)
These scripts must run 24/7. They maintain persistent network connections to listen for live events (either from Redpanda or from Telegram). A cron job cannot run these because they are designed to never exit.

Open 3 separate terminal tabs and leave these running:

**Tab 1 (Triage Agent):** Continuously listens to the Redpanda broker. As soon as a raw article hits the queue, it evaluates it using Ollama.
```bash
python ingestion/consumer_triage.py
```
**Tab 2 (Storage Agent):** Continuously listens to the Redpanda broker. As soon as a verified article is approved, it encodes it into ChromaDB.
```bash
python storage/consumer_storage.py
```
**Tab 3 (FastAPI & Webhooks):** Runs your ASGI web server on `http://localhost:8050`. It must be online 24/7 to receive and respond to incoming Telegram/WhatsApp messages from users.
```bash
python api/main.py
```

### 2. Scheduled Batch Scripts (Manual or Cron)
Unlike the daemons, these scripts are designed to wake up, perform a specific task, and then shut down. These are the scripts you put in a cron job.

**A. Ingesting the News (The RSS Scraper)**
You must explicitly trigger the RSS scraper to fetch new articles. In production, this should be a cron job running every 6 hours.
```bash
# Run this manually whenever you want to fetch the latest news:
python ingestion/producer.py
```

**B. Sending the Daily Briefs**
You must explicitly trigger the Map-Reduce pipeline that compiles the daily news and messages users. In production, this should be a cron job running every morning at 7:00 AM.
```bash
# Run this manually to generate and send out briefs to all registered users:
python processor/daily_brief.py
```

**Production Cron Setup Example (`crontab -e`):**
```bash
# Run RSS Ingestion every 6 hours
0 */6 * * * cd /path/to/news-aggregator && .venv/bin/python ingestion/producer.py

# Send Daily Briefs every morning at 7:00 AM
0 7 * * * cd /path/to/news-aggregator && .venv/bin/python processor/daily_brief.py
```

---

## Verification & API Commands

Once the server is running, verify it using `curl` from another terminal tab:

### 1. Conversational Query (Fact-Seeking RAG)
```bash
curl -X POST http://localhost:8050/query \
     -H "Content-Type: application/json" \
     -d '{"query": "What is Web Search on Bedrock AgentCore?"}'
```

### 2. Conversational Query (Relative Time Pre-Filtered)
```bash
curl -X POST http://localhost:8050/query \
     -H "Content-Type: application/json" \
     -d '{"query": "What updates happened yesterday?"}'
```

### 3. Fetch Consolidation Brief
```bash
curl http://localhost:8050/brief
```

---

## 🧠 The User Experience Lifecycle (How it Works)

The system is designed to require zero explicit configuration from the end user. It learns what they like entirely through conversation.

1. **The "Cold Start" (Initial Setup):**
   When a user sends their very first message to the bot on Telegram, the system automatically creates their profile in the PostgreSQL database. Because we don't know what they like yet, the system assigns a default interest of `"Top News"`. This ensures that even if they never ask a specific question, they will still receive a generalized Daily Brief the next morning.
   
2. **Refining Interests (The Observer Agent):**
   Every time the user asks a question (e.g., *"Did Apple release anything today?"* or *"Any news on PostgreSQL performance?"*), the message is processed by a background worker called the **Observer Agent**. This agent performs zero-shot entity extraction to quietly update their profile in the database, adding "Apple" or "PostgreSQL" to their explicit list of interests.

3. **Interest Decay (The Forgetting Algorithm):**
   Over time, users change jobs or lose interest in certain topics. Our database tracks an `engagement_score` and `last_interacted_at` timestamp for every topic. If a user stops asking about "React.js", the interest decays mathematically. Once it drops below a specific threshold (e.g., after a few weeks of inactivity), the system stops pulling articles about React.js for their personalized Daily Brief.
