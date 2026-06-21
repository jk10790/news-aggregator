# Personalized AI News Aggregator & Analyst (Local-First MVP)

A local-first, zero-cost pipeline that ingests RSS feeds, filters articles using high-speed LLM triage, semantically chunks and embeds relevant articles, and indexes them in a vector database for conversational RAG.

---

## Technical Stack & Infrastructure

This architecture runs locally inside a Docker Compose cluster.

| Service | Port | Local Endpoint | Purpose in the System |
| :--- | :--- | :--- | :--- |
| **Redpanda** | `9092` / `29092` | `localhost:9092` | Kafka-compatible event stream broker buffering data flows. |
| **Redpanda Console** | `8080` | [http://localhost:8080](http://localhost:8080) | Web-based GUI to inspect topics, offsets, and payloads. |
| **ChromaDB** | `8000` | [http://localhost:8000](http://localhost:8000) | Vector database storing semantic text embeddings and parent-child metadata. |
| **Ollama** | `11434` | [http://localhost:11434](http://localhost:11434) | Local runtime hosting offline LLM models (e.g., Llama-3). |

---

## Local Validation Commands

Use these check scripts to ensure your infrastructure containers are active:

### 1. General Docker Status
```bash
docker ps
```
*(Verify that `redpanda`, `redpanda-console`, and `chromadb` show up as `Up`).*

### 2. Verify ChromaDB Heartbeat
```bash
curl http://localhost:8000/api/v1/heartbeat
```
*(Expected response is a JSON object containing a nanosecond timestamp).*

### 3. Query ChromaDB Document Count
```bash
python -c "import chromadb; client = chromadb.HttpClient(host='localhost', port=8000); col = client.get_collection('news_archive'); print('Total items in ChromaDB:', col.count())"
```

---

## Run Procedures

All python runs should be executed with your virtual environment activated (`source .venv/bin/activate`).

### Phase 1: Ingestion & Triage Pipeline

1. **Ingest Feeds to Redpanda (`raw-articles`)**:
   ```bash
   python ingestion/producer.py
   ```
2. **Execute LLM Triage (Filter `raw-articles` -> `verified-articles`)**:
   ```bash
   python ingestion/consumer_triage.py
   ```

### Phase 2: Structural Enrichment & Storage

1. **Index Articles (Process `verified-articles` -> ChromaDB)**:
   ```bash
   python storage/consumer_storage.py
   ```
