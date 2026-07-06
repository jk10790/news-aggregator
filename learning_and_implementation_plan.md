# Learning & Implementation Master Plan
**Role:** Principal AI Engineer / Mentor
**Goal:** Transform the student into an elite Agentic RAG Architect through hands-on implementation of the Agentic Newsroom.

This plan intertwines deep theoretical understanding with practical, production-grade implementation. We do not copy-paste code; we build from first principles.

---

## Module 1: The Hexagonal Foundation & Cloud Agnosticism
*A true expert doesn't just build scripts; they design resilient systems. We start by decoupling our logic from our infrastructure.*

**📚 Concept to Learn:**
*   Ports and Adapters (Hexagonal Architecture).
*   Dependency Injection in Python.
*   Abstract Base Classes (`abc` module).

**🛠 Implementation Task:**
1. Scaffold the `/core` and `/infrastructure` directories.
2. Define the exact Python `Interfaces` (Abstract Base Classes) for `LLMProvider`, `VectorStoreProvider`, and `EventStreamProvider`.
3. Build the first concrete adapter: A dummy `MockLLMProvider` that returns static text, just to prove the interface works.

**🧠 Mentor's Challenge:** 
*Why is dependency injection critical for testing Agentic AI systems? (Hint: Think about token costs during unit tests).*

---

## Module 2: The Agentic State Machine (LangGraph Basics)
*Agents are not just "chatbots". They are state machines that route, loop, and act based on programmatic logic.*

**📚 Concept to Learn:**
*   Graphs, Nodes, and Edges in software architecture.
*   Managing "State" (Context) across an agent's lifecycle.
*   LangGraph vs. LangChain (Why imperative chains fail at complex reasoning).

**🛠 Implementation Task:**
1. Build a basic LangGraph workflow for the **Router Agent**.
2. Define a `State` dictionary that tracks `user_query`, `intent`, and `current_action`.
3. Create a conditional edge: If the LLM classifies the intent as "News", route to the `ResearchNode`. If "Account Update", route to the `AccountNode`.

**🧠 Mentor's Challenge:** 
*What happens if an LLM outputs an intent string that doesn't match any of your graph's edges? How do we build fault tolerance into graph routing?*

---

## Module 3: Vector Math & Semantic Chunking (Advanced RAG)
*Anyone can chunk by character count. Experts chunk by semantic meaning.*

**📚 Concept to Learn:**
*   Cosine Similarity, Dot Product, and HNSW graph indexes.
*   Parent-Child Chunking: Why you embed the child (for precision) but retrieve the parent (for context).
*   Semantic Chunking boundaries.

**🛠 Implementation Task:**
1. Spin up ChromaDB via Docker.
2. Write the chunking logic that takes a raw article and breaks it into Parent/Child segments.
3. Generate embeddings locally using Ollama (`nomic-embed-text`) and upsert them into ChromaDB, attaching metadata like `date` and `topic`.

**🧠 Mentor's Challenge:** 
*If two sentences have exactly opposite meanings (e.g., "I love this" vs "I hate this"), how far apart are they in the embedding space, really? You might be surprised by how embedding models actually work.*

---

## Module 4: Corrective RAG (CRAG) & Tool Calling
*Expert agents don't hallucinate; they self-correct when they don't know the answer.*

**📚 Concept to Learn:**
*   LLM Function Calling (Tool Use) at the API level.
*   Corrective RAG (CRAG) architecture: Retrieve -> Grade -> Generate/Rewrite.

**🛠 Implementation Task:**
1. Equip the `ResearchNode` from Module 2 with a `QueryChromaDB` tool.
2. Build the **Evaluator Node**: After retrieval, an LLM grades the context as `relevant` or `irrelevant`.
3. If irrelevant, route to a **Rewrite Node** to change the search query, or invoke a `Search_Web` tool.

**🧠 Mentor's Challenge:** 
*How do you prevent an agent from getting stuck in an infinite loop of retrieving, failing, and rewriting?*

---

## Module 5: High-Throughput Ingestion & Orchestration
*Connecting the AI brain to the global data firehose.*

**📚 Concept to Learn:**
*   Event-Driven Architecture (Publish-Subscribe patterns).
*   Workflow Orchestration (DAGs, Prefect).
*   Idempotency (Why your workers must be able to process the same message twice without breaking).

**🛠 Implementation Task:**
1. Spin up Redpanda. Write the `Ingestion_Worker` that fetches RSS feeds and pushes to `raw-articles`.
2. Write the `Enrichment_Worker` that listens to the queue, calls an LLM to extract topics, and pushes to ChromaDB.
3. Install Prefect and write a `@flow` to replace cron for the Outbound Daily Briefs.

**🧠 Mentor's Challenge:** 
*If the Enrichment Worker crashes halfway through processing 1,000 messages, how does Redpanda know which messages were successfully processed when the worker restarts?*

---

## Module 6: Multi-Tenant State & The WhatsApp API
*Turning a personal script into a scalable product.*

**📚 Concept to Learn:**
*   Multi-tenant data isolation.
*   Webhook security and asynchronous API responses.

**🛠 Implementation Task:**
1. Scaffold the SQLite Database (`Users`, `Interests`).
2. Build FastAPI endpoints to receive Twilio/WhatsApp webhooks.
3. Tie it all together: The incoming webhook triggers the LangGraph agent, injecting the specific User's SQLite `Interests` into the RAG search filter.

**🧠 Mentor's Challenge:** 
*If 50 users message the WhatsApp bot at the exact same second, how does your local FastAPI server handle the LangGraph invocations without blocking?*
