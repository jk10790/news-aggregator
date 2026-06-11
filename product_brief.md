Product Requirement Document 
Project Title: Personalized AI News Aggregator & Analyst (Local-First MVP)
Target Execution Environment: Local Docker & AI Development Environment (Cursor / Claude Code)
1. Executive Summary & Objective
The objective is to build a local, zero-cost, end-to-end prototype of a Personalized AI News Aggregator and Analyst. The system ingests messy real-time data from various platforms, triages and filters content using high-speed LLM inference, stores semantically enriched chunks in a vector database, runs complex map-reduce summarization schedules, and presents an interactive conversational RAG interface.
The architecture is explicitly designed to map 1:1 with enterprise AWS services to serve as a practical playground for the AWS Generative AI Professional certification.
2. Core Product Principles
Local-First & Zero-Cost: The entire stack must run inside Docker or utilize free-tier APIs on a local developer machine.
Strict Cost Optimization: Implement multi-model routing (cheap models for triage, heavy models for reasoning) to mimic enterprise financial guardrails.
Information Density: Avoid long, generic text dumps. Summaries must use deterministic schemas and advanced chunking to preserve specific insights.
3. Product Phases & Technical Requirements
Phase 1: Ingestion & Triage Pipeline (The Firehose)
Functional Requirements:
Periodically poll external public streams (e.g., RSS feeds, Reddit JSON APIs, or mock news endpoints).
Publish raw, unstructured article data into a streaming broker to decouple data harvesting from processing.
Consume raw events asynchronously and execute a "Triage Prompt" using a high-throughput, low-latency LLM to check alignment against user interest profiles.
Drop irrelevant articles immediately; output accepted articles as structured JSON with core data fields (headline, body, author, source_url, timestamp).
Proposed Tech Stack (Local): Python (Asyncio) or Kotlin worker scripts, Dockerized Redpanda (Kafka API compatible), Groq API running Llama-3-8b-8192 (or free-tier Claude 3 Haiku).
AWS Enterprise Equivalent: Amazon Kinesis Data Streams / Amazon MSK + AWS Lambda + Amazon Bedrock (Claude 3 Haiku).
Phase 2: Structural Enrichment & Storage (The Archive)
Functional Requirements:
Process incoming validated JSON payloads from Phase 1.
Implement Semantic Chunking: Instead of rigid token windows, break articles down based on paragraph boundaries and embedding distance shifts.
Generate high-quality vector embeddings for each chunk.
Store chunks inside a vector database using a Parent-Child relation: Search queries look at small, precise chunks (children), but pass the broader context (parents) to the LLM.
Attach comprehensive metadata tags to every record: publish_date, source_url, primary_category, and extracted_entities.
Proposed Tech Stack (Local): Ollama running the nomic-embed-text model locally, Dockerized Milvus or ChromaDB.
AWS Enterprise Equivalent: Amazon OpenSearch Serverless (Vector Engine) + Amazon Titan Text Embeddings.
Phase 3: Knowledge Consolidation (The Daily Brief)
Functional Requirements:
Trigger a automated batch routine every morning (or via a specific endpoint trigger).
Query the database for all chunks and articles ingested within the last 24 hours matching the active user interest index.
Execute a strict Map-Reduce summarization pattern:
Map: Parallelize the collection into distinct topical batches (e.g., Tech, Finance, Policy) and generate concise, entity-dense bullet-point summaries per batch.
Reduce: Feed the batch summaries into a high-reasoning model to synthesize an overarching, cohesive daily briefing.
Force the final output into a deterministic JSON schema to guarantee clean rendering on frontends.
Proposed Tech Stack (Local): Python task runners (or standard cron scripting), Google Gemini 1.5 Flash/Pro Free Tier (or Anthropic API using Claude 3.5 Sonnet).
AWS Enterprise Equivalent: Amazon EventBridge + AWS Step Functions + AWS Lambda + Amazon Bedrock (Claude 3.5 Sonnet).
Phase 4: Conversational Discovery (Ask Your News)
Functional Requirements:
Expose an interactive conversational chat API interface.
Implement a Query Translation engine: Intercept relative time bounds from user queries (e.g., "What happened last week vs yesterday?") and programmatically calculate absolute date ranges.
Execute a Hybrid Search sequence: Query the vector database using a combination of semantic vector distance and strict structural metadata filters (the derived date ranges).
Ground the LLM's response strictly within the retrieved context chunks, enforcing strict inline source URLs or citation tags.
Proposed Tech Stack (Local): FastAPI (Python) or Ktor (Kotlin), LangChain / LangGraph (Python) or LangChain4j (Kotlin), Google Gemini Pro or Anthropic API.
AWS Enterprise Equivalent: Amazon Bedrock Agents + Amazon Bedrock Knowledge Bases.
4. Technical Architecture Mapping Matrix
Functional Layer
Local Dev Component (Zero Cost)
AWS Production Target
AWS Exam Core Focus Concept
Streaming & Ingestion
Redpanda (Docker)
Amazon Kinesis / MSK
Decoupled event buffering, cost controls
High-Volume Triage
Groq (Llama 3)
Bedrock (Claude 3 Haiku)
Model tiering, token input cost optimization
Vector Storage & Retrieval
Milvus / ChromaDB
OpenSearch Serverless
Hybrid search, semantic & hierarchical chunking
Batch Aggregation
Python / Cron
Step Functions + Lambda
Map-Reduce patterns, handling large context lengths
Orchestration Agent
LangGraph / LangChain4j
Bedrock Agents
ReAct framework, Tool Calling, Hallucination checks

5. Non-Functional Requirements & Security Guardrails
Prompt Engineering Isolation: System prompts must remain distinct from code logic, utilizing external template files.
API Rate Limiting: Local worker loops hitting free APIs (Groq/Gemini) must implement exponential backoff to handle HTTP 429 status codes elegantly.
Context Grounding Rules: The system prompt for Phase 4 must contain a strict fallback command: "If the retrieved context does not contain sufficient facts to answer the query, state that you do not have the information in your ingested feeds. Do not hallucinate external training knowledge."
Instructions for Next-Gen LLM Builder:
Use this document as your immutable architectural blueprint. Start by scaffolding the Docker Compose environment for Phase 1 containing the streaming broker, then move to building the ingestion and triage script sequentially. Maintain strict separation of concerns across the directory layout (/ingestion, /storage, /processor, /api).

