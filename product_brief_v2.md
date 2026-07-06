# Product Requirement Document (v2)
Project Title: The Agentic Newsroom (Multi-Tenant WhatsApp AI Aggregator)
Target Execution Environment: Local-First MVP (Docker + Free Tier APIs)
North Star Target: Enterprise AWS Cloud Deployment

## 1. Executive Summary & Vision
The vision is to build a highly scalable, multi-tenant "Agentic Newsroom" that serves as a personalized AI assistant for users via WhatsApp. The system will ingest a massive global firehose of data, enrich it, and store it semantically. Autonomous AI agents will then curate personalized daily briefs and answer real-time conversational queries on WhatsApp based on each user's specific interest profile.

**The North Star**: The application is designed to be completely **Cloud-Agnostic**. It will start entirely locally (using Dockerized Redpanda, ChromaDB, and Free Tier APIs) but is engineered using a Hexagonal Architecture (Ports and Adapters). This ensures that migrating to AWS (Kinesis, OpenSearch, Bedrock) requires simply changing a `config.yml` file, with zero modifications to the core agentic business logic.

## 2. Core Product Principles
*   **Agentic Over Imperative**: Replace rigid cron jobs and linear Python scripts with autonomous, stateful agents (LangGraph/CrewAI) capable of self-reflection, tool-calling, and dynamic routing.
*   **Hexagonal Architecture**: Absolute separation of core business logic from infrastructure. All databases, LLMs, and event streams must be wrapped in abstract interfaces.
*   **Multi-Tenant Personalization**: The central aggregator serves many users. State and context are isolated by User Phone Number.
*   **Cost & Scalability**: Leverage local infrastructure and free-tier APIs (Groq/Gemini Flash) for the MVP, with a paved road to managed AWS services for production scale.

## 3. Product Workflows & Agent Roles

### Workflow A: Global Ingestion (The Firehose)
1.  **Pollers**: Fetch raw feeds (RSS, APIs) and publish to a high-throughput event stream (`raw-articles`).
2.  **Enrichment Agent**: Consumes raw articles, extracts global metadata (Entities, Topics, Sentiment), and publishes to `enriched-articles`.
3.  **Storage Worker**: Sinks enriched articles into the Vector Database (with rich metadata) and optionally a Knowledge Graph.

### Workflow B: The Outbound Daily Brief (Proactive Agents)
*Replaces legacy cron jobs with a Workflow Orchestrator (e.g., Prefect).*
1.  **Orchestrator Flow**: Triggers daily. Queries the User Database for all active users and their `Interests`.
2.  **Editor Agent**: Spun up for each user. It queries the Vector DB applying strict metadata filters (`topic IN user.interests`).
3.  **Map-Reduce Synthesis**: The Editor Agent compiles the retrieved chunks into a highly personalized, emoji-rich summary formatted specifically for WhatsApp.
4.  **Delivery**: Payload is sent to the WhatsApp/Twilio outgoing queue.

### Workflow C: Inbound Conversational RAG (Reactive Agents)
1.  **Webhook Router**: Twilio webhook receives a user message and passes it to the **Router Agent**.
2.  **State Management**: The system retrieves the user's past conversation history and specific interests based on their phone number.
3.  **Research Agent**: If the user asks a news query, the Research Agent invokes a `Search_Vector_DB` tool, automatically injecting the user's interest filters.
4.  **Self-Reflective RAG (CRAG)**: The Research Agent evaluates the retrieved vectors. If insufficient, it autonomously falls back to a `Search_Web` tool.
5.  **Observer Agent (Background)**: Monitors the conversation for new topics the user mentions and updates their `Interests` profile in the SQLite database autonomously.

## 4. Infrastructure Mapping: Local vs. North Star (AWS)

Through the `config.yml`, the application can seamlessly toggle between local infrastructure and its North Star AWS equivalent:

| Functional Layer | Config Interface | Local MVP (Current) | North Star (AWS Target) |
| :--- | :--- | :--- | :--- |
| **Event Streaming** | `EventBusProvider` | Redpanda (Docker) | Amazon Kinesis Data Streams |
| **Vector Storage** | `VectorStoreProvider` | ChromaDB (Docker) | Amazon OpenSearch Serverless |
| **Triage LLM** | `LLMProvider` | Groq API (Llama-3.1 8b) | Amazon Bedrock (Claude 3 Haiku) |
| **Reasoning LLM** | `LLMProvider` | Gemini 1.5 Flash API | Amazon Bedrock (Claude 3.5 Sonnet) |
| **User Database** | `RelationalDBProvider`| SQLite | Amazon RDS (PostgreSQL) / DynamoDB |
| **Orchestration** | `WorkflowEngine` | Prefect (Local Server) | Prefect Cloud / AWS Step Functions |

## 5. Advanced GenAI Concepts to Implement
*   **Semantic Routing**: Agents classifying intent dynamically before executing a workflow.
*   **Tool Calling**: Equipping the LLM with Python REPLs, Web Search, and internal API triggers.
*   **Corrective RAG (CRAG)**: The ability for the agent to grade its own retrieval results and retry or pivot its search strategy before answering the user.
*   **GraphRAG (Phase 2)**: Storing entity relationships in a graph database to answer complex, multi-hop reasoning questions ("How are the companies funded today connected to OpenAI?").

## 6. Implementation Roadmap
*   **Phase 1: Hexagonal Foundation**: Scaffold the new directory structure (`/core`, `/infrastructure`, `/api`) and write the Abstract Base Classes.
*   **Phase 2: Global Ingestion**: Spin up Redpanda and ChromaDB via Docker Compose. Write the Enrichment Agent.
*   **Phase 3: Multi-Tenant & Workflow**: Scaffold the User SQLite DB. Implement Prefect to manage the Outbound WhatsApp brief generation.
*   **Phase 4: Agentic RAG**: Build the FastAPI webhook server and the LangGraph inbound Router/Research agents for Conversational WhatsApp interactions.
