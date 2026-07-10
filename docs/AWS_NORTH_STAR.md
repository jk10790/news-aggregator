# North Star Implementation Plan: Enterprise AWS & Taut Integration

This document outlines the migration of the Agentic Newsroom from a local Dockerized MVP to its North Star target: a highly scalable, cloud-agnostic Enterprise AWS deployment. Crucially, this plan embeds the **Taut AI Efficiency Middleware** deeply into every phase of the architecture to guarantee "Zero Waste Compute" at enterprise scale.

---

## Phase 1: The Hexagonal Cloud Foundation

**Goal:** Establish the AWS infrastructure and update the Hexagonal ports to seamlessly route through Taut.

1. **Infrastructure as Code (IaC):**
   * Provision Amazon Kinesis Data Streams (replacing Redpanda).
   * Provision Amazon OpenSearch Serverless (replacing ChromaDB).
   * Deploy the **Taut Proxy Server** via AWS ECS (Elastic Container Service) behind an Application Load Balancer (ALB).
2. **Adapter Refactoring:**
   * Update the abstract `EventStreamProvider` to use `boto3` Kinesis APIs.
   * Update the `VectorStoreProvider` to use `opensearch-py`.
   * **Taut Integration:** Update the `LLMProvider` adapter to point globally to the ECS Taut Proxy ALB. Configure Taut's underlying provider via LiteLLM to point to **Amazon Bedrock**.

---

## Phase 2: Global Ingestion & Enrichment (The Firehose)

**Goal:** Process massive volumes of RSS feeds via Kinesis and extract entities/topics using Bedrock, optimized by Taut.

1. **Kinesis Consumers:**
   * Deploy the Pollers and Enrichment Agents as AWS Lambda functions triggered by the `raw-articles` Kinesis stream.
2. **Taut Integration - Layer 3 (Payload Compression):**
   * The firehose will generate massive text blocks. The Enrichment Agent will rely on Taut's `CompressionConfig(json=True)` to strip out boilerplate structural data before it hits Bedrock (Claude 3 Haiku).
3. **Taut Integration - Layer 1 (Semantic Caching):**
   * Redundant news articles (e.g., AP wire stories published across 50 feeds) will hit Taut's Semantic Cache. Taut will immediately return the cached entity extraction, completely bypassing the Bedrock API and saving massive costs on firehose ingestion.

---

## Phase 3: Multi-Tenant & Workflow Orchestration

**Goal:** Manage state per user and compile personalized daily briefs.

1. **State Management:**
   * Scaffold Amazon RDS (PostgreSQL) or DynamoDB for the `Users` and `Interests` tables.
2. **Workflow Orchestration:**
   * Migrate from local Prefect to **AWS Step Functions** to manage the Outbound Daily Brief Map-Reduce flow.
3. **Taut Integration - Layer 4 (Prefix Alignment):**
   * The Map-Reduce Lambda workers will use the **Taut Python SDK** (instead of the proxy) to explicitly construct `SystemBlock`, `ContextBlock`, and `QueryBlock` payloads. This guarantees maximum KV cache discounts from Bedrock on the heavy context mapping phase.
4. **Taut Integration - Tenant Isolation:**
   * Inject the user's phone number as the `X-Taut-Namespace` header into every outbound LLM request. This ensures that the Semantic Cache for personalized briefs is strictly isolated per tenant in the RDS/DynamoDB ecosystem.

---

## Phase 4: Agentic RAG (Conversational WhatsApp)

**Goal:** Real-time conversational agent via Twilio and WhatsApp.

1. **API Gateway:**
   * Deploy an Amazon API Gateway + AWS Lambda to receive Twilio webhooks.
2. **LangGraph on AWS:**
   * Host the stateful LangGraph Router and Research Agents on AWS Lambda or ECS.
3. **Taut Integration - Layer 2 (Tiered Routing):**
   * When a user asks a simple greeting ("Hi", "What can you do?"), Taut automatically routes the request to a cheaper model (Claude 3 Haiku).
   * When the query requires deep RAG analysis ("Synthesize yesterday's updates on Web Search on Bedrock"), Taut dynamically escalates it to Claude 3.5 Sonnet.
4. **Taut Integration - Layer 5 (Output Restraint):**
   * Taut enforces strict output brevity, which is critical for WhatsApp's character limits and UX, preventing the LLM from generating unnecessarily bloated, expensive responses.

---

## 🚀 Taut SDK Enterprise Capabilities

The newly updated **Taut SDK** natively implements several critical features that make it an undisputed powerhouse for this Enterprise AWS workload:

1. **Streaming Cache Playback (Critical for UX):**
   * Taut supports caching streamed responses and instantly "playing back" the stream from the cache. For conversational AI (Phase 4), this minimizes time-to-first-token (TTFT) and guarantees instantaneous responses on repeat queries without blocking or degrading the chat UX.
2. **Resilience Primitives (Token Buckets):**
   * In Phase 2 (Firehose Ingestion), we will easily hit AWS Bedrock's Tokens-Per-Minute (TPM) limits. Taut intercepts this traffic and uses a Token Bucket algorithm to emit `CapacityExceededError`s. This allows our AWS Step Functions / Prefect MapReduce jobs to handle backpressure and retries cleanly, without bloating the SDK with persistent queuing dependencies.
3. **Graph / XML Compression (Layer 3):**
   * Adding onto JSON and Code compression, Taut's Extensible Compression Layer natively handles AST/structural compression for XML (RSS feeds) and Graph data. This makes Taut an essential tool for Phase 2's knowledge-graph pipelines, drastically cutting down context window sizes before hitting the Cloud LLM.
4. **Dynamic Fallback Routing:**
   * Taut seamlessly supports dynamic fallback routing. If AWS Bedrock or the primary LLM throttles or times out under heavy load, Taut automatically reroutes the request to alternative fallback models, ensuring the conversational pipeline remains highly available.
