# Conversational RAG & Query Translation

In the final phase of our architecture, we implement the user-facing query engine. This engine must handle natural language questions and return fact-grounded answers with precise citations. 

To achieve this, we use two advanced techniques: **Query Translation** and **Hybrid Vector Retrieval**.

---

## 1. The Time-Bound Challenge: Query Translation

When a user asks:
* *"What AI chips were announced yesterday?"*
* *"Show me ClickHouse news from last week."*

A naive RAG system will convert the entire string into a vector:
$$\vec{v}_{\text{query}} = \text{Embed}(\text{"What AI chips were announced yesterday?"})$$

### The Failure Mode:
1. The embedding model does not know what calendar date "yesterday" represents.
2. It will perform a vector search for the semantic concept of "yesterday", returning articles that happen to use the word "yesterday" in their body, regardless of when they were published.
3. It will retrieve articles from months ago, leading to outdated or incorrect answers.

### The Solution: Query Translation
We run a **Query Translation Engine** (a fast, structured LLM call) to intercept the user query and parse it into two parts:
1. **The Semantic Query**: A stripped version of the query containing only the topical terms (e.g. `"AI chips"`). We use this vector to search ChromaDB.
2. **The Metadata Filter**: An absolute date range calculated based on today's calendar date (e.g., `start_date="2026-06-20"`, `end_date="2026-06-20"`).

---

## 2. Hybrid Vector Search (Pre-Filtering)

Once we have the semantic query and the absolute date ranges, we execute a **Hybrid Search** in ChromaDB. 

We query the child chunk collection using:
* The vector representing `"AI chips"` (cosine similarity).
* A strict metadata filter matching the calculated dates:

```python
collection.query(
    query_embeddings=[query_vector],
    where={
        "$and": [
            {"type": {"$eq": "child"}},
            {"published": {"$gte": start_date}},
            {"published": {"$lte": end_date}}
        ]
    }
)
```

By enforcing `published` dates as a **pre-filter**, ChromaDB instantly narrows the search space to only those articles published in the target range, and then runs the cosine similarity search within that small set.

---

## 3. Context Grounding & Citations

Once the database returns the best matching **Child Chunks**, our query engine:
1. Reads their metadata `parent_id`.
2. Queries ChromaDB to pull the full text of the corresponding **Parent Documents** (which contain the complete Title, Source, and URL).
3. Constructs the final RAG Prompt.

```text
[RAG Prompt Structure]

You are a factual news assistant. Answer the User Query using ONLY the retrieved Context.
If the context does not contain the answer, say "I do not have this information." Do not hallucinate.

### CONTEXT:
1. Source: [AWS Blog](https://aws.amazon.com/blog/1)
   Content: "Amazon Bedrock announces AgentCore for AI routing."

### USER QUERY:
"What is the new Bedrock release?"

### TARGET ANSWER:
"Amazon announced AgentCore on Bedrock to manage AI agent routing. ([AWS Blog](https://aws.amazon.com/blog/1))"
```

---

## 4. Async Server Architecture (FastAPI)

We wrap this engine inside **FastAPI**, an asynchronous web framework built on the **ASGI** (Asynchronous Server Gateway Interface) standard.

### Why Async?
In a RAG API:
* The server spends 99% of its time waiting for external services: waiting for ChromaDB to fetch vectors, waiting for local model encoders, and waiting for Google's API to return the generated response.
* In a synchronous server (like Flask), a worker thread is blocked during these waits, unable to handle other users.
* In an asynchronous server (using Python's `asyncio` inside FastAPI), the server yields control back to the event loop during network waits. A single CPU core can handle thousands of concurrent queries without blocking.
