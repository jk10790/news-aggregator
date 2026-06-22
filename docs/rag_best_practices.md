# RAG Best Practices, Data Retention & Vector DB Limitations

In production Retrieval-Augmented Generation (RAG) applications, finding the right balance between **semantic query recall** (finding the right concept) and **metadata pre-filtering** (filtering by time, user, or category) is critical. 

This document addresses how to handle queries without date constraints, vector database scaling limitations, and enterprise RAG best practices.

---

## 1. The Dynamic Date-Filtering Dilemma: Evergreen vs. Ephemeral

In our previous implementation, when a user did not specify a date bound, the translation engine defaulted to searching the **last 30 days**. 

### The Problem:
* **Ephemeral Queries**: Questions like *"What database updates happened this week?"* are time-bound. Pre-filtering by date is required to ensure fresh, relevant results.
* **Evergreen Queries**: Questions like *"What is Web Search on Bedrock AgentCore?"* are topical. The answer does not change based on today's calendar date, and the relevant article might have been ingested 6 months ago. 
* By forcing a default 30-day filter, we completely hide evergreen facts from the user.

### Best Practice Solution: Optional Filtering
Rather than always applying a date filter, the system should dynamically decide whether to filter based on the user's intent:
1. **Time-Bound Queries**: If the query implies a relative date ("yesterday", "this week"), extract the bounds and apply the `where` pre-filter.
2. **Topical/Fact-Seeking Queries**: If no date limit is mentioned, search the **entire database** by omitting the date filter constraint entirely.

---

## 2. Limitations of ChromaDB and Vector Databases

Vector databases differ significantly from traditional relational (SQL) or document (NoSQL) databases. When designing systems, you must account for their technical boundaries:

### A. Memory & Compute Overhead
* **Vector Indexing (HNSW)**: Most vector databases use Hierarchical Navigable Small World (HNSW) graphs for fast Approximate Nearest Neighbor (ANN) search. HNSW requires loading the index directly into memory (RAM).
* **Dimensionality Cost**: High-dimensional embeddings (e.g., 1536 dims for OpenAI) consume massive amounts of memory. Storing 10 million vectors can easily require hundreds of gigabytes of RAM.

### B. Metadata Filtering Constraints
* **Type Checking**: Databases like ChromaDB enforce strict type constraints. For example, order comparisons (`$gte`, `$lte`) only work on numeric values (`int` or `float`), not strings.
* **Scan Performance**: In simpler vector databases, metadata filtering is implemented as a post-filter or a basic linear scan, which slows down query latency as the database grows. In enterprise systems (like Milvus or Qdrant), indexes are created on metadata fields to speed up pre-filtering.

### C. Lack of Relational Operations
* Vector databases cannot perform SQL-style `JOIN` operations between collections. For example, you cannot join a "users" collection with a "documents" collection at query time. All context, access control lists (ACLs), and metadata must be denormalized and propagated down to the individual chunk level.

---

## 3. Production RAG Best Practices

To build enterprise-grade RAG systems, developers employ several advanced strategies:

### A. Hierarchical Retrieval (Parent-Child)
* **Ingestion**: Split documents into small **child chunks** (100–300 tokens) for precise vector representation, but link them to a larger **parent document** (full text or 1000+ tokens).
* **Retrieval**: Run the similarity search on child chunks to find the exact matching paragraph, but retrieve and feed the full parent document context to the generator LLM. This prevents the LLM from losing context.

### B. Re-Ranking (Cross-Encoders)
* **First Stage**: Retrieve a large set of candidates (e.g., top 25) using a fast, low-cost bi-encoder (vector similarity).
* **Second Stage**: Run the retrieved candidates through a **Cross-Encoder Re-ranker** (e.g., Cohere Re-rank, BAAI/bge-reranker). The cross-encoder computes full attention between the query and each document, scoring them with high accuracy. Pass only the top 5 to the LLM.

### C. Hybrid Search (Lexical + Semantic)
* **Dense Retrieval**: Vector search handles conceptual matching (e.g., matching "database scaling" to "sharding").
* **Sparse Retrieval**: BM25 (keyword search) handles exact term matches (e.g., product IDs, function names, or specific jargon like "AgentCore").
* **Reciprocal Rank Fusion (RRF)**: Combine the ranks of both search methods to get the absolute best of both worlds.

### D. Groundedness and Citations
* Always instruct the LLM to only answer using the provided context. If the query cannot be answered from the context, return a standard fallback: *"I do not have this information in my ingested feeds."*
* Require the LLM to output structured citations (e.g., appending `[AWS Blog](https://...)` to the specific facts it reports).
