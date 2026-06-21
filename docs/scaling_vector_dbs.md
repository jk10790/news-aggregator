# Scaling Vector Databases to Enterprise Volumes

As a distributed system grows, storing and searching high-dimensional vectors introduces significant bottlenecks in memory, storage, and CPU consumption. This guide details the engineering strategies used to scale vector databases to production volumes.

---

## 1. The Memory Bottleneck (RAM Cost)

Unlike relational databases that store indexes on disk (SSD), **vector search indexes (like HNSW graphs) must reside entirely in RAM** to maintain millisecond response times.

### The Memory Math:
Let's calculate the RAM needed to store **100,000,000 document vectors** using a standard 768-dimensional embedding model (e.g. OpenAI's `text-embedding-3-small`):

1. **Raw Vector Size**:
   * Each dimension is a 32-bit floating-point number (4 bytes).
   * 1 vector = $768 \times 4 \text{ bytes} = 3,072 \text{ bytes} \approx 3 \text{ KB}$.
2. **Raw Database Size**:
   * $100,000,000 \times 3 \text{ KB} = 300,000,000 \text{ KB} \approx 300 \text{ GB}$ of raw RAM.
3. **Index Graph Overhead**:
   * The HNSW index graph stores the links (edges) between nodes. This adds roughly 50% to 100% memory overhead.
   * **Total RAM Required: ~450 GB to 600 GB.**

At enterprise scale, hosting 600 GB of RAM in cloud instances is extremely expensive. We use **Vector Quantization** to scale down.

---

## 2. Vector Quantization (Compression)

Quantization reduces the bit-depth of vector dimensions to save space:

### Scalar Quantization (SQ)
* **What it does**: Converts 32-bit float values (which range between $-1.0$ and $1.0$ with high precision) to 8-bit integers (`int8`, values from $0$ to $255$).
* **Compression**: Reduces memory footprint by **75%** (from 4 bytes to 1 byte per dimension).
* **RAM needed**: 300 GB drops to **75 GB**.
* **Trade-off**: Introduces a minor loss in retrieval accuracy (recall drops by ~1-2%).

### Product Quantization (PQ)
* **What it does**: Breaks a high-dimensional vector into smaller sub-vectors, maps each sub-vector to a predefined cluster centroid (using a codebook), and stores only the index of the closest centroid.
* **Compression**: Can compress vectors by **95%** or more.
* **Trade-off**: Requires training the codebook on your dataset, and search speeds can slow down due to decompression calculations.

---

## 3. Metadata Pre-Filtering vs. Post-Filtering

When searching vectors, we often combine semantic search with strict structural filters (e.g. `"Find AI news from techcrunch published in the last 24 hours"`):

```text
[POST-FILTERING]                                [PRE-FILTERING]
Query Vector -> Search 1,000,000 vectors        Metadata Index -> Filter to 500 documents
                     │                                                │
                     ▼                                                ▼
           Retrieve top 100 matches                      Search vector space on only
                     │                                        those 500 vectors
                     ▼
           Filter to match criteria
     (Might return 0 results if matches
        didn't make top 100 initially)
```

* **Post-Filtering (Inefficient)**:
  1. The database performs the HNSW vector search first, returning the top $K$ semantic matches (e.g., 100 matches).
  2. It then discards any matches that don't fit the metadata filter (e.g. they aren't from "techcrunch").
  3. **The Risk**: If the top 100 semantic matches are all from hacker_news, post-filtering returns 0 results to the user.
* **Pre-Filtering (Scale-Ready)**:
  1. The database uses a traditional relational index (like a B-Tree or inverted index) to instantly isolate the subset of records matching the metadata criteria.
  2. It then performs the vector similarity calculations *only* inside that subset.
  3. This guarantees you get results and reduces the search space, saving massive CPU cycles.

---

## 4. Sharding and Distributed Architecture

Single-node databases (like ChromaDB or SQLite) cannot scale horizontally. To scale to billions of vectors, enterprise systems (like Milvus or Qdrant) use **Distributed Sharding**:

1. **Partitioning (Sharding)**:
   * The vector index is split across multiple database nodes (shards).
   * Shards can be partitioned by **Document ID** (each node gets a random slice of vectors) or by **Category/Metadata** (Node A stores tech vectors, Node B stores health vectors).
2. **Scatter-Gather Queries**:
   * The query vector is broadcasted concurrently to all shards.
   * Each shard searches its local HNSW graph and returns its top matches.
   * The coordinator node merges the results, reranks them, and returns the final top matches to the client.
