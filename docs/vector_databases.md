# Understanding Vector Databases & HNSW Indexing

Traditional relational databases (PostgreSQL, MySQL) index data using B-Trees, which are optimized for exact matches or numerical ranges (e.g. `WHERE age > 21` or `WHERE name = 'Alice'`).

A **Vector Database** (like ChromaDB, Milvus, or Qdrant) is built to store vectors and search for coordinates that are *close* to a query vector, even if there are no exact matches. This is called **K-Nearest Neighbors (KNN)** search.

---

## 1. The Challenge of exact KNN Search

If your database contains 1,000,000 document vectors, and a user inputs a search query, finding the exact closest vectors requires calculating the Cosine Similarity between the query vector and **every single one** of the 1,000,000 vectors.
* **Complexity**: $O(d \cdot N)$ where $d$ is dimensions (384) and $N$ is documents (1,000,000).
* **Problem**: This is called a "Flat" search. As $N$ scales, search latency increases linearly. Under heavy load, your system will stall.

To solve this, vector databases sacrifice 1-2% accuracy to gain massive speed. They use **Approximate Nearest Neighbor (ANN)** search algorithms.

---

## 2. HNSW (Hierarchical Navigable Small World) Indexing

ChromaDB uses the **HNSW** algorithm to index its collections. HNSW organizes vectors into a multi-layered graph, similar to how skip-lists work in standard memory structures.

### The Highway Network Analogy
Imagine you are driving from a small street in Seattle to a specific house in Miami:
1. You don't drive on local streets all the way.
2. First, you get onto a local highway, then a major Interstate (long-distance jumps, skipping thousands of cities).
3. As you get close to Florida, you exit the Interstate onto regional highways.
4. Finally, you drop down to local streets to find the exact house coordinate.

HNSW replicates this structure using layers of graphs:

```text
[HNSW Multi-Layer Search Graph]

Layer 2 (Express Links):   [Node A] ─────────────────────────── [Node Z]
                                                                   │
                                                                   ▼ (Drop Down)
Layer 1 (Regional Links):  [Node A] ───────── [Node M] ───────── [Node Z]
                                                 │                 │
                                                 ▼                 ▼
Layer 0 (Local Streets):   [Node A] ─ [Node G] ─ [Node M] ─ [Node Q] ─ [Node Z]
```

### The Search Process:
1. **Entry**: The search starts at a predefined entry point in **Layer 2** (the sparsest layer, containing only a few "hub" nodes).
2. **Greedy Routing**: The engine compares the query vector to the nodes in Layer 2 and moves to the node closest to the query.
3. **Drop Down**: When it cannot get any closer on Layer 2, it drops down to the same node's position in **Layer 1** (which contains more connections).
4. **Locality Search**: It repeats the routing on Layer 1, then drops to **Layer 0** (the densest layer containing every vector in the database) to find the exact closest match.

Using HNSW, the search complexity drops from $O(N)$ to **$O(\log N)$**, allowing the database to search millions of records in milliseconds.

---

## 3. Distance Metrics in Vector Space

When you create a collection in ChromaDB, you select a distance metric to define how "closeness" is measured. ChromaDB defaults to **L2 Squared (Euclidean Distance)**, but for text embeddings, **Cosine Similarity** is preferred:

1. **Squared L2 (Euclidean)** (`l2`):
   * Measures the straight-line distance between two points in space.
   * Drawback: Sensitive to document length. If one text is 10 words and another is 1000 words, their distance will be large even if they are about the same topic.
2. **Cosine Similarity** (`cosine`):
   * Measures only the angle between the vectors, ignoring vector length.
   * Best for text processing since long and short summaries map to similar angles.
3. **Inner Product** (`ip`):
   * Simple dot product. Extremely fast to calculate, but assumes all vectors have a normalized length of 1.0.
