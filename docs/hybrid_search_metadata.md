# Hierarchical Metadata Propagation in Vector Databases

In advanced RAG (Retrieval-Augmented Generation) architectures, we separate documents into hierarchical parent-child schemas:
* **Parents**: The full articles or large paragraphs containing the complete details.
* **Children**: The small, high-density coordinate chunks optimized for similarity search.

This separation introduces a key challenge during hybrid vector search: **how to apply metadata filters (like dates, categories, or user permissions) correctly.**

---

## 1. The Metadata Isolation Problem

In a vector database, search queries execute against the **Child** chunks. If a user queries:
> *"What database updates happened yesterday?"*

Our engine translates this query into:
1. **Semantic Query**: `"database updates"`
2. **Pre-Filtering Constraint**: `published == "2026-06-21"`

If the database is configured to search child chunks, it will run the metadata filter `published == "2026-06-21"` against the **Child** records.
* **The Failure**: If the `published` date property is only saved on the **Parent** records, the child records will not contain this attribute.
* **The Outcome**: The database filter returns **0 matches** because the target metadata attribute does not exist on the child documents being queried.

---

## 2. The Solution: Metadata Propagation

To ensure that pre-filtering constraints work correctly, we must **propagate (duplicate)** key metadata attributes from the parent article down into every child chunk during the storage phase:

```text
  Parent Document (Stored in DB)
  ┌──────────────────────────────────────────────────────────┐
  │ title: "Ten years of ClickHouse"                         │
  │ published: "2026-06-21"                                  │
  │ url: "https://..."                                       │
  └───────────────────────────┬──────────────────────────────┘
                              │
                              ▼ (Split & Propagate)
       ┌──────────────────────┼──────────────────────┐
       ▼                      ▼                      ▼
  Child Chunk 1          Child Chunk 2          Child Chunk 3
  ┌──────────────────┐   ┌──────────────────┐   ┌──────────────────┐
  │ parent_id: 102   │   │ parent_id: 102   │   │ parent_id: 102   │
  │ published: 21st  │   │ published: 21st  │   │ published: 21st  │ (Propagated)
  │ text: "..."      │   │ text: "..."      │   │ text: "..."      │
  └──────────────────┘   └──────────────────┘   └──────────────────┘
```

By ensuring that each child document contains copy of the `published` date, we enable the database to filter child chunks dynamically before performing vector calculations.

---

## 3. Storage Implementation (Python Syntax)

When writing child chunks to ChromaDB, we compile the propagated fields into the child metadatas array:

```python
for idx, chunk in enumerate(child_chunks):
    child_metadatas.append({
        "type": "child",
        "parent_id": parent_id,
        "source": article.source,
        "url": url,
        "published": article.published,       # Human-readable string
        "published_int": parse_date_to_int(article.published)  # Numeric YYYYMMDD for inequalities
    })
```
This guarantees that query engines can execute rapid, pre-filtered similarity searches using order-based operators (`$gte` and `$lte`) over large collections.

---

## 4. ChromaDB Strict Filtering Limitation

ChromaDB enforces strict type checking on its `where` clause:
* **Equality operators** (`$eq`, `$ne`) work with strings, integers, floats, and booleans.
* **Order-based operators** (`$gt`, `$gte`, `$lt`, `$lte`) **only** support numeric types (`int` or `float`).

If you attempt to perform an inequality comparison on a string metadata attribute (e.g., `{"published": {"$gte": "2026-05-22"}}`), ChromaDB will throw a `ValueError`:
> *Expected operand value to be an int or a float for operator $gte, got 2026-05-22 in query.*

### The Numeric Solution: YYYYMMDD Date Integer
To circumvent this limitation, we parse all incoming RSS date formats into a unified integer string format `YYYYMMDD` (e.g., `"2026-06-21"` becomes `20260621`) and store it as a separate integer field `published_int`. 

This allows us to run standard numeric comparisons inside ChromaDB's pre-filtering query:
```python
where_filter = {
    "$and": [
        {"type": {"$eq": "child"}},
        {"published_int": {"$gte": 20260522}},
        {"published_int": {"$lte": 20260621}}
    ]
}
```
This numeric approach ensures full compatibility with ChromaDB's fast indexing engine.
