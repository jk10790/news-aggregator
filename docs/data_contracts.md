# Data Contracts & Serialization (Protobuf, Avro, Pydantic)

In an early prototype, developers often pass data around using raw Python dictionaries (e.g. `article.get("link")`). However, in production-grade distributed systems, **untyped dicts are a major anti-pattern.**

---

## 1. The Problem with Untyped Dictionaries

If Service A (Producer) sends raw JSON to Service B (Consumer):
1. **Lack of Type Safety**: A typo in your consumer code (`article.get("lnik")` instead of `"link"`) will compile fine but fail at runtime, returning `None` silently.
2. **Schema Drift**: If the team building the Producer renames the JSON key `link` to `source_url`, the Consumer service will immediately break.
3. **No Cross-Language Contract**: In a distributed system, the producer might be written in Python, but a consumer might be written in Kotlin or Go. A raw Python dictionary cannot be shared across languages.

To solve this, we enforce a **Data Contract**—a shared, immutable specification of the data model.

---

## 2. Serialization Formats: Comparison

Distributed systems use three major serialization paradigms to enforce data contracts:

| Feature | JSON (with Pydantic) | Protocol Buffers (Protobuf) | Apache Avro |
| :--- | :--- | :--- | :--- |
| **Type Safety** | 🟢 High (Python only) | 🟢 High (Cross-Language) | 🟢 High (Cross-Language) |
| **Data Size** | 🔴 Large (verbose text keys) | 🟢 Tiny (binary format) | 🟢 Tiny (binary format) |
| **Schema Registry** | 🔴 No | 🟡 Optional | 🟢 Mandatory (standard in Kafka) |
| **Build Step** | 🟢 None | 🔴 Required (`protoc` compiler) | 🟡 Minimal |
| **Serialization Speed**| 🟡 Medium | 🟢 Extremely Fast | 🟢 Extremely Fast |

---

## 3. Deep Dive: Protocol Buffers (Google Protobuf)

Protobuf is Google's language-neutral, platform-neutral binary serialization format.

### How it works:
1. You write a schema definition file: `article.proto`
   ```protobuf
   syntax = "proto3";

   message Article {
     string title = 1;
     string source = 2;
     string url = 3;
     string summary = 4;
     string published = 5;
   }
   ```
2. You run the Protobuf compiler (`protoc`). It compiles the `.proto` file into Python classes, Java POJOs, or Go Structs.
3. Your Python producer populates the compiled class:
   ```python
   article = Article(title="Hacker News", url="https://...")
   producer.send(topic, value=article.SerializeToString()) # Sends highly compressed binary bytes
   ```
4. Your Kotlin consumer receives the binary bytes and deserializes it back into a native Kotlin object:
   ```kotlin
   val article = Article.parseFrom(messageBytes)
   ```

---

## 4. Deep Dive: Apache Avro & Schema Registry

In the Kafka/Redpanda ecosystem, **Apache Avro** combined with a **Confluent Schema Registry** is the enterprise standard.

```text
[Producer] ──(1. Check Schema)──> [Schema Registry]
    │                                    │
    ▼ (2. Send binary data)              ▼ (3. Cache Schema Definition)
[Redpanda Broker] ───────────────> [Consumer] (4. Fetch Schema to decode)
```

### How it works:
1. **Schema Isolation**: The schema is written in JSON but stored centrally in a service called the **Schema Registry**.
2. **Bandwidth Optimization**: Unlike JSON which repeats the keys `"title"`, `"url"` in every single message, Avro strips the keys entirely. It sends only raw binary values prefixed with a 4-byte Schema ID.
3. **Compatibility Safeguards**: If a developer tries to deploy a producer that breaks the schema contract (e.g., deleting a required field), the Schema Registry rejects the change, preventing the broker from ever accepting the corrupt data.

---

## 5. Local Solution: Shared Pydantic Models

For Python-only services, we enforce the data contract using **Pydantic Models** defined in a shared file (e.g. `models.py` at the project root) that both our ingestion and storage services import:

```python
from pydantic import BaseModel, HttpUrl

class ArticleEvent(BaseModel):
    title: str
    source: str
    link: HttpUrl # Validates that this is a real HTTP link
    summary: str
    published: str
```
By loading the raw JSON bytes directly into the model:
`event = ArticleEvent.model_validate_json(raw_json)`
We guarantee that if any field is missing or invalid, the code raises an error *before* we execute database writes or LLM requests.
