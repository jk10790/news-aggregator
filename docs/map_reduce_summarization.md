# Map-Reduce Summarization & Context Engineering

When building scale-ready AI systems, we face a major challenge: **how to summarize large volumes of data without running out of token limits, incurring massive API costs, or losing key details.**

To solve this, we adapt a classic distributed systems pattern: **Map-Reduce**.

---

## 1. The Challenge of Large-Scale Summarization

Imagine your news aggregator accepts **200 articles** a day. If you feed all 200 articles directly into a single LLM prompt:
1. **Cost**: You would send roughly 150,000 tokens per API call, costing significant API credits per run.
2. **"Lost in the Middle" Phenomenon**: Research shows that LLMs pay high attention to the beginning and end of long context windows, but frequently forget details embedded in the middle.
3. **Context Overflow**: If the volume spikes to 1,000 articles, you will exceed the token context limits of standard models.

---

## 2. The Map-Reduce Pattern for LLMs

The **Map-Reduce** pattern (originally popularized by Google for processing massive datasets across clusters of computers) divides the task into two distinct steps:

```text
               [ 200 Raw Verified Articles ]
                             │
                             ▼ (Split into topical batches)
       ┌─────────────────────┼─────────────────────┐
       ▼ (Batch A: AI)       ▼ (Batch B: DBs)      ▼ (Batch C: Langs)
 ┌───────────┐         ┌───────────┐         ┌───────────┐
 │ MAP STAGE │         │ MAP STAGE │         │ MAP STAGE │  (Cheap, fast LLM)
 └─────┬─────┘         └─────┬─────┘         └─────┬─────┘
       │ (Bullet points)     │ (Bullet points)     │ (Bullet points)
       ▼                     ▼                     ▼
 ┌─────────────────────────────────────────────────┐
 │                  REDUCE STAGE                   │  (High-reasoning LLM)
 └────────────────────────┬────────────────────────┘
                          │ (Cohesive Synthesis)
                          ▼
             [ Deterministic JSON Brief ]
```

### Phase 1: The MAP Step (Parallel Processing)
1. We group the raw documents into small batches (e.g., by topic or in chunks of 5-10 articles).
2. For each batch, we run a **Map Prompt** using a fast, cheap model (e.g., `gemini-2.5-flash`).
3. The model extracts key entities, names, and concrete facts as bullet points. 
4. **Outcome**: 200 large articles are compressed into 3 distinct, high-density bullet-point summaries.

### Phase 2: The REDUCE Step (Cohesive Synthesis)
1. We collect all the bullet-point summaries from the Map step.
2. We feed these summaries into a single **Reduce Prompt** using a higher-reasoning model.
3. The model analyzes the summaries, groups them logically, removes redundant news, and synthesizes them into a single cohesive report.
4. **Outcome**: A single, clean, structured summary covering all 200 articles.

---

## 3. Context Engineering & Hallucination Guardrails

During the Map and Reduce steps, we must prevent the LLMs from fabricating external details (hallucinating). We enforce two strict constraints in the prompt instructions:

1. **Closed-World Constraint**:
   *"You must ground your summary strictly within the provided context. If a fact, name, or detail is not explicitly written in the input, do not mention it."*
2. **Entity Density Constraint**:
   *"Avoid vague summaries (e.g. 'The article discusses database improvements'). Use concrete entities and metrics (e.g., 'ClickHouse version 24.6 introduces 3x faster vector searches using HNSW indexes')."*

---

## 4. Deterministic JSON Synthesis

To display the daily brief on a webpage, the output must be structured. If the LLM outputs markdown or conversational text, the frontend code will fail to parse it.

We force the LLM to output a strict, validated JSON schema:

```json
{
  "date": "YYYY-MM-DD",
  "headline_summary": "One sentence daily overview.",
  "categories": [
    {
      "name": "Topic (e.g. Distributed Systems)",
      "articles": [
        {
          "title": "Article Title",
          "url": "Source URL",
          "key_insights": [
            "Entity-dense bullet point 1",
            "Entity-dense bullet point 2"
          ]
        }
      ]
    }
  ]
}
```

By using the SDK's **Structured Output** features, we force the LLM's decoding layers to only output tokens that conform to this specific JSON structure.
