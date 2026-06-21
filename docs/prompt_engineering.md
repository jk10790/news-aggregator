# Prompt Engineering for Smaller Models (Ollama/Llama-3-8B)

When building LLM-based pipelines, we often observe a major difference in behavior between massive cloud models (like Gemini 1.5 Pro or GPT-4) and smaller local models (like Llama 3 8B or Phi-3).

One common issue is **False Negative Bias** (being overly restrictive and rejecting relevant content). This guide explains why this happens and how we refine prompts to solve it.

---

## 1. Why Smaller Models Suffer from False Negative Bias

In our triage session, Llama 3 rejected articles about **ClickHouse** (a massive distributed database), **ASML/S3** (infrastructure), and **Rust/Java** (software engineering) claiming they *"lack relevance to distributed systems or software engineering."*

This happens for three reasons:

### Reason A: Weak Semantic Association
A massive model has billions of parameters and deep background knowledge. It instantly knows that:
$$\text{"ClickHouse"} \rightarrow \text{Distributed columnar OLAP database} \rightarrow \text{Distributed Systems}$$
A smaller 8B model has a compressed knowledge graph. It might know ClickHouse is a "database", but fails to map "database internals" back to the broader category of "Software Engineering" or "Distributed Systems" unless explicitly told.

### Reason B: Over-Sensitivity to Negative Constraints
In our interest profile, we listed:
> *"The user is NOT interested in: Marketing, advertising strategies, and non-technical business news."*
Smaller models have difficulty balancing complex positive and negative constraints. When they see a negative constraint (e.g. "business news"), and read a title like *"ClickHouse in open source"* (which mentions "open source" and "ten years"), they get confused and trigger the negative rule, defaulting to "false" to be safe.

### Reason C: "Instruction Compliance" vs. "Reasoning"
Smaller models are heavily aligned to follow formatting instructions (like outputting strict JSON) at the expense of their conceptual reasoning performance.

---

## 2. Advanced Prompt Refinement Techniques

To fix this, we apply three prompt engineering patterns:

### Pattern 1: Concept Expansion (Mapping Synonyms)
Instead of relying on the model to guess what "Software Engineering" means, we define it explicitly in the prompt:
* *Software Engineering includes: compilers, programming languages (Rust, C++, Go, Java, Python), database internals, git, testing, and editor configurations (Emacs, Vim).*

### Pattern 2: Calibrated Inclusivity (The Inclusion Bias)
We force the model to err on the side of caution. We add a rule:
* *If the article is borderline, or touches upon developer tools, database internals, cloud services, or coding, you MUST classify it as relevant (true). It is better to include a borderline article than to reject a technical one.*

### Pattern 3: Few-Shot Prompting (Learning by Example)
Instead of just explaining the rules (*Zero-Shot*), we show the model exact examples of input-output pairs (*Few-Shot*). This establishes a clear boundary for what is relevant.

```json
[Example 1 - Input]
Title: "Ten years of ClickHouse in open source"
[Example 1 - Output]
{
  "relevant": true,
  "reasoning": "ClickHouse is a distributed database system."
}
```
