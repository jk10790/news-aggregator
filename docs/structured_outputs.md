# Structured Outputs: Cloud vs. Local Schema Enforcement

In modern AI applications, getting an LLM to output structured JSON is critical for frontend rendering and database updates. However, the mechanisms used to enforce these schemas differ significantly between cloud providers (like Gemini/OpenAI) and local runtimes (like Ollama).

---

## 1. How Gemini/OpenAI Enforces JSON (Constrained Decoding)

When you pass a Pydantic schema to Gemini:
```python
GenerateContentConfig(
    response_mime_type="application/json",
    response_schema=DailyBrief
)
```
The cloud API does not just "ask" the model to output JSON. It uses a technique called **Constrained Decoding** (or Context-Free Grammar sampling) at the model's token generation layer:

1. **Logit Biasing**: When the model is about to generate the next token (word/character), the engine checks the Pydantic schema.
2. If the schema requires a bracket `{`, the engine programmatically forces the probability of generating a `{` to $100\%$, and all other characters to $0\%$.
3. If the model is currently outputting a key (e.g. `"date"`), the engine blocks the model from outputting anything other than characters that conform to the key definition.
4. **Outcome**: The output is mathematically guaranteed to conform to the exact JSON structure of your Pydantic model. It *cannot* output invalid syntax.

---

## 2. How Ollama Enforces JSON (JSON Mode)

When you call Ollama with `format="json"`, it runs a much simpler mechanism:

1. **Syntax Guarantee**: The engine checks the token output and ensures it represents valid JSON syntax (brackets are closed, commas are placed correctly, strings are quoted).
2. **Schema Blindness**: The model's local weights (Llama-3) *do not know* your specific Pydantic class fields (e.g., `date`, `categories`).
3. **The Risk**: Llama-3 might output a perfectly valid JSON object, but representing a different schema entirely (e.g. returning keys like `project_name` instead of `categories`).
4. **Outcome**: Pydantic's validator (`model_validate_json`) will throw a `ValidationError` because required fields are missing, even though the JSON itself is valid.

---

## 3. Optimizing Prompts for Local JSON Mode

To guide a local model to match a target schema, you must include a **Structural Blueprint** directly inside the prompt template.

### Guidelines for Local Prompts:
1. **Provide a Skeleton Example**: Show the exact structure of the JSON inside the prompt, using empty strings or placeholders.
2. **Key Definitions**: Explicitly list the keys and their expected types (e.g. `"categories": (array of objects)`).
3. **Pydantic Fallback**: Always wrap the validation in a `try-except` block to log the raw output and fail gracefully if validation fails.
