import sys
import os
import asyncio

# 1. Update models.py
with open("models.py", "r") as f:
    content = f.read()

target = """    entities: list[str] = Field(default_factory=list, description="Extracted entities")
    importance_score: int = Field(default=5, description="Global impact rating (1-10)")"""

replacement = """    entities: list[str] = Field(default_factory=list, description="Extracted entities")
    importance_score: int = Field(default=5, description="Global impact rating (1-10)")
    key_insights: list[str] = Field(default_factory=list, description="2 to 3 entity-dense bullet points summarizing the insights")"""

if target in content:
    with open("models.py", "w") as f:
        f.write(content.replace(target, replacement))
else:
    print("Could not find target in models.py")


# 2. Update ingestion/prompts/triage_system_prompt.txt
with open("ingestion/prompts/triage_system_prompt.txt", "r") as f:
    content = f.read()

target_json = """### RESPONSE FORMAT (STRICT JSON ONLY):
{
  "relevant": true,
  "reasoning": "Explain the decision in 10 words or less.",
  "topics": ["AI"],
  "entities": ["OpenAI"],
  "importance_score": 5
}"""

replacement_json = """### RESPONSE FORMAT (STRICT JSON ONLY):
{
  "relevant": true,
  "reasoning": "Explain the decision in 10 words or less.",
  "topics": ["AI"],
  "entities": ["OpenAI"],
  "importance_score": 5,
  "key_insights": ["Insight bullet 1", "Insight bullet 2"]
}"""

if target_json in content:
    with open("ingestion/prompts/triage_system_prompt.txt", "w") as f:
        f.write(content.replace(target_json, replacement_json))
else:
    print("Could not find target in triage_system_prompt.txt")

# 3. Update storage/vector_store.py
with open("storage/vector_store.py", "r") as f:
    content = f.read()

target = """    # 2. Store the Parent Document (Title + Summary) without embeddings
    logger.info(f"Storing Parent Document: {parent_id} | '{article.title}' | DateInt: {published_int} | Impact: {article.importance_score}")
    topics_str = ",".join(article.topics) if article.topics else ""
    collection.upsert(
        ids=[parent_id],
        documents=[parent_text],
        metadatas=[{
            "type": "parent",
            "title": article.title,
            "url": url,
            "source": article.source,
            "published": article.published,
            "published_int": published_int,
            "triage_reason": article.triage_reason,
            "topics": topics_str,
            "importance_score": article.importance_score
        }]
    )"""

replacement = """    # 2. Store the Parent Document (Title + Summary) without embeddings
    logger.info(f"Storing Parent Document: {parent_id} | '{article.title}' | DateInt: {published_int} | Impact: {article.importance_score}")
    topics_str = ",".join(article.topics) if article.topics else ""
    insights_str = " | ".join(article.key_insights) if getattr(article, "key_insights", None) else ""
    
    collection.upsert(
        ids=[parent_id],
        documents=[parent_text],
        metadatas=[{
            "type": "parent",
            "title": article.title,
            "url": url,
            "source": article.source,
            "published": article.published,
            "published_int": published_int,
            "triage_reason": article.triage_reason,
            "topics": topics_str,
            "importance_score": article.importance_score,
            "key_insights": insights_str
        }]
    )"""

if target in content:
    with open("storage/vector_store.py", "w") as f:
        f.write(content.replace(target, replacement))
else:
    print("Could not find target in vector_store.py")

# 4. Update processor/daily_brief.py
with open("processor/daily_brief.py", "r") as f:
    content = f.read()

target1 = """    articles_data = []
    for doc, meta in zip(documents, metadatas):
        articles_data.append({
            "title": meta.get("title", "No Title"),
            "source": meta.get("source", "Unknown"),
            "url": meta.get("url", ""),
            "summary": doc
        })
    return articles_data"""

replacement1 = """    articles_data = []
    for doc, meta in zip(documents, metadatas):
        articles_data.append({
            "title": meta.get("title", "No Title"),
            "source": meta.get("source", "Unknown"),
            "url": meta.get("url", ""),
            "summary": doc,
            "key_insights": meta.get("key_insights", "")
        })
    return articles_data"""

target2 = """async def run_map_reduce(articles_data: list[dict], interests: list[str], phone_number: str) -> str:
    batch_size = 5
    map_tasks = []
    for i in range(0, len(articles_data), batch_size):
        batch = articles_data[i:i+batch_size]
        map_tasks.append(query_llm_map(batch, interests))
        
    try:
        map_summaries = await asyncio.gather(*map_tasks)
    except CapacityExceededError as e:
        logger.warning(f"Capacity exceeded for user {phone_number}. Yielding to Prefect backoff. {e}")
        raise
        
    combined_map_summaries = "\\n\\n".join(map_summaries)
    return await query_llm_reduce(combined_map_summaries, interests)"""

replacement2 = """async def run_map_reduce(articles_data: list[dict], interests: list[str], phone_number: str) -> str:
    # We skip the Map phase because key_insights are already pre-computed during ingestion
    map_summaries = []
    for article in articles_data:
        insights = article.get("key_insights", "")
        if insights:
            summary = f"Title: {article['title']}\\nURL: {article['url']}\\nInsights: {insights.replace(' | ', '\\n- ')}"
            map_summaries.append(summary)
            
    combined_map_summaries = "\\n\\n".join(map_summaries)
    return await query_llm_reduce(combined_map_summaries, interests)"""

if target1 in content and target2 in content:
    with open("processor/daily_brief.py", "w") as f:
        f.write(content.replace(target1, replacement1).replace(target2, replacement2))
else:
    print("Could not find targets in daily_brief.py")

# 5. Update api/observer.py
with open("api/observer.py", "r") as f:
    content = f.read()

target = """    prompt = f\"\"\"
    You are an AI observing a user's conversational history.
    Based on their latest message, extract any implied topics of interest.
    For example, if they ask about "SpaceX", output "Aerospace".
    
    Current Interests: {state['current_interests']}
    Latest Message: "{latest_msg}"
    
    Output strictly in JSON. You must return a JSON object with exactly two keys: "topics" (a list of strings) and "confidence" (a list of floats between 0.0 and 1.0). You MUST extract at least one interest. Example: {{"topics": ["Aerospace"], "confidence": [0.95]}}
    \"\"\""""

replacement = """    prompt = f\"\"\"
    You are an AI observing a user's conversational history.
    Based on their latest message, extract any implied topics of interest.
    CRITICAL: You must explicitly calculate intent and sentiment negative constraints.
    Distinguish between inquiry/curiosity ("Tell me more about X") and rejection/fatigue ("I don't care about Y", "stop showing me X").
    ONLY extract topics the user has a POSITIVE sentiment or active curiosity towards.
    If the user expresses fatigue or rejection of a topic (e.g., "I am sick of hearing about Elon Musk"), do NOT include it, or give it a 0.0 confidence score.
    
    Current Interests: {state['current_interests']}
    Latest Message: "{latest_msg}"
    
    Output strictly in JSON. You must return a JSON object with exactly two keys: "topics" (a list of strings) and "confidence" (a list of floats between 0.0 and 1.0). Only include topics with positive sentiment. Example: {{"topics": ["Aerospace"], "confidence": [0.95]}}
    \"\"\""""

if target in content:
    with open("api/observer.py", "w") as f:
        f.write(content.replace(target, replacement))
else:
    print("Could not find target in observer.py")

# 6. Update ingestion/consumer_triage.py
with open("ingestion/consumer_triage.py", "r") as f:
    content = f.read()

# We need to replace the while loop entirely with the new semaphore-based batch processor.
target_triage = """    try:
        while True:
            # 3. Read raw messages with a 5-second timeout (continuous polling)
            try:
                # If no message arrives for 5 seconds, raises asyncio.TimeoutError
                msg = await asyncio.wait_for(consumer.getone(), timeout=5.0)
            except asyncio.TimeoutError:
                continue
                
            # Decode and validate the message using our ArticleRaw contract
            article = ArticleRaw.model_validate_json(msg.value.decode("utf-8"))
            total_processed += 1
            
            logger.info(f"[{total_processed}] Triaging: '{article.title}' ({article.source})")
            
            # 4. Query LLM for triage decision
            decision = await query_llm_triage(article.title, article.source, article.summary)
            
            is_relevant = decision.get("relevant", False)
            reasoning = decision.get("reasoning", "No explanation.")
            
            if is_relevant:
                accepted_count += 1
                logger.info(f"   🟢 ACCEPTED: {reasoning}")
                
                # Instantiate the ArticleVerified contract
                verified_article = ArticleVerified(
                    **article.model_dump(),
                    triage_reason=reasoning,
                    topics=decision.get("topics", []),
                    entities=decision.get("entities", [])
                )
                
                # Publish the serialized verified article to Redpanda
                serialized_verified = verified_article.model_dump_json().encode("utf-8")
                await producer.send_and_wait(TOPIC_VERIFIED_ARTICLES, value=serialized_verified)
            else:
                logger.info(f"   🔴 REJECTED: {reasoning}")
                
    except Exception as e:"""

replacement_triage = """    semaphore = asyncio.Semaphore(10)

    async def process_msg(msg):
        nonlocal total_processed, accepted_count
        try:
            article = ArticleRaw.model_validate_json(msg.value.decode("utf-8"))
            logger.info(f"Triaging: '{article.title}' ({article.source})")
            
            async with semaphore:
                decision = await query_llm_triage(article.title, article.source, article.summary)
            
            is_relevant = decision.get("relevant", False)
            reasoning = decision.get("reasoning", "No explanation.")
            
            if is_relevant:
                logger.info(f"   🟢 ACCEPTED: {reasoning}")
                verified_article = ArticleVerified(
                    **article.model_dump(),
                    triage_reason=reasoning,
                    topics=decision.get("topics", []),
                    entities=decision.get("entities", []),
                    key_insights=decision.get("key_insights", [])
                )
                serialized_verified = verified_article.model_dump_json().encode("utf-8")
                await producer.send_and_wait(TOPIC_VERIFIED_ARTICLES, value=serialized_verified)
                return True
            else:
                logger.info(f"   🔴 REJECTED: {reasoning}")
                return False
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            return False

    try:
        while True:
            records = await consumer.getmany(timeout_ms=5000, max_records=50)
            if not records:
                continue
                
            tasks = []
            for tp, messages in records.items():
                for msg in messages:
                    total_processed += 1
                    tasks.append(process_msg(msg))
                    
            if tasks:
                results = await asyncio.gather(*tasks)
                accepted_count += sum(1 for res in results if res)
                
    except Exception as e:"""

# Add TriageOutput model update to include key_insights
target_triage_model = """class TriageOutput(BaseModel):
    relevant: bool
    reasoning: str
    topics: list[Literal["AI", "Cloud", "Security", "Startups", "Programming", "Distributed Systems", "Databases"]]
    entities: list[str]
    importance_score: int"""
    
replacement_triage_model = """class TriageOutput(BaseModel):
    relevant: bool
    reasoning: str
    topics: list[Literal["AI", "Cloud", "Security", "Startups", "Programming", "Distributed Systems", "Databases"]]
    entities: list[str]
    importance_score: int
    key_insights: list[str]"""

if target_triage in content and target_triage_model in content:
    content = content.replace(target_triage, replacement_triage).replace(target_triage_model, replacement_triage_model)
    with open("ingestion/consumer_triage.py", "w") as f:
        f.write(content)
else:
    print("Could not find target in consumer_triage.py")

print("Patch complete.")
