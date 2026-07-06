import asyncio
import datetime
import json
import logging
import os
import sys
from pydantic import BaseModel, Field

# Dynamic path resolution to import from parent directory (project root)
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import taut
import chromadb
from prefect import flow, task
from prefect.tasks import task_input_hash

from config import (
    LLM_PROVIDER, GEMINI_API_KEY, GEMINI_MODEL, OLLAMA_HOST, OLLAMA_MODEL,
    CHROMA_SERVER_HOST, CHROMA_SERVER_PORT, MAX_RETRIES
)
from database import SessionLocal, User, Interest

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# =========================================================================
# 1. Define Pydantic Schema for Structured Output
# =========================================================================
class ArticleBrief(BaseModel):
    title: str = Field(description="The original headline of the article")
    url: str = Field(description="The source URL link of the article")
    key_insights: list[str] = Field(description="2 to 3 entity-dense bullet points summarizing the insights")

class CategoryBrief(BaseModel):
    name: str = Field(description="The category name")
    articles: list[ArticleBrief] = Field(description="List of articles under this category")

class DailyBrief(BaseModel):
    date: str = Field(description="The date of the briefing in YYYY-MM-DD format")
    headline_summary: str = Field(description="A single-sentence overarching overview of today's key news")
    categories: list[CategoryBrief] = Field(description="List of categorized insights")

# =========================================================================
# 2. Load isolated prompts
# =========================================================================
PROMPTS_DIR = os.path.join(os.path.dirname(__file__), "prompts")
MAP_PROMPT_PATH = os.path.join(PROMPTS_DIR, "map_prompt.txt")
REDUCE_PROMPT_PATH = os.path.join(PROMPTS_DIR, "reduce_prompt.txt")

try:
    with open(MAP_PROMPT_PATH, "r") as f:
        MAP_PROMPT_TEMPLATE = f.read()
    with open(REDUCE_PROMPT_PATH, "r") as f:
        REDUCE_PROMPT_TEMPLATE = f.read()
except FileNotFoundError as e:
    logger.error(f"Failed to load prompts: {str(e)}.")
    MAP_PROMPT_TEMPLATE = "Summarize these articles: {articles_text}"
    REDUCE_PROMPT_TEMPLATE = "Compile these summaries: {map_summaries}"

# =========================================================================
# 3. Hybrid Client Initialization via Taut SDK
# =========================================================================
gemini_model_str = f"gemini/{GEMINI_MODEL}" if GEMINI_API_KEY else None
ollama_model_str = f"ollama/{OLLAMA_MODEL}"

# Initialize taut config with rate limiter (Local Token Bucket)
taut_config = taut.TautConfig(
    provider="litellm",
    num_retries=MAX_RETRIES,
    timeout=60.0,
    fallback_models=[ollama_model_str],
    routing=taut.TieredRoutingConfig(),
    compression=taut.CompressionConfig(json=True, code=False),
    rate_limiter=taut.TokenBucketRateLimiter(capacity=10, fill_rate=2) # Token bucket example
)
pipeline = taut.create_pipeline(taut_config)

MAP_LLM_PROVIDER = ollama_model_str
REDUCE_LLM_PROVIDER = gemini_model_str if GEMINI_API_KEY else ollama_model_str

# =========================================================================
# 4. LLM Wrapper Functions
# =========================================================================
async def query_llm_map(articles_text: str) -> str:
    request = taut.LLMRequest(
        blocks=[
            taut.SystemBlock(content="You are a data extraction assistant."),
            taut.ContextBlock(content=MAP_PROMPT_TEMPLATE),
            taut.QueryBlock(content=f"Extract bullet points for the following batch:\n{articles_text}")
        ],
        model=MAP_LLM_PROVIDER
    )
    response = await pipeline.run(request)
    return response.content

async def query_llm_reduce(map_summaries: str) -> str:
    request = taut.LLMRequest(
        blocks=[
            taut.SystemBlock(content="You are a news compiler assistant. You MUST output strictly in JSON matching the schema."),
            taut.ContextBlock(content=REDUCE_PROMPT_TEMPLATE),
            taut.QueryBlock(content=f"Compile these summaries:\n{map_summaries}")
        ],
        model=REDUCE_LLM_PROVIDER,
        response_format={"type": "json_object"}
    )
    response = await pipeline.run(request)
    return response.content

# =========================================================================
# 5. Prefect Tasks and Flows
# =========================================================================
@task(retries=3, retry_delay_seconds=[10, 30, 60])
async def compile_user_brief(user_id: int, phone_number: str, interests: list[str]):
    try:
        chroma_client = chromadb.HttpClient(host=CHROMA_SERVER_HOST, port=int(CHROMA_SERVER_PORT))
        collection = chroma_client.get_collection("news_archive")
    except Exception as e:
        logger.error(f"ChromaDB connection failed: {e}")
        return
    
    where_filter = {"type": "parent"}
    if interests:
        topic_clauses = [{"topic": {"$eq": interest}} for interest in interests]
        if len(topic_clauses) > 1:
            where_filter = {"$and": [{"type": "parent"}, {"$or": topic_clauses}]}
        elif len(topic_clauses) == 1:
            where_filter = {"$and": [{"type": "parent"}, topic_clauses[0]]}
            
    results = collection.get(where=where_filter)
    documents = results.get("documents", [])
    metadatas = results.get("metadatas", [])
    
    if not documents:
        return
        
    articles_data = []
    for doc, meta in zip(documents, metadatas):
        articles_data.append({
            "title": meta.get("title", "No Title"),
            "source": meta.get("source", "Unknown"),
            "url": meta.get("url", ""),
            "summary": doc
        })
        
    batch_size = 5
    map_tasks = []
    for i in range(0, len(articles_data), batch_size):
        batch = articles_data[i:i+batch_size]
        batch_text = ""
        for idx, art in enumerate(batch):
            batch_text += f"\n--- Article {idx+1} ---\nTitle: {art['title']}\nSource URL: {art['url']}\nContent: {art['summary']}\n"
        map_tasks.append(query_llm_map(batch_text))
        
    try:
        map_summaries = await asyncio.gather(*map_tasks)
    except getattr(taut.errors, "CapacityExceededError", Exception) as e:
        # If CapacityExceededError is raised, it triggers Prefect retry backoff
        logger.warning(f"Capacity exceeded for user {phone_number}. Yielding to Prefect backoff. {e}")
        raise
        
    combined_map_summaries = "\n\n".join(map_summaries)
    
    try:
        raw_json_output = await query_llm_reduce(combined_map_summaries)
        daily_brief_data = DailyBrief.model_validate_json(raw_json_output)
        
        output_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), f"daily_brief_{phone_number}.json")
        with open(output_path, "w") as f:
            f.write(daily_brief_data.model_dump_json(indent=2))
        logger.info(f"Brief compiled for {phone_number}.")
        
        # Here we would use Twilio SDK to send the WhatsApp message
        # twilio_client.messages.create(from_='whatsapp:+123', to=f'whatsapp:{phone_number}', body=f"Your brief is ready!")
        
    except Exception as e:
        logger.error(f"Reduce step failed: {e}")

@flow(name="Daily Proactive WhatsApp Briefs")
async def process_all_users():
    db = SessionLocal()
    users = db.query(User).all()
    
    tasks = []
    for user in users:
        interests = [i.topic for i in user.interests]
        # Prefect await tasks
        await compile_user_brief(user.id, user.phone_number, interests)

if __name__ == "__main__":
    asyncio.run(process_all_users())
