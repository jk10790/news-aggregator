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
from taut import TieredRoutingConfig, CapacityExceededError
from taut import Compression as CompressionConfig
from taut import SystemBlock
from taut.core.prompt_blocks import ContextBlock, QueryBlock
import chromadb
from prefect import flow, task
from prefect.tasks import task_input_hash

from config import (
    LLM_PROVIDER, GEMINI_API_KEY, GEMINI_MODEL, OLLAMA_HOST, OLLAMA_MODEL,
    CHROMA_SERVER_HOST, CHROMA_SERVER_PORT, MAX_RETRIES,
    TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_WHATSAPP_SENDER,
    MESSAGING_PROVIDER, TELEGRAM_BOT_TOKEN
)
from database import SessionLocal, User, Interest
from twilio.rest import Client

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
    MAP_PROMPT_TEMPLATE = "Summarize these articles."
    REDUCE_PROMPT_TEMPLATE = "Compile these summaries."

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
    routing=TieredRoutingConfig(),
    compression=CompressionConfig(json=True, code=False)
)
pipeline = taut.create_pipeline(taut_config)

MAP_LLM_PROVIDER = ollama_model_str
REDUCE_LLM_PROVIDER = gemini_model_str if GEMINI_API_KEY else ollama_model_str

# =========================================================================
# 4. LLM Wrapper Functions
# =========================================================================
async def query_llm_map(articles_data: list[dict], interests: list[str]) -> str:
    interests_str = ", ".join(interests) if interests else "general news"
    request = taut.LLMRequest(
        intent="extract_article_bullets",
        blocks=[
            SystemBlock(content=f"You are a data extraction assistant tailored for a user interested in: {interests_str}."),
            ContextBlock(content=MAP_PROMPT_TEMPLATE),
            QueryBlock(content=json.dumps(articles_data))
        ],
        model=MAP_LLM_PROVIDER
    )
    response = await pipeline.run(request)
    return response.content

async def query_llm_reduce(map_summaries: str, interests: list[str]) -> str:
    interests_str = ", ".join(interests) if interests else "general news"
    request = taut.LLMRequest(
        intent="compile_daily_brief",
        blocks=[
            SystemBlock(content=f"You are a news compiler assistant. You MUST output strictly in JSON matching the schema: {DailyBrief.model_json_schema()}. Focus on the user's interests: {interests_str}."),
            ContextBlock(content=REDUCE_PROMPT_TEMPLATE),
            QueryBlock(content=f"Compile these summaries into a cohesive daily brief that specifically caters to someone interested in {interests_str}:\n{map_summaries}")
        ],
        model=REDUCE_LLM_PROVIDER,
        response_format={"type": "json_object"},
        max_tokens=4096
    )
    response = await pipeline.run(request)
    return response.content

# =========================================================================
# 5. Prefect Tasks and Flows
# =========================================================================
def fetch_articles_from_chroma(interests: list[str]) -> list[dict]:
    try:
        chroma_client = chromadb.HttpClient(host=CHROMA_SERVER_HOST, port=int(CHROMA_SERVER_PORT))
        collection = chroma_client.get_collection("news_archive")
    except Exception as e:
        logger.error(f"ChromaDB connection failed: {e}")
        return []
    
    where_filter = {"type": "parent"}
    # Filter out "Top News" as it's a generic fallback, not a strict taxonomy topic
    valid_interests = [i for i in interests if i.lower() != "top news"]
    
    if valid_interests:
        topic_clauses = [{"topic": {"$eq": interest}} for interest in valid_interests]
        if len(topic_clauses) > 1:
            where_filter = {"$and": [{"type": "parent"}, {"$or": topic_clauses}]}
        elif len(topic_clauses) == 1:
            where_filter = {"$and": [{"type": "parent"}, topic_clauses[0]]}
            
    # Use similarity query instead of get() to pull the top 10 most relevant articles
    query_text = " ".join(interests) if interests else "important daily news tech startup"
    results = collection.query(
        query_texts=[query_text],
        n_results=10,
        where=where_filter
    )
    
    documents = results.get("documents", [[]])[0]
    metadatas = results.get("metadatas", [[]])[0]
    
    articles_data = []
    for doc, meta in zip(documents, metadatas):
        articles_data.append({
            "title": meta.get("title", "No Title"),
            "source": meta.get("source", "Unknown"),
            "url": meta.get("url", ""),
            "summary": doc
        })
    return articles_data

async def run_map_reduce(articles_data: list[dict], interests: list[str], phone_number: str) -> str:
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
        
    combined_map_summaries = "\n\n".join(map_summaries)
    return await query_llm_reduce(combined_map_summaries, interests)

def deliver_brief(phone_number: str, daily_brief_data: DailyBrief):
    # Build the message body from the daily brief
    body = f"*{daily_brief_data.date} Briefing*\n_{daily_brief_data.headline_summary}_\n\n"
    for category in daily_brief_data.categories:
        body += f"*{category.name}*\n"
        for article in category.articles:
            body += f"🔹 [{article.title}]({article.url})\n"
            for insight in article.key_insights:
                body += f"   • {insight}\n"
            body += "\n"
    body += "Reply to this message to ask questions about today's news!"

    if MESSAGING_PROVIDER == "telegram":
        if not TELEGRAM_BOT_TOKEN:
            logger.warning("Telegram credentials missing. Skipping Telegram delivery.")
            return
        try:
            import httpx
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
            payload = {
                "chat_id": phone_number,
                "text": body,
                "parse_mode": "Markdown"
            }
            # We use httpx synchronously or asynchronously; since this is not strictly async we use sync
            response = httpx.post(url, json=payload)
            if response.status_code == 200:
                logger.info(f"Telegram message sent to {phone_number}.")
            else:
                logger.error(f"Failed to send Telegram message to {phone_number}: {response.text}")
        except Exception as e:
            logger.error(f"Error sending Telegram message: {e}")

    else: # Default to Twilio
        if not TWILIO_ACCOUNT_SID or not TWILIO_AUTH_TOKEN:
            logger.warning("Twilio credentials missing. Skipping WhatsApp delivery.")
            return
        try:
            twilio_client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
            message = twilio_client.messages.create(
                from_=TWILIO_WHATSAPP_SENDER,
                to=f"whatsapp:{phone_number}",
                body=body
            )
            logger.info(f"WhatsApp message sent to {phone_number}: SID {message.sid}")
        except Exception as e:
            logger.error(f"Error sending Twilio message: {e}")

@task(retries=3, retry_delay_seconds=[10, 30, 60])
async def compile_user_brief(user_id: int, phone_number: str, interests: list[str]):
    articles_data = fetch_articles_from_chroma(interests)
    if not articles_data:
        logger.info(f"No matching articles found for {phone_number}. Skipping brief.")
        return
        
    try:
        raw_json_output = await run_map_reduce(articles_data, interests, phone_number)
        daily_brief_data = DailyBrief.model_validate_json(raw_json_output)
        
        outputs_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "outputs")
        os.makedirs(outputs_dir, exist_ok=True)
        output_path = os.path.join(outputs_dir, f"daily_brief_{phone_number}.json")
        with open(output_path, "w") as f:
            f.write(daily_brief_data.model_dump_json(indent=2))
            
        logger.info(f"Brief compiled and saved for {phone_number}.")
        
        # Deliver brief via chosen provider
        deliver_brief(phone_number, daily_brief_data)
        
    except Exception as e:
        logger.error(f"Failed to compile or deliver brief for {phone_number}: {e}")


@flow(name="Daily Proactive WhatsApp Briefs")
async def process_all_users():
    db = SessionLocal()
    users = db.query(User).all()
    
    tasks = []
    for user in users:
        import datetime
        now = datetime.datetime.utcnow()
        active_interests = []
        for i in user.interests:
            days_inactive = (now - i.last_interacted_at).days if getattr(i, 'last_interacted_at', None) else 0
            engagement = getattr(i, 'engagement_score', 1.0)
            engagement = engagement if engagement is not None else 1.0
            decayed_score = engagement * (0.5 ** (days_inactive / 7.0))
            if decayed_score >= 0.2:
                active_interests.append(i.topic)
        
        if active_interests:
            # Prefect await tasks
            await compile_user_brief(user.id, user.phone_number, active_interests)
            
    # After generating all briefs, prune old articles from ChromaDB to maintain vector speed
    from storage.cleanup import prune_old_articles
    from config import CHROMA_RETENTION_DAYS
    prune_old_articles(days_to_keep=CHROMA_RETENTION_DAYS)

if __name__ == "__main__":
    asyncio.run(process_all_users())
