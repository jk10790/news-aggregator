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
    fallback_models=[ollama_model_str, gemini_model_str] if gemini_model_str else [ollama_model_str],
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
    
    valid_interests = [i for i in interests if i.lower() != "top news"]
    # Use semantic similarity as the primary relevance engine.
    # ChromaDB 0.6.x does not support $contains — we only filter by type=parent
    # and then do a fast Python-side topic check on the results.
    query_text = " ".join(valid_interests) if valid_interests else "important daily news tech startup"
    results = collection.query(
        query_texts=[query_text],
        n_results=20,           # Fetch more so post-filtering has enough to work with
        where={"type": "parent"}
    )
    
    documents = results.get("documents", [[]])[0]
    metadatas = results.get("metadatas", [[]])[0]
    
    # Python-side topic relevance check: keep articles whose stored topic
    # matches at least one of the user's interests (case-insensitive substring)
    interests_lower = [i.lower() for i in valid_interests]
    articles_data = []
    for doc, meta in zip(documents, metadatas):
        stored_topic = meta.get("topics", "").lower()
        topic_match = not valid_interests or any(
            interest in stored_topic or stored_topic in interest
            for interest in interests_lower
        )
        if topic_match:
            articles_data.append({
                "title": meta.get("title", "No Title"),
                "source": meta.get("source", "Unknown"),
                "url": meta.get("url", ""),
                "summary": doc,
                "key_insights": meta.get("key_insights", "")
            })
    
    return articles_data[:10]  # Cap at 10 after filtering


async def run_map_reduce(articles_data: list[dict], interests: list[str], phone_number: str) -> str:
    # We skip the Map phase because key_insights are already pre-computed during ingestion
    map_summaries = []
    for article in articles_data:
        insights = article.get("key_insights", "")
        if insights:
            formatted_insights = insights.replace(' | ', '\n- ')
            summary = f"Title: {article['title']}\nURL: {article['url']}\nInsights: {formatted_insights}"
            map_summaries.append(summary)
            
    combined_map_summaries = "\n\n".join(map_summaries)
    return await query_llm_reduce(combined_map_summaries, interests)

import re as _re

def _plain(text: str) -> str:
    # Strip any LLM-generated markdown so Telegram formatting stays clean
    text = _re.sub(r'\*\*(.*?)\*\*', r'\1', text)   # **bold** -> plain
    text = _re.sub(r'\*(.*?)\*', r'\1', text)           # *italic* -> plain
    text = _re.sub(r'__(.*?)__', r'\1', text)             # __bold__ -> plain
    text = _re.sub(r'_(.*?)_', r'\1', text)               # _italic_ -> plain
    text = _re.sub(r'`(.*?)`', r'\1', text)               # `code` -> plain
    text = _re.sub(r'\[(.*?)\]\(.*?\)', r'\1', text)  # [text](url) -> text
    return text.strip()

def deliver_brief(user_label: str, daily_brief_data: DailyBrief,
                  telegram_chat_id: str = None, telegram_bot_token: str = None,
                  whatsapp_number: str = None):
    """Deliver a brief to a user via their configured channel.
    
    Uses per-user telegram_bot_token stored in DB. Falls back to the
    system TELEGRAM_BOT_TOKEN from .env if the user has no personal token.
    """
    # Build the message body from the daily brief
    body = f"\U0001f5de\ufe0f *THE DAILY BRIEF* | {daily_brief_data.date}\n"
    body += "\u3030\ufe0f" * 11 + "\n"
    body += f"_{_plain(daily_brief_data.headline_summary)}_\n\n"

    for category in daily_brief_data.categories:
        body += f"\U0001f525 *{category.name.upper()}*\n"
        for article in category.articles:
            body += f"\U0001f4f0 *{_plain(article.title)}*\n"
            for insight in article.key_insights:
                body += f"  \u2746 _{_plain(insight)}_\n"
            body += f"  \U0001f517 [Read Full Story]({article.url})\n\n"

    body += "\u3030\ufe0f" * 11 + "\n"
    body += "\U0001f4ac *Want to dive deeper?* Reply to ask questions about today's news!"

    if MESSAGING_PROVIDER == "telegram":
        # Per-user token takes priority; fall back to system-wide .env token
        effective_token = telegram_bot_token or TELEGRAM_BOT_TOKEN
        effective_chat_id = telegram_chat_id

        if not effective_token or not effective_chat_id:
            logger.warning(f"Telegram not configured for user {user_label}. Skipping delivery.")
            return
        try:
            import httpx
            url = f"https://api.telegram.org/bot{effective_token}/sendMessage"
            payload = {
                "chat_id": effective_chat_id,
                "text": body,
                "parse_mode": "Markdown"
            }
            response = httpx.post(url, json=payload)
            if response.status_code == 200:
                logger.info(f"Telegram message sent to {user_label} (chat_id={effective_chat_id}).")
            else:
                logger.error(f"Failed to send Telegram message to {user_label}: {response.text}")
        except Exception as e:
            logger.error(f"Error sending Telegram message to {user_label}: {e}")

    else:  # Twilio WhatsApp
        if not TWILIO_ACCOUNT_SID or not TWILIO_AUTH_TOKEN:
            logger.warning("Twilio credentials missing. Skipping WhatsApp delivery.")
            return
        if not whatsapp_number:
            logger.warning(f"No WhatsApp number for user {user_label}. Skipping delivery.")
            return
        try:
            twilio_client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
            message = twilio_client.messages.create(
                from_=TWILIO_WHATSAPP_SENDER,
                to=f"whatsapp:{whatsapp_number}",
                body=body
            )
            logger.info(f"WhatsApp message sent to {user_label}: SID {message.sid}")
        except Exception as e:
            logger.error(f"Error sending Twilio message to {user_label}: {e}")

@task(retries=3, retry_delay_seconds=[10, 30, 60])
async def compile_user_brief(user_id: int, user_label: str, interests: list[str],
                              telegram_chat_id: str = None, telegram_bot_token: str = None,
                              whatsapp_number: str = None):
    articles_data = fetch_articles_from_chroma(interests)
    if not articles_data:
        logger.info(f"No matching articles found for {user_label}. Skipping brief.")
        return

    try:
        raw_json_output = await run_map_reduce(articles_data, interests, user_label)
        daily_brief_data = DailyBrief.model_validate_json(raw_json_output)

        outputs_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "outputs")
        os.makedirs(outputs_dir, exist_ok=True)
        safe_label = str(user_id)
        output_path = os.path.join(outputs_dir, f"daily_brief_{safe_label}.json")
        with open(output_path, "w") as f:
            f.write(daily_brief_data.model_dump_json(indent=2))

        logger.info(f"Brief compiled and saved for {user_label}.")

        deliver_brief(
            user_label=user_label,
            daily_brief_data=daily_brief_data,
            telegram_chat_id=telegram_chat_id,
            telegram_bot_token=telegram_bot_token,
            whatsapp_number=whatsapp_number,
        )

    except Exception as e:
        logger.error(f"Failed to compile or deliver brief for {user_label}: {e}")


@flow(name="Daily Proactive WhatsApp Briefs")
async def process_all_users():
    db = SessionLocal()
    users = db.query(User).all()

    import datetime
    now = datetime.datetime.utcnow()

    for user in users:
        # Skip users with no delivery channel configured
        has_telegram = bool(user.telegram_chat_id)
        has_whatsapp = bool(user.phone_number)
        if not has_telegram and not has_whatsapp:
            logger.warning(f"User {user.id} has no delivery channel. Skipping.")
            continue

        # Skip paused users
        if getattr(user, 'delivery_cadence', 'daily') == 'paused':
            logger.info(f"User {user.id} is paused. Skipping.")
            continue

        # Compute engagement-decayed active interests
        active_interests = []
        for i in user.interests:
            days_inactive = (now - i.last_interacted_at).days if getattr(i, 'last_interacted_at', None) else 0
            engagement = getattr(i, 'engagement_score', 1.0) or 1.0
            decayed_score = engagement * (0.5 ** (days_inactive / 7.0))
            if decayed_score >= 0.2:
                active_interests.append(i.topic)

        if not active_interests:
            logger.info(f"User {user.id} has no active interests. Skipping.")
            continue

        user_label = user.name or user.telegram_chat_id or user.phone_number or str(user.id)
        await compile_user_brief(
            user_id=user.id,
            user_label=user_label,
            interests=active_interests,
            telegram_chat_id=user.telegram_chat_id,
            telegram_bot_token=user.telegram_bot_token,  # None = fall back to system token
            whatsapp_number=user.phone_number,
        )

    db.close()

    # Prune old articles from ChromaDB to maintain vector search speed
    from storage.cleanup import prune_old_articles
    from config import CHROMA_RETENTION_DAYS
    prune_old_articles(days_to_keep=CHROMA_RETENTION_DAYS)

if __name__ == "__main__":
    asyncio.run(process_all_users())
