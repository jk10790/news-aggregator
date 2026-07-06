import asyncio
import json
import logging
import os
import sys
from openai import AsyncOpenAI

# Dynamic path resolution to import config from parent directory (project root)
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import (
    LLM_PROVIDER, GEMINI_API_KEY, GEMINI_MODEL, OLLAMA_HOST, OLLAMA_MODEL,
    REDPANDA_BROKER, TOPIC_RAW_ARTICLES, TOPIC_VERIFIED_ARTICLES,
    USER_INTERESTS, RATE_DELAY_SECONDS, BACKOFF_BASE_SECONDS, MAX_RETRIES, TAUT_URL
)
from models import ArticleRaw, ArticleVerified
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Load the triage prompt templates from the isolated text files
SYSTEM_PROMPT_PATH = os.path.join(os.path.dirname(__file__), "prompts", "triage_system_prompt.txt")
USER_PROMPT_PATH = os.path.join(os.path.dirname(__file__), "prompts", "triage_user_prompt.txt")
try:
    with open(SYSTEM_PROMPT_PATH, "r") as f:
        TRIAGE_SYSTEM_PROMPT = f.read()
    with open(USER_PROMPT_PATH, "r") as f:
        TRIAGE_USER_PROMPT = f.read()
except FileNotFoundError:
    logger.error(f"Prompt files not found. Exiting.")
    exit(1)

# Initialize LLM Client pointing to Taut Proxy
taut_client = AsyncOpenAI(base_url=TAUT_URL, api_key="placeholder")

async def query_llm_triage(title: str, source: str, summary: str) -> dict:
    """
    Sends the article to the configured LLM provider via Taut proxy and returns the parsed JSON decision.
    Implements basic retries for robustness.
    """
    system_prompt = TRIAGE_SYSTEM_PROMPT.format(
        user_interests=USER_INTERESTS
    )
    user_prompt = TRIAGE_USER_PROMPT.format(
        title=title,
        source=source,
        summary=summary
    )
    
    model_name = f"gemini/{GEMINI_MODEL}" if LLM_PROVIDER == "gemini" else f"ollama/{OLLAMA_MODEL}"
    
    for attempt in range(MAX_RETRIES):
        try:
            response = await taut_client.chat.completions.create(
                model=model_name,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                response_format={"type": "json_object"}
            )
            return json.loads(response.choices[0].message.content)
                
        except Exception as e:
            delay = BACKOFF_BASE_SECONDS * (2 ** attempt)  # Exponential backoff
            logger.warning(f"LLM query failed (attempt {attempt + 1}/{MAX_RETRIES}): {str(e)}. Retrying in {delay}s...")
            await asyncio.sleep(delay)
            
    # If all retries fail, default to false (safe fallback)
    logger.error(f"All LLM triage attempts failed for: '{title}'. Marking as irrelevant.")
    return {"relevant": False, "reasoning": "LLM connection failed after multiple retries."}

async def main():
    logger.info(f"Starting Triage Consumer (Provider: {LLM_PROVIDER.upper()})...")
    
    # 1. Initialize Redpanda Consumer
    # We assign a group_id "triage-group" so Redpanda tracks our offsets.
    # auto_offset_reset="earliest" ensures we read all raw articles from the beginning if it is a new group.
    consumer = AIOKafkaConsumer(
        TOPIC_RAW_ARTICLES,
        bootstrap_servers=REDPANDA_BROKER,
        group_id="triage-group",
        auto_offset_reset="earliest",
        enable_auto_commit=True
    )
    
    # 2. Initialize Redpanda Producer to write validated articles
    producer = AIOKafkaProducer(bootstrap_servers=REDPANDA_BROKER)
    
    await consumer.start()
    await producer.start()
    logger.info("Successfully connected to Redpanda broker.")
    
    total_processed = 0
    accepted_count = 0
    
    try:
        while True:
            # 3. Read raw messages with a 5-second idle timeout (drain mode)
            try:
                # If no message arrives for 5 seconds, raises asyncio.TimeoutError
                msg = await asyncio.wait_for(consumer.getone(), timeout=5.0)
            except asyncio.TimeoutError:
                logger.info("No raw articles received for 5 seconds. Assuming queue is drained.")
                break
                
            # Decode and validate the message using our ArticleRaw contract
            article = ArticleRaw.model_validate_json(msg.value.decode("utf-8"))
            total_processed += 1
            
            logger.info(f"[{total_processed}] Triaging: '{article.title}' ({article.source})")
            
            # 4. Query LLM for triage decision
            decision = await query_llm_triage(article.title, article.source, article.summary)
            
            # Sleep to respect provider-specific rate constraints
            if RATE_DELAY_SECONDS > 0:
                await asyncio.sleep(RATE_DELAY_SECONDS)
            
            is_relevant = decision.get("relevant", False)
            reasoning = decision.get("reasoning", "No explanation.")
            
            if is_relevant:
                accepted_count += 1
                logger.info(f"   🟢 ACCEPTED: {reasoning}")
                
                # Instantiate the ArticleVerified contract
                verified_article = ArticleVerified(
                    **article.model_dump(),
                    triage_reason=reasoning
                )
                
                # Publish the serialized verified article to Redpanda
                serialized_verified = verified_article.model_dump_json().encode("utf-8")
                await producer.send_and_wait(TOPIC_VERIFIED_ARTICLES, value=serialized_verified)
            else:
                logger.info(f"   🔴 REJECTED: {reasoning}")
                
    except Exception as e:
        logger.error(f"Error in Consumer Triage loop: {str(e)}")
    finally:
        # 5. Clean up connections
        await consumer.stop()
        await producer.stop()
        logger.info(f"Triage session complete. Processed: {total_processed}, Accepted: {accepted_count}")

if __name__ == "__main__":
    asyncio.run(main())
