import asyncio
import json
import logging
import os
from google import genai
from google.genai import types
import ollama
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from config import (
    LLM_PROVIDER, GEMINI_API_KEY, GEMINI_MODEL, OLLAMA_HOST, OLLAMA_MODEL,
    REDPANDA_BROKER, TOPIC_RAW_ARTICLES, TOPIC_VERIFIED_ARTICLES,
    USER_INTERESTS, RATE_DELAY_SECONDS, BACKOFF_BASE_SECONDS, MAX_RETRIES
)

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Load the triage prompt template from the isolated text file
PROMPT_PATH = os.path.join(os.path.dirname(__file__), "prompts", "triage_prompt.txt")
try:
    with open(PROMPT_PATH, "r") as f:
        TRIAGE_PROMPT_TEMPLATE = f.read()
except FileNotFoundError:
    logger.error(f"Prompt file not found at {PROMPT_PATH}. Exiting.")
    exit(1)

# Initialize LLM Clients
gemini_client = None
if LLM_PROVIDER == "gemini":
    if not GEMINI_API_KEY:
        logger.warning("GEMINI_API_KEY is empty in .env. Falling back to default or Ollama if configured.")
    # Initialize the new Google GenAI SDK Client
    gemini_client = genai.Client(api_key=GEMINI_API_KEY)
elif LLM_PROVIDER == "ollama":
    logger.info(f"Ollama provider active. Connecting to host: {OLLAMA_HOST}")
    # Async client for Ollama
    ollama_client = ollama.AsyncClient(host=OLLAMA_HOST)

async def query_llm_triage(title: str, source: str, summary: str) -> dict:
    """
    Sends the article to the configured LLM provider and returns the parsed JSON decision.
    Implements basic retries for robustness.
    """
    prompt = TRIAGE_PROMPT_TEMPLATE.format(
        user_interests=USER_INTERESTS,
        title=title,
        source=source,
        summary=summary
    )
    
    for attempt in range(MAX_RETRIES):
        try:
            if LLM_PROVIDER == "gemini":
                # We run this in an executor or use the async client. For now, since the 
                # new SDK supports direct generation, we will fetch it.
                # Use GEMINI_MODEL for high-speed triage.
                response = gemini_client.models.generate_content(
                    model=GEMINI_MODEL,
                    contents=prompt,
                    config=types.GenerateContentConfig(
                        response_mime_type="application/json",
                    ),
                )
                # Parse response text
                return json.loads(response.text)
                
            elif LLM_PROVIDER == "ollama":
                # Async call to local Ollama
                response = await ollama_client.generate(
                    model=OLLAMA_MODEL,
                    prompt=prompt,
                    format="json"  # Forces JSON schema response
                )
                return json.loads(response["response"])
            
            else:
                raise ValueError(f"Unsupported LLM provider: {LLM_PROVIDER}")
                
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
                
            # Decode the raw article from bytes to JSON
            raw_data = json.loads(msg.value.decode("utf-8"))
            total_processed += 1
            
            title = raw_data.get("title", "No Title")
            source = raw_data.get("source", "Unknown")
            summary = raw_data.get("summary", "")
            
            logger.info(f"[{total_processed}] Triaging: '{title}' ({source})")
            
            # 4. Query LLM for triage decision
            decision = await query_llm_triage(title, source, summary)
            
            # Sleep to respect provider-specific rate constraints
            if RATE_DELAY_SECONDS > 0:
                await asyncio.sleep(RATE_DELAY_SECONDS)
            
            is_relevant = decision.get("relevant", False)
            reasoning = decision.get("reasoning", "No explanation.")
            
            if is_relevant:
                accepted_count += 1
                logger.info(f"   🟢 ACCEPTED: {reasoning}")
                
                # Append triage explanation to metadata
                raw_data["triage_reason"] = reasoning
                
                # Publish to 'verified-articles' topic
                serialized_verified = json.dumps(raw_data).encode("utf-8")
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
