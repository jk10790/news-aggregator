import asyncio
import json
import logging
import os
from typing import Literal

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from pydantic import BaseModel

from newsagg.config import REDPANDA_BROKER, TOPIC_RAW_ARTICLES, TOPIC_VERIFIED_ARTICLES
from newsagg.core.models import ArticleRaw, ArticleVerified
from newsagg.core.llm import complete


class TriageOutput(BaseModel):
    relevant: bool
    reasoning: str
    topics: list[Literal["AI", "Cloud", "Security", "Startups", "Programming", "Distributed Systems", "Databases"]]
    entities: list[str]
    importance_score: int

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


async def query_llm_triage(title: str, source: str, summary: str) -> dict:
    """
    Sends the article through the shared LLM gateway (newsagg.core.llm.complete,
    ADR-3) and returns the parsed JSON decision. `core.llm.complete` already
    implements the Taut -> Gemini fallback and retries internally.

    NOTE: this still returns a raw dict rather than validating against
    TriageOutput end-to-end (taxonomy-slug mapping, key_insights plumbing) —
    that tightening is Phase 4 work.
    """
    system_prompt = TRIAGE_SYSTEM_PROMPT
    user_prompt = TRIAGE_USER_PROMPT.format(
        title=title,
        source=source,
        summary=summary
    )

    try:
        text = await complete(
            tier="simple",
            system=system_prompt,
            user=user_prompt,
            context="Triage-Agent",
        )
        return json.loads(text)
    except Exception as e:
        logger.error(f"LLM triage query failed for '{title}': {str(e)}")
        return {"relevant": False, "reasoning": "LLM connection failed after multiple retries.", "topics": [], "entities": []}

async def main():
    logger.info("Starting Triage Consumer...")

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

    except Exception as e:
        logger.error(f"Error in Consumer Triage loop: {str(e)}")
    finally:
        # 5. Clean up connections
        await consumer.stop()
        await producer.stop()
        logger.info(f"Triage session complete. Processed: {total_processed}, Accepted: {accepted_count}")

if __name__ == "__main__":
    asyncio.run(main())
