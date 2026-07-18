import asyncio
import json
import logging

from aiokafka import AIOKafkaConsumer
from newsagg.config import REDPANDA_BROKER, TOPIC_VERIFIED_ARTICLES
from newsagg.storage.vector_store import store_article
from newsagg.core.models import ArticleVerified

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

async def main():
    logger.info("Starting Storage Consumer...")
    
    # 1. Initialize Redpanda Consumer
    consumer = AIOKafkaConsumer(
        TOPIC_VERIFIED_ARTICLES,
        bootstrap_servers=REDPANDA_BROKER,
        group_id="storage-group-v2",
        auto_offset_reset="earliest",
        enable_auto_commit=True
    )
    
    await consumer.start()
    logger.info(f"Connected to Redpanda. Consuming from topic '{TOPIC_VERIFIED_ARTICLES}'...")
    
    total_stored = 0
    
    try:
        while True:
            # 2. Read messages with a 5-second timeout (continuous polling)
            try:
                msg = await asyncio.wait_for(consumer.getone(), timeout=5.0)
            except asyncio.TimeoutError:
                continue
                
            # Decode and validate using the ArticleVerified contract
            article = ArticleVerified.model_validate_json(msg.value.decode("utf-8"))
            
            logger.info(f"Received verified article: '{article.title}'")
            
            # 3. Store the parent and embedded child chunks
            try:
                store_article(article)
                total_stored += 1
            except Exception as e:
                logger.error(f"Failed to store article '{article.title}': {str(e)}")
                
    except Exception as e:
        logger.error(f"Error in Consumer Storage loop: {str(e)}")
    finally:
        # 4. Clean up connections
        await consumer.stop()
        logger.info(f"Storage session complete. Successfully indexed: {total_stored} articles.")

if __name__ == "__main__":
    asyncio.run(main())
