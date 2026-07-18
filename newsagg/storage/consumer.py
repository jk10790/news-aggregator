"""Storage consumer (ADR-11): reads verified articles, embeds + upserts them
into ChromaDB, and manually commits offsets only after a full batch resolves.
Poison messages are routed to storage-dlq rather than dropped.
"""
import asyncio
import json
import logging

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from newsagg.config import REDPANDA_BROKER, TOPIC_VERIFIED_ARTICLES
from newsagg.core.models import ArticleVerified
from newsagg.storage.vector_store import store_article

logger = logging.getLogger(__name__)

STORAGE_DLQ_TOPIC = "storage-dlq"
GETMANY_MAX_RECORDS = 50


async def _publish_dlq(producer: AIOKafkaProducer, raw_value: bytes, error: Exception) -> None:
    try:
        payload = json.loads(raw_value.decode("utf-8"))
        if not isinstance(payload, dict):
            payload = {"raw": payload}
    except Exception:
        payload = {"raw": raw_value.decode("utf-8", errors="replace")}
    payload["error"] = str(error)
    payload["stage"] = "storage"
    await producer.send_and_wait(STORAGE_DLQ_TOPIC, value=json.dumps(payload).encode("utf-8"))


async def _process_one(raw_value: bytes, producer: AIOKafkaProducer) -> None:
    try:
        article = ArticleVerified.model_validate_json(raw_value.decode("utf-8"))
    except Exception as e:
        logger.error("Failed to parse verified article: %s", e)
        await _publish_dlq(producer, raw_value, e)
        return

    logger.info("Received verified article: '%s'", article.title)
    try:
        # store_article does blocking I/O (embeddings + Chroma HTTP calls);
        # run it off the event loop thread.
        await asyncio.to_thread(store_article, article)
    except Exception as e:
        logger.error("Failed to store article '%s': %s", article.title, e)
        await _publish_dlq(producer, raw_value, e)


async def process_batch(raw_values: list[bytes], producer: AIOKafkaProducer) -> None:
    """Processes every message in a batch, returning only once each has
    resolved (stored, or routed to storage-dlq)."""
    for raw_value in raw_values:
        await _process_one(raw_value, producer)


async def _run_once(consumer: AIOKafkaConsumer, producer: AIOKafkaProducer) -> int:
    """Fetches one batch, processes it fully, and commits offsets. Returns
    the number of messages processed (0 if the poll timed out empty)."""
    batches = await consumer.getmany(timeout_ms=5000, max_records=GETMANY_MAX_RECORDS)
    if not batches:
        return 0
    raw_values = [msg.value for messages in batches.values() for msg in messages]
    await process_batch(raw_values, producer)
    await consumer.commit()
    return len(raw_values)


async def main():
    logger.info("Starting Storage Consumer...")

    consumer = AIOKafkaConsumer(
        TOPIC_VERIFIED_ARTICLES,
        bootstrap_servers=REDPANDA_BROKER,
        group_id="storage-group-v2",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    producer = AIOKafkaProducer(bootstrap_servers=REDPANDA_BROKER)

    await consumer.start()
    await producer.start()
    logger.info("Connected to Redpanda. Consuming from '%s'...", TOPIC_VERIFIED_ARTICLES)

    try:
        while True:
            await _run_once(consumer, producer)
    finally:
        await consumer.stop()
        await producer.stop()
        logger.info("Storage consumer stopped.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    asyncio.run(main())
