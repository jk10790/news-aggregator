"""ChromaDB retention cleanup (Phase 5 / used by newsagg.scheduler daily at
03:00 UTC per the overhaul plan). Lazy — importing this module never touches
Chroma; the client is only constructed the first time `prune_expired()` runs,
via `vector_store.get_or_create_collection()`.
"""
import datetime
import logging

from newsagg import config
from newsagg.storage.vector_store import get_or_create_collection

logger = logging.getLogger(__name__)


def prune_expired(retention_days: int | None = None) -> int:
    """
    Deletes vectors from ChromaDB whose `published_int` is older than
    `retention_days` (defaults to `config.CHROMA_RETENTION_DAYS`, read at
    call time so it can't go stale / isn't needed at import time).
    Prevents vector index bloat and ensures RAG retrieval only fetches recent news.

    Returns the number of chunks deleted.
    """
    days_to_keep = retention_days if retention_days is not None else config.CHROMA_RETENTION_DAYS
    logger.info("Starting ChromaDB Pruning Job. Deleting articles older than %s days.", days_to_keep)
    try:
        collection = get_or_create_collection()

        # Calculate the YYYYMMDD cutoff integer
        cutoff_date = datetime.date.today() - datetime.timedelta(days=days_to_keep)
        cutoff_int = int(cutoff_date.strftime("%Y%m%d"))

        logger.info("TTL Cutoff set to %s", cutoff_int)

        # Query ChromaDB for all document IDs older than the cutoff
        results = collection.get(
            where={"published_int": {"$lt": cutoff_int}},
            include=["metadatas"],
        )

        ids_to_delete = results.get("ids", [])

        if not ids_to_delete:
            logger.info("No stale articles found. Pruning complete.")
            return 0

        logger.info("Found %d stale chunks. Deleting from vector space...", len(ids_to_delete))

        # Delete them in batches if necessary, but Chroma handles normal list sizes fine
        collection.delete(ids=ids_to_delete)

        logger.info("Successfully deleted %d old chunks.", len(ids_to_delete))
        return len(ids_to_delete)
    except Exception as e:
        logger.error("Failed to prune old articles: %s", str(e))
        return 0


# Back-compat alias (pre-overhaul name).
prune_old_articles = prune_expired


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    prune_expired()
