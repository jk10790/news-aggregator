import datetime
import logging
import os
import sys

# Dynamic path resolution to import from parent directory
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from storage.vector_store import get_or_create_collection
from config import CHROMA_RETENTION_DAYS

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def prune_old_articles(days_to_keep: int = CHROMA_RETENTION_DAYS):
    """
    Deletes vectors from ChromaDB that are older than `days_to_keep`.
    Prevents vector index bloat and ensures RAG retrieval only fetches recent news.
    """
    logger.info(f"Starting ChromaDB Pruning Job. Deleting articles older than {days_to_keep} days.")
    try:
        collection = get_or_create_collection()
        
        # Calculate the YYYYMMDD cutoff integer
        cutoff_date = datetime.date.today() - datetime.timedelta(days=days_to_keep)
        cutoff_int = int(cutoff_date.strftime("%Y%m%d"))
        
        logger.info(f"TTL Cutoff set to {cutoff_int}")
        
        # Query ChromaDB for all document IDs older than the cutoff
        results = collection.get(
            where={"published_int": {"$lt": cutoff_int}},
            include=["metadatas"]
        )
        
        ids_to_delete = results.get("ids", [])
        
        if not ids_to_delete:
            logger.info("No stale articles found. Pruning complete.")
            return
            
        logger.info(f"Found {len(ids_to_delete)} stale chunks. Deleting from vector space...")
        
        # Delete them in batches if necessary, but Chroma handles normal list sizes fine
        collection.delete(ids=ids_to_delete)
        
        logger.info(f"Successfully deleted {len(ids_to_delete)} old chunks.")
    except Exception as e:
        logger.error(f"Failed to prune old articles: {str(e)}")

if __name__ == "__main__":
    prune_old_articles()
