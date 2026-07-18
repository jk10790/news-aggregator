import datetime
import email.utils
import hashlib
import logging
import re

from sentence_transformers import SentenceTransformer
import chromadb
from newsagg.config import CHROMA_SERVER_HOST, CHROMA_SERVER_PORT, EMBEDDING_MODEL
from newsagg.core.models import ArticleVerified

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Initialize the local embedding model on load
logger.info(f"Loading local embedding model '{EMBEDDING_MODEL}'...")
embedding_model = SentenceTransformer(EMBEDDING_MODEL)
logger.info("Embedding model loaded successfully.")

# Connect to the remote ChromaDB container
logger.info(f"Connecting to ChromaDB at HTTP {CHROMA_SERVER_HOST}:{CHROMA_SERVER_PORT}...")
chroma_client = chromadb.HttpClient(host=CHROMA_SERVER_HOST, port=int(CHROMA_SERVER_PORT))

def get_or_create_collection():
    """Retrieves or creates the target vector collection."""
    return chroma_client.get_or_create_collection(
        name="news_archive",
        metadata={"hnsw:space": "cosine"}
    )

def generate_url_hash(url: str) -> str:
    """Generates a short MD5 hash of the URL to act as a deterministic primary key."""
    return hashlib.md5(url.encode("utf-8")).hexdigest()

def parse_date_to_int(date_str: str) -> int:
    """
    Parses various date string formats (RFC 2822, ISO 8601, YYYY-MM-DD) into a YYYYMMDD integer.
    Defaults to today's date if parsing fails.
    """
    if not date_str or date_str == "Unknown Date":
        return int(datetime.date.today().strftime("%Y%m%d"))
    
    # 1. Try RFC 2822 format (common in RSS feeds, e.g. "Wed, 17 Jun 2026 15:09:20 +0000")
    try:
        dt = email.utils.parsedate_to_datetime(date_str)
        return int(dt.strftime("%Y%m%d"))
    except Exception:
        pass
        
    # 2. Try ISO 8601 / RFC 3339 format (e.g. "2026-06-21T20:30:56Z")
    try:
        clean_str = date_str.replace("Z", "+00:00")
        dt = datetime.datetime.fromisoformat(clean_str)
        return int(dt.strftime("%Y%m%d"))
    except Exception:
        pass
        
    # 3. Fallback: try scanning for YYYY-MM-DD pattern
    try:
        match = re.search(r"(\d{4})-(\d{2})-(\d{2})", date_str)
        if match:
            return int("".join(match.groups()))
    except Exception:
        pass

    # Safe fallback to today
    logger.warning(f"Could not parse date string '{date_str}'. Falling back to today's date.")
    return int(datetime.date.today().strftime("%Y%m%d"))

def chunk_text(text: str) -> list[str]:
    """
    Performs basic semantic chunking.
    Splits text by sentence boundaries and groups them into chunks of ~2-3 sentences.
    """
    sentences = re.split(r'(?<=[.!?]) +', text.strip())
    chunks = []
    current_chunk = []
    current_word_count = 0
    
    for sentence in sentences:
        if not sentence:
            continue
        words = len(sentence.split())
        
        if current_word_count + words > 60 and current_chunk:
            chunks.append(" ".join(current_chunk))
            current_chunk = []
            current_word_count = 0
            
        current_chunk.append(sentence)
        current_word_count += words
        
    if current_chunk:
        chunks.append(" ".join(current_chunk))
        
    return chunks

def store_article(article: ArticleVerified):
    """
    Splits an article into parent and child chunks, embeds the children,
    and inserts them into ChromaDB.
    """
    collection = get_or_create_collection()
    
    url = article.link
    parent_id = f"parent_{generate_url_hash(url)}"
    
    # Parse human date to integer YYYYMMDD
    published_int = parse_date_to_int(article.published)
    
    # 1. Deduplication Check (Semantic Similarity)
    parent_text = f"Title: {article.title}\nSummary: {article.summary}"
    try:
        dup_results = collection.query(
            query_texts=[parent_text],
            n_results=1,
            where={"type": "parent"}
        )
        if dup_results and dup_results.get("distances") and len(dup_results["distances"][0]) > 0:
            distance = dup_results["distances"][0][0]
            # Cosine distance < 0.15 indicates highly overlapping semantic meaning
            if distance < 0.15:
                logger.info(f"Semantic Duplicate Detected (distance {distance:.3f}). Skipping: '{article.title}'")
                return
    except Exception as e:
        logger.warning(f"Deduplication check failed, proceeding with insertion: {e}")

    # 2. Store the Parent Document (Title + Summary) without embeddings
    logger.info(f"Storing Parent Document: {parent_id} | '{article.title}' | DateInt: {published_int} | Impact: {article.importance_score}")
    topics_str = ",".join(article.topics) if article.topics else ""
    collection.upsert(
        ids=[parent_id],
        documents=[parent_text],
        metadatas=[{
            "type": "parent",
            "title": article.title,
            "url": url,
            "source": article.source,
            "published": article.published,
            "published_int": published_int,
            "triage_reason": article.triage_reason,
            "topics": topics_str,
            "importance_score": article.importance_score
        }]
    )
    
    # 2. Chunk the text into Child segments
    child_chunks = chunk_text(article.summary)
    if not child_chunks:
        child_chunks = [article.summary]
        
    logger.info(f"Split article into {len(child_chunks)} child chunks. Generating embeddings...")
    
    # 3. Generate embeddings for all child chunks locally
    child_embeddings = embedding_model.encode(child_chunks).tolist()
    
    # 4. Store the Child Documents
    child_ids = []
    child_metadatas = []
    
    for idx, chunk in enumerate(child_chunks):
        child_ids.append(f"child_{generate_url_hash(url)}_chunk_{idx}")
        child_metadatas.append({
            "type": "child",
            "parent_id": parent_id,
            "source": article.source,
            "url": url,
            "published": article.published,
            "published_int": published_int,  # Propagate numeric date
            "topics": topics_str,
            "importance_score": article.importance_score
        })
        
    collection.upsert(
        ids=child_ids,
        documents=child_chunks,
        embeddings=child_embeddings,
        metadatas=child_metadatas
    )
    logger.info(f"Successfully indexed parent {parent_id} and {len(child_ids)} child chunks.")

