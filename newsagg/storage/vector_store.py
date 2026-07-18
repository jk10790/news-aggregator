"""ChromaDB-backed vector store for verified articles (Phase 5).

Critical fix vs. the pre-overhaul version: this module used to construct a
`chromadb.HttpClient` and load a `SentenceTransformer` at import time, which
meant `import newsagg.storage.vector_store` failed with no live infra. Both
are now lazy (`lru_cache` accessors) so importing this module never touches
the network or loads a model — construction only happens the first time
`get_or_create_collection()` / an embedding call is actually used.

ADR-8: parents AND children are embedded explicitly via
`newsagg.core.embeddings.embed()` — never Chroma's default embedder.
ADR-4: every parent (and child, for filterable search) carries a
`topic_<slug>` boolean for every classifiable taxonomy slug.
"""
import datetime
import email.utils
import hashlib
import logging
import re
from functools import lru_cache

import chromadb

from newsagg import config
from newsagg.core.embeddings import embed
from newsagg.core.models import ArticleVerified
from newsagg.core.taxonomy import CLASSIFIABLE, SLUGS, chroma_key

logger = logging.getLogger(__name__)

COLLECTION_NAME = "news_archive"
DEDUP_DISTANCE_THRESHOLD = 0.15


@lru_cache(maxsize=1)
def get_chroma_client():
    """Lazily constructs the ChromaDB HTTP client. Never called at import time."""
    logger.info(
        "Connecting to ChromaDB at HTTP %s:%s...",
        config.CHROMA_SERVER_HOST, config.CHROMA_SERVER_PORT,
    )
    return chromadb.HttpClient(
        host=config.CHROMA_SERVER_HOST, port=int(config.CHROMA_SERVER_PORT)
    )


def get_or_create_collection():
    """Retrieves or creates the target vector collection."""
    return get_chroma_client().get_or_create_collection(
        name=COLLECTION_NAME,
        metadata={"hnsw:space": "cosine"},
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
    logger.warning("Could not parse date string '%s'. Falling back to today's date.", date_str)
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


def topic_filter(slugs: list[str]) -> dict:
    """Builds a ChromaDB `where` clause selecting parent docs matching any of
    the given taxonomy slugs (ADR-4). The pseudo-topic "top" is excluded from
    the OR clause — it isn't a boolean metadata key, it's a curated "high
    importance" view handled separately by callers (e.g. brief_engine)."""
    ors = [{chroma_key(s): {"$eq": True}} for s in slugs if s in SLUGS and s != "top"]
    if not ors:
        return {"type": {"$eq": "parent"}}
    clause = ors[0] if len(ors) == 1 else {"$or": ors}
    return {"$and": [{"type": {"$eq": "parent"}}, clause]}


def _topic_booleans(article_topics: list[str]) -> dict:
    return {chroma_key(t.slug): (t.slug in article_topics) for t in CLASSIFIABLE}


def store_article(article: ArticleVerified):
    """
    Splits an article into parent and child chunks, embeds everything
    explicitly via core.embeddings, and upserts into ChromaDB.
    """
    collection = get_or_create_collection()

    url = article.link
    parent_id = f"parent_{generate_url_hash(url)}"

    # Parse human date to integer YYYYMMDD
    published_int = parse_date_to_int(article.published)

    # Parent text + explicit embedding (ADR-8 — never Chroma's default embedder)
    parent_text = f"{article.title} {article.summary}"
    parent_embedding = embed([parent_text])[0]

    # 1. Deduplication check (semantic similarity) using the same explicit embedder
    try:
        dup_results = collection.query(
            query_embeddings=[parent_embedding],
            n_results=1,
            where={"type": "parent"},
        )
        if dup_results and dup_results.get("distances") and len(dup_results["distances"][0]) > 0:
            distance = dup_results["distances"][0][0]
            # Cosine distance < 0.15 indicates highly overlapping semantic meaning
            if distance < DEDUP_DISTANCE_THRESHOLD:
                logger.info(
                    "Semantic Duplicate Detected (distance %.3f). Skipping: '%s'",
                    distance, article.title,
                )
                return
    except Exception as e:
        logger.warning("Deduplication check failed, proceeding with insertion: %s", e)

    # 2. Store the Parent Document
    logger.info(
        "Storing Parent Document: %s | '%s' | DateInt: %s | Impact: %s",
        parent_id, article.title, published_int, article.importance_score,
    )
    parent_meta = {
        "type": "parent",
        "title": article.title,
        "url": url,
        "source": article.source,
        "published": article.published,
        "published_int": published_int,
        "importance_score": article.importance_score,
        "key_insights": " | ".join(article.key_insights),
        "topics": ",".join(article.topics),
        **_topic_booleans(article.topics),
    }
    collection.upsert(
        ids=[parent_id],
        documents=[parent_text],
        embeddings=[parent_embedding],
        metadatas=[parent_meta],
    )

    # 3. Chunk the summary into child segments
    child_chunks = chunk_text(article.summary)
    if not child_chunks:
        child_chunks = [article.summary]

    logger.info("Split article into %d child chunks. Generating embeddings...", len(child_chunks))

    # 4. Explicit embeddings for all child chunks (ADR-8)
    child_embeddings = embed(child_chunks)

    # 5. Store the Child Documents — inherit published_int + topic booleans (ADR-4)
    child_ids = []
    child_metadatas = []
    topic_bools = _topic_booleans(article.topics)

    for idx, chunk in enumerate(child_chunks):
        child_ids.append(f"child_{generate_url_hash(url)}_chunk_{idx}")
        child_metadatas.append({
            "type": "child",
            "parent_id": parent_id,
            "source": article.source,
            "url": url,
            "published": article.published,
            "published_int": published_int,
            **topic_bools,
        })

    collection.upsert(
        ids=child_ids,
        documents=child_chunks,
        embeddings=child_embeddings,
        metadatas=child_metadatas,
    )
    logger.info("Successfully indexed parent %s and %d child chunks.", parent_id, len(child_ids))
