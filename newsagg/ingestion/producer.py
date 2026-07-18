import asyncio
import json
import logging
import html
import re
import feedparser
from bs4 import BeautifulSoup

from aiokafka import AIOKafkaProducer
from newsagg.config import RSS_FEEDS, REDPANDA_BROKER, TOPIC_RAW_ARTICLES
from newsagg.core.models import ArticleRaw

# Setup logging to print nicely in the console
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Boilerplate scrubber (Phase 4): drop sentences that are newsletter/marketing
# noise rather than actual article content.
BOILERPLATE_PATTERN = re.compile(
    r"(?i)(subscribe|sign up|newsletter|click here|read more|share (this|on)|follow us)"
)

async def fetch_and_parse_feed(feed_name: str, feed_url: str) -> list:
    """
    Asynchronously fetches and parses a single RSS feed.
    Uses asyncio.to_thread because feedparser is a synchronous blocking library.
    """
    logger.info(f"Fetching feed '{feed_name}' from {feed_url}...")
    try:
        # feedparser.parse performs blocking HTTP request; we run it in a separate thread
        # to prevent blocking the main asyncio event loop.
        parsed = await asyncio.to_thread(feedparser.parse, feed_url)
        
        articles = []
        for entry in parsed.entries[:15]:  # Limit to the 15 most recent articles per feed
            # Clean up the summary field (RSS summaries often contain HTML tags or raw entities)
            summary_raw = entry.get("summary", entry.get("description", ""))
            clean_summary = html.unescape(summary_raw)
            # Simple regex/replace to strip basic HTML tags if any exist
            clean_summary = BeautifulSoup(clean_summary, "html.parser").get_text(separator=" ").strip()

            # Split by basic sentence delimiters and drop boilerplate/marketing sentences
            sentences = [s.strip() for s in re.split(r'(?<=[.!?]) +', clean_summary) if s.strip()]
            clean_summary = " ".join(s for s in sentences if not BOILERPLATE_PATTERN.search(s))
            
            article = ArticleRaw(
                source=feed_name,
                title=entry.get("title", "No Title"),
                link=entry.get("link", ""),
                summary=clean_summary,
                published=entry.get("published", entry.get("updated", "Unknown Date")),
                author=entry.get("author", "Unknown Author")
            )
            articles.append(article)
            
        logger.info(f"Successfully parsed {len(articles)} articles from '{feed_name}'.")
        return articles
    except Exception as e:
        logger.error(f"Error parsing feed '{feed_name}': {str(e)}")
        return []

async def main():
    logger.info("Initializing Ingestion Producer...")
    
    # 1. Fetch all configured feeds concurrently
    tasks = [fetch_and_parse_feed(name, url) for name, url in RSS_FEEDS.items()]
    results = await asyncio.gather(*tasks)
    
    # Flatten the list of lists of articles
    all_articles = [article for sublist in results for article in sublist]
    logger.info(f"Total parsed articles across all feeds: {len(all_articles)}")
    
    if not all_articles:
        logger.warning("No articles fetched. Exiting.")
        return
    
    # 2. Connect to Redpanda
    logger.info(f"Connecting to Redpanda broker at {REDPANDA_BROKER}...")
    producer = AIOKafkaProducer(bootstrap_servers=REDPANDA_BROKER)
    await producer.start()
    
    try:
        # 3. Publish each article to the 'raw-articles' topic
        published_count = 0
        for article in all_articles:
            # Serialize the Pydantic model directly to JSON and encode to bytes
            serialized_article = article.model_dump_json().encode("utf-8")
            
            # Send message to Redpanda and wait for acknowledgment
            await producer.send_and_wait(TOPIC_RAW_ARTICLES, value=serialized_article)
            published_count += 1
            
        logger.info(f"Successfully published {published_count} raw articles to Redpanda topic '{TOPIC_RAW_ARTICLES}'.")
    except Exception as e:
        logger.error(f"Failed to publish events to Redpanda: {str(e)}")
    finally:
        # 4. Clean up connections
        await producer.stop()
        logger.info("Redpanda Producer connection closed cleanly.")

if __name__ == "__main__":
    # Run the async main loop
    asyncio.run(main())
