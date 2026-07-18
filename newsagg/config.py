import os
from dotenv import load_dotenv

# Load environment variables from the root .env file
load_dotenv()

# =========================================================================
# 1. RSS Feeds Configuration
# =========================================================================
RSS_FEEDS = {
    # Tech News Aggregators & Publications
    "tech_meme": "https://www.techmeme.com/feed.xml",
    # "hacker_news": "https://news.ycombinator.com/rss",
    # "techcrunch": "https://techcrunch.com/feed/",
    # "the_verge": "https://www.theverge.com/tech/rss/index.xml",
    # "wired": "https://www.wired.com/feed/rss",
    # "ars_technica": "https://feeds.arstechnica.com/arstechnica/index",
    # "engadget": "https://www.engadget.com/rss.xml",
    # "venturebeat": "https://venturebeat.com/feed/",
    # "zdnet": "https://www.zdnet.com/news/rss.xml",
    # "techradar": "https://www.techradar.com/rss",
    "mit_tech_review": "https://www.technologyreview.com/feed/",

    # Artificial Intelligence & Data Science
    # "openai_blog": "https://openai.com/blog/rss.xml",
    # "kdnuggets": "https://www.kdnuggets.com/feed",
    # "microsoft_research": "https://www.microsoft.com/en-us/research/feed/",
    "google_feed": "https://news.google.com/rss/search?q=Artificial+Intelligence+OR+Machine+Learning",
    "arxiv_feed": "https://rss.arxiv.org/rss/cs.AI",

    #Sports
    "espn_feed": "https://www.espn.com/espn/rss/news",
    "yahoo_sports": "https://sports.yahoo.com/rss/",

    "google_news": "https://news.google.com/rss",
    # Developer Communities & Forums
    # "reddit_programming": "https://www.reddit.com/r/programming/.rss",
    # "dev_to": "https://dev.to/feed",
    # "stackoverflow_blog": "https://stackoverflow.blog/feed/",
    # "infoq": "https://feed.infoq.com/",

    # Engineering & Cloud Infrastructure
    # "aws_blog": "https://aws.amazon.com/blogs/aws/feed/",
    # "cloudflare_blog": "https://blog.cloudflare.com/rss/",
    # "github_blog": "https://github.blog/feed/",
    # "martin_fowler": "https://martinfowler.com/feed.atom",
    # "smashing_magazine": "https://www.smashingmagazine.com/feed/",

    # Startups & Venture Capital
    "yc_blog": "https://blog.ycombinator.com/feed/"
}

# =========================================================================
# 2. LLM gateway settings (ADR-3 — the actual client construction and all
#    retry/backoff/fallback logic live only in newsagg/core/llm.py)
# =========================================================================
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
TAUT_URL = os.getenv("TAUT_URL", "http://localhost:8000/v1")

# =========================================================================
# 3. Infrastructure endpoints
# =========================================================================
REDPANDA_BROKER = os.getenv("REDPANDA_BROKER", "localhost:9092")
CHROMA_SERVER_HOST = os.getenv("CHROMA_SERVER_HOST", "localhost")
CHROMA_SERVER_PORT = os.getenv("CHROMA_SERVER_PORT", "8002")
CHROMA_RETENTION_DAYS = int(os.getenv("CHROMA_RETENTION_DAYS", "7"))
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/news_aggregator")
EMBEDDING_MODEL = "all-MiniLM-L6-v2"

# Kafka/Redpanda Topics we will use
TOPIC_RAW_ARTICLES = "raw-articles"
TOPIC_VERIFIED_ARTICLES = "verified-articles"

# =========================================================================
# 4. Telegram (ADR-1/ADR-2 — the ONE product bot; no per-user bot tokens,
#    no Twilio/WhatsApp)
# =========================================================================
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_POLL_TIMEOUT = int(os.getenv("TELEGRAM_POLL_TIMEOUT", "50"))

# =========================================================================
# 5. Brief generation tuning (ADR-5 — topic-centric brief engine)
# =========================================================================
BRIEF_LOOKBACK_HOURS = int(os.getenv("BRIEF_LOOKBACK_HOURS", "24"))
TOPIC_MODULE_MAX_ARTICLES = int(os.getenv("TOPIC_MODULE_MAX_ARTICLES", "5"))
