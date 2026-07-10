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
# 2. Application & LLM Environment Settings
# =========================================================================
LLM_PROVIDER = os.getenv("LLM_PROVIDER", "gemini").lower()
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")
OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://localhost:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3")

# Resolve provider-specific rate delay, retries, and backoff settings
if LLM_PROVIDER == "gemini":
    RATE_DELAY_SECONDS = float(os.getenv("GEMINI_RATE_DELAY", "13.0"))
    BACKOFF_BASE_SECONDS = 5.0
    MAX_RETRIES = 3
elif LLM_PROVIDER == "ollama":
    RATE_DELAY_SECONDS = float(os.getenv("OLLAMA_RATE_DELAY", "0.0"))
    BACKOFF_BASE_SECONDS = 1.0
    MAX_RETRIES = 2
else:
    RATE_DELAY_SECONDS = 2.0
    BACKOFF_BASE_SECONDS = 2.0
    MAX_RETRIES = 3

REDPANDA_BROKER = os.getenv("REDPANDA_BROKER", "localhost:9092")
CHROMA_SERVER_HOST = os.getenv("CHROMA_SERVER_HOST", "localhost")
CHROMA_SERVER_PORT = os.getenv("CHROMA_SERVER_PORT", "8002")
CHROMA_RETENTION_DAYS = int(os.getenv("CHROMA_RETENTION_DAYS", "7"))
TAUT_URL = os.getenv("TAUT_URL", "http://localhost:8000/v1")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/news_aggregator")
EMBEDDING_MODEL = "all-MiniLM-L6-v2"

# Kafka/Redpanda Topics we will use
TOPIC_RAW_ARTICLES = "raw-articles"
TOPIC_VERIFIED_ARTICLES = "verified-articles"

# Messaging Configuration
MESSAGING_PROVIDER = os.getenv("MESSAGING_PROVIDER", "twilio").lower()

# Twilio Configuration
TWILIO_ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID", "")
TWILIO_AUTH_TOKEN = os.getenv("TWILIO_AUTH_TOKEN", "")
TWILIO_WHATSAPP_SENDER = os.getenv("TWILIO_WHATSAPP_SENDER", "whatsapp:+14155238886")

# Telegram Configuration
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")

