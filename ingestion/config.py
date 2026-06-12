import os
from dotenv import load_dotenv

# Load environment variables from the root .env file
load_dotenv()

# =========================================================================
# 1. RSS Feeds Configuration
# =========================================================================
RSS_FEEDS = {
    "hacker_news": "https://news.ycombinator.com/rss",
    "techcrunch": "https://techcrunch.com/feed/",
    "aws_blog": "https://aws.amazon.com/blogs/aws/feed/",
    "reddit_programming": "https://www.reddit.com/r/programming/.rss",
}

# =========================================================================
# 2. User Interest Profile
# =========================================================================
USER_INTERESTS = """
The user is highly interested in the following topics:
1. Distributed Systems: Event-driven architecture, message brokers (Kafka, Redpanda), system scalability, Docker, Kubernetes, and cloud infrastructure.
2. Generative AI & LLMs: Large Language Models, prompt engineering, RAG (Retrieval-Augmented Generation), vector databases (ChromaDB, Milvus, pgvector), local LLMs (Ollama), and agentic workflows.
3. Software Engineering: Python, async programming, API development, and software design principles.

The user is NOT interested in:
- General politics, economy, and financial markets (unless directly related to major tech companies).
- Sports, entertainment, celebrity gossip, and movies.
- Marketing, advertising strategies, and non-technical business news.
"""

# =========================================================================
# 3. Application & LLM Environment Settings
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

# Kafka/Redpanda Topics we will use
TOPIC_RAW_ARTICLES = "raw-articles"
TOPIC_VERIFIED_ARTICLES = "verified-articles"
