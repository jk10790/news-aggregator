from pydantic import BaseModel, Field, HttpUrl
from typing import Optional

# =========================================================================
# Shared Data Contracts for the Aggregator Pipeline
# =========================================================================

class ArticleRaw(BaseModel):
    """
    Contract representing the raw, unprocessed article parsed from RSS feeds.
    Published by: ingestion/producer.py
    Consumed by: ingestion/consumer_triage.py
    """
    source: str = Field(description="Name of the source feed (e.g. techcrunch)")
    title: str = Field(description="The headline of the article")
    link: str = Field(description="The absolute source URL of the article")
    summary: str = Field(description="The body or summary snippet of the article")
    published: str = Field(description="The publication timestamp string")
    author: Optional[str] = Field(default="Unknown", description="Author name if available")

class ArticleVerified(ArticleRaw):
    """
    Contract representing a verified article that has passed the LLM triage check.
    Inherits all fields from ArticleRaw and adds the triage reasoning.
    Published by: ingestion/consumer_triage.py
    Consumed by: storage/consumer_storage.py
    """
    triage_reason: str = Field(description="LLM explanation of why this article is relevant")
    topics: list[str] = Field(default_factory=list, description="Categorized topics")
    entities: list[str] = Field(default_factory=list, description="Extracted entities")
    importance_score: int = Field(default=5, description="Global impact rating (1-10)")
    key_insights: list[str] = Field(default_factory=list, description="2 to 3 entity-dense bullet points summarizing the insights")
