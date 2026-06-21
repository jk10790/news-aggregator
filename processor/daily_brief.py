import asyncio
import datetime
import json
import logging
import os
import sys
from pydantic import BaseModel, Field

# Dynamic path resolution to import from parent directory (project root)
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from google import genai
from google.genai import types
import ollama
import chromadb
from config import (
    LLM_PROVIDER, GEMINI_API_KEY, GEMINI_MODEL, OLLAMA_HOST, OLLAMA_MODEL,
    CHROMA_SERVER_HOST, CHROMA_SERVER_PORT, RATE_DELAY_SECONDS, MAX_RETRIES, BACKOFF_BASE_SECONDS
)

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# =========================================================================
# 1. Define Pydantic Schema for Structured Output
# =========================================================================
class ArticleBrief(BaseModel):
    title: str = Field(description="The original headline of the article")
    url: str = Field(description="The source URL link of the article")
    key_insights: list[str] = Field(description="2 to 3 entity-dense bullet points summarizing the insights")

class CategoryBrief(BaseModel):
    name: str = Field(description="The category name (e.g. Distributed Systems, Generative AI, Software Engineering)")
    articles: list[ArticleBrief] = Field(description="List of articles under this category")

class DailyBrief(BaseModel):
    date: str = Field(description="The date of the briefing in YYYY-MM-DD format")
    headline_summary: str = Field(description="A single-sentence overarching overview of today's key news")
    categories: list[CategoryBrief] = Field(description="List of categorized insights")

# =========================================================================
# 2. Load isolated prompts
# =========================================================================
PROMPTS_DIR = os.path.join(os.path.dirname(__file__), "prompts")
MAP_PROMPT_PATH = os.path.join(PROMPTS_DIR, "map_prompt.txt")
REDUCE_PROMPT_PATH = os.path.join(PROMPTS_DIR, "reduce_prompt.txt")

try:
    with open(MAP_PROMPT_PATH, "r") as f:
        MAP_PROMPT_TEMPLATE = f.read()
    with open(REDUCE_PROMPT_PATH, "r") as f:
        REDUCE_PROMPT_TEMPLATE = f.read()
except FileNotFoundError as e:
    logger.error(f"Failed to load prompts: {str(e)}. Ensure prompt files exist.")
    exit(1)

# =========================================================================
# 3. Hybrid Client Initialization
# =========================================================================
# We initialize BOTH clients if configuration settings exist.
gemini_client = None
if GEMINI_API_KEY:
    logger.info("Initializing cloud Gemini Client for high-reasoning tasks...")
    gemini_client = genai.Client(api_key=GEMINI_API_KEY)

logger.info(f"Initializing local Ollama Client at {OLLAMA_HOST} for high-volume tasks...")
ollama_client = ollama.AsyncClient(host=OLLAMA_HOST)

# Dynamic task routing:
# We execute Map tasks locally (Ollama) to avoid cloud rate limits.
# We execute the single Reduce task on Gemini (if key is present) for schema enforcement.
MAP_LLM_PROVIDER = "ollama"
REDUCE_LLM_PROVIDER = "gemini" if GEMINI_API_KEY else "ollama"

logger.info(f"Hybrid Task Routing Active: Map={MAP_LLM_PROVIDER.upper()} | Reduce={REDUCE_LLM_PROVIDER.upper()}")

# =========================================================================
# 4. LLM Wrapper Functions
# =========================================================================
async def query_llm_map(articles_text: str) -> str:
    """Queries the configured Map LLM provider to extract bullet points from a batch."""
    prompt = MAP_PROMPT_TEMPLATE.format(articles_text=articles_text)
    
    for attempt in range(MAX_RETRIES):
        try:
            if MAP_LLM_PROVIDER == "gemini":
                response = await asyncio.to_thread(
                    gemini_client.models.generate_content,
                    model=GEMINI_MODEL,
                    contents=prompt
                )
                return response.text
            elif MAP_LLM_PROVIDER == "ollama":
                response = await ollama_client.generate(
                    model=OLLAMA_MODEL,
                    prompt=prompt
                )
                return response["response"]
        except Exception as e:
            delay = BACKOFF_BASE_SECONDS * (2 ** attempt)
            logger.warning(f"Map step failed: {str(e)}. Retrying in {delay}s...")
            await asyncio.sleep(delay)
            
    return "Error: Map step failed after multiple attempts."

async def query_llm_reduce(map_summaries: str) -> str:
    """Queries the configured Reduce LLM provider and enforces the DailyBrief JSON schema."""
    prompt = REDUCE_PROMPT_TEMPLATE.format(map_summaries=map_summaries)
    
    for attempt in range(MAX_RETRIES):
        try:
            if REDUCE_LLM_PROVIDER == "gemini":
                # Cloud Gemini with strict schema enforcement
                response = await asyncio.to_thread(
                    gemini_client.models.generate_content,
                    model=GEMINI_MODEL,
                    contents=prompt,
                    config=types.GenerateContentConfig(
                        response_mime_type="application/json",
                        response_schema=DailyBrief
                    )
                )
                return response.text
            elif REDUCE_LLM_PROVIDER == "ollama":
                # Local Ollama JSON mode
                response = await ollama_client.generate(
                    model=OLLAMA_MODEL,
                    prompt=prompt,
                    format="json"
                )
                return response["response"]
        except Exception as e:
            delay = BACKOFF_BASE_SECONDS * (2 ** attempt)
            logger.warning(f"Reduce step failed: {str(e)}. Retrying in {delay}s...")
            await asyncio.sleep(delay)
            
    raise RuntimeError("Reduce step failed after multiple attempts.")

# =========================================================================
# 5. Main Batch Runner
# =========================================================================
async def main():
    logger.info("Initializing Daily Brief Processor...")
    
    # Connect to ChromaDB database service
    logger.info(f"Connecting to ChromaDB at {CHROMA_SERVER_HOST}:{CHROMA_SERVER_PORT}...")
    chroma_client = chromadb.HttpClient(host=CHROMA_SERVER_HOST, port=int(CHROMA_SERVER_PORT))
    
    try:
        collection = chroma_client.get_collection("news_archive")
    except Exception as e:
        logger.error(f"Failed to find collection 'news_archive': {str(e)}. Run storage consumer first.")
        return
        
    # Query all parent articles from ChromaDB
    logger.info("Fetching verified parent documents from vector archive...")
    results = collection.get(where={"type": "parent"})
    
    documents = results.get("documents", [])
    metadatas = results.get("metadatas", [])
    
    if not documents:
        logger.warning("No parent articles found in the database. Exiting.")
        return
        
    logger.info(f"Retrieved {len(documents)} parent documents. Starting Map-Reduce...")
    
    # 1. Batching & the MAP Step
    batch_size = 5
    articles_data = []
    for doc, meta in zip(documents, metadatas):
        articles_data.append({
            "title": meta.get("title", "No Title"),
            "source": meta.get("source", "Unknown"),
            "url": meta.get("url", ""),
            "summary": doc
        })
        
    map_tasks = []
    for i in range(0, len(articles_data), batch_size):
        batch = articles_data[i:i+batch_size]
        batch_text = ""
        for idx, art in enumerate(batch):
            batch_text += f"\n--- Article {idx+1} ---\nTitle: {art['title']}\nSource URL: {art['url']}\nContent: {art['summary']}\n"
            
        logger.info(f"Queueing Map task for Batch {len(map_tasks)+1} ({len(batch)} articles)...")
        map_tasks.append(query_llm_map(batch_text))
        
    # Run all Map steps concurrently on Ollama (Fast & Free)
    map_summaries = await asyncio.gather(*map_tasks)
    logger.info(f"Completed {len(map_summaries)} Map summaries.")
    
    # 2. The REDUCE Step
    # Combine all map summaries into a single text block
    combined_map_summaries = "\n\n".join(map_summaries)
    
    logger.info("Executing REDUCE step (synthesizing and structuring daily brief)...")
    raw_json_output = await query_llm_reduce(combined_map_summaries)
    
    # 3. Validate JSON output using Pydantic schema
    try:
        daily_brief_data = DailyBrief.model_validate_json(raw_json_output)
        logger.info("Daily Brief JSON successfully validated against Pydantic schema.")
        
        # 4. Write output to daily_brief.json in project root
        output_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "daily_brief.json")
        with open(output_path, "w") as f:
            f.write(daily_brief_data.model_dump_json(indent=2))
            
        logger.info(f"🟢 Daily Brief successfully written to: {output_path}")
        
    except Exception as e:
        logger.error(f"Failed to validate JSON brief: {str(e)}.")
        logger.debug(f"Raw Output: {raw_json_output}")

if __name__ == "__main__":
    asyncio.run(main())
