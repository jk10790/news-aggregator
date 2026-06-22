import asyncio
import datetime
import logging
import os
import sys
from typing import Optional
from pydantic import BaseModel, Field

# Dynamic path resolution to import from parent directory (project root)
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from google import genai
from google.genai import types
import ollama
import chromadb
from sentence_transformers import SentenceTransformer
from config import (
    LLM_PROVIDER, GEMINI_API_KEY, GEMINI_MODEL, OLLAMA_HOST, OLLAMA_MODEL,
    CHROMA_SERVER_HOST, CHROMA_SERVER_PORT
)

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Load RAG Prompt template
RAG_PROMPT_PATH = os.path.join(os.path.dirname(__file__), "prompts", "rag_prompt.txt")
try:
    with open(RAG_PROMPT_PATH, "r") as f:
        RAG_PROMPT_TEMPLATE = f.read()
except FileNotFoundError:
    logger.error("RAG prompt template file not found. Exiting.")
    exit(1)

# Initialize Embedding Model (locally on CPU)
logger.info("Initializing local SentenceTransformer for query embedding...")
embedding_model = SentenceTransformer("all-MiniLM-L6-v2")

# Initialize LLM Clients
gemini_client = None
if GEMINI_API_KEY:
    gemini_client = genai.Client(api_key=GEMINI_API_KEY)
ollama_client = ollama.AsyncClient(host=OLLAMA_HOST)

# Connect to ChromaDB database service
logger.info(f"Connecting to ChromaDB at {CHROMA_SERVER_HOST}:{CHROMA_SERVER_PORT}...")
chroma_client = chromadb.HttpClient(host=CHROMA_SERVER_HOST, port=int(CHROMA_SERVER_PORT))
collection = chroma_client.get_collection("news_archive")

# =========================================================================
# 1. Define Query Translation Schema
# =========================================================================
class TranslatedQuery(BaseModel):
    semantic_query: str = Field(description="Core search terms stripped of time-related words (e.g. 'ClickHouse scalability')")
    days_offset_start: Optional[int] = Field(default=None, description="Days ago start bounds. Today is 0, yesterday is 1, last week is 7. If no time bounding is requested or implied, set to null.")
    days_offset_end: Optional[int] = Field(default=None, description="Days ago end bounds. Today is 0, yesterday is 1, last week is 7. If no time bounding is requested or implied, set to null.")

# =========================================================================
# 2. Query Translation Logic
# =========================================================================
async def translate_query(user_query: str) -> TranslatedQuery:
    """
    Uses the active LLM provider to translate relative time queries into structured offsets.
    E.g. "What happened yesterday about AI?" -> semantic: "AI", start: 1, end: 1
    """
    today_str = datetime.date.today().isoformat()
    prompt = f"""
You are a database query translation assistant. Your job is to parse relative date bounds from user questions and translate them into offset days relative to TODAY.

TODAY'S DATE IS: {today_str}

### TRANSLATION RULES:
* If the user refers to "yesterday", set days_offset_start=1, days_offset_end=1.
* If the user refers to "today" or "now", set days_offset_start=0, days_offset_end=0.
* If the user refers to "last week" or "this week", set days_offset_start=7, days_offset_end=0.
* If the user does not specify or imply any time bounding (e.g., "what is clickhouse", "tell me about local LLMs"), set days_offset_start=null and days_offset_end=null.

Respond strictly in JSON matching the required schema.

### USER QUERY:
"{user_query}"
"""
    try:
        if GEMINI_API_KEY:
            # High-reasoning structured translation via Gemini
            response = await asyncio.to_thread(
                gemini_client.models.generate_content,
                model=GEMINI_MODEL,
                contents=prompt,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json",
                    response_schema=TranslatedQuery
                )
            )
            return TranslatedQuery.model_validate_json(response.text)
        else:
            # Fallback to Ollama JSON mode
            response = await ollama_client.generate(
                model=OLLAMA_MODEL,
                prompt=prompt,
                format="json"
            )
            return TranslatedQuery.model_validate_json(response["response"])
    except Exception as e:
        logger.warning(f"Query translation failed: {str(e)}. Defaulting to full database search.")
        return TranslatedQuery(semantic_query=user_query, days_offset_start=None, days_offset_end=None)

# =========================================================================
# 3. Hybrid Search Execution
# =========================================================================
async def execute_hybrid_search(translated: TranslatedQuery) -> list[dict]:
    """
    Executes a hybrid search query inside ChromaDB:
    - Calculates absolute YYYYMMDD integer date limits if bounds are specified.
    - Generates semantic vectors locally.
    - Queries ChromaDB with optional metadata pre-filtering.
    """
    # 1. Generate query embedding locally on CPU
    query_vector = embedding_model.encode(translated.semantic_query).tolist()
    
    # 2. Build ChromaDB pre-filtering constraints
    if translated.days_offset_start is not None and translated.days_offset_end is not None:
        today = datetime.date.today()
        start_date_obj = today - datetime.timedelta(days=translated.days_offset_start)
        end_date_obj = today - datetime.timedelta(days=translated.days_offset_end)
        
        start_date_int = int(start_date_obj.strftime("%Y%m%d"))
        end_date_int = int(end_date_obj.strftime("%Y%m%d"))
        
        logger.info(f"Hybrid Search Bounds: Start={start_date_int} | End={end_date_int} | Query='{translated.semantic_query}'")
        
        where_filter = {
            "$and": [
                {"type": {"$eq": "child"}},
                {"published_int": {"$gte": start_date_int}},
                {"published_int": {"$lte": end_date_int}}
            ]
        }
    else:
        logger.info(f"Hybrid Search without date filter. Query='{translated.semantic_query}'")
        where_filter = {
            "type": {"$eq": "child"}
        }
    
    # Query ChromaDB (retrieve top 8 child chunks)
    results = collection.query(
        query_embeddings=[query_vector],
        n_results=8,
        where=where_filter
    )

    
    # 3. Resolve Parent Context
    # Querying child chunks gives us high semantic precision. Now, we resolve their parent IDs
    # to feed the full text context to the LLM.
    context_articles = []
    seen_parents = set()
    
    metadatas = results.get("metadatas", [[]])[0]
    
    for meta in metadatas:
        parent_id = meta.get("parent_id")
        if parent_id and parent_id not in seen_parents:
            seen_parents.add(parent_id)
            
            # Retrieve the full Parent Document from ChromaDB
            parent_result = collection.get(ids=[parent_id])
            parent_docs = parent_result.get("documents", [])
            parent_metas = parent_result.get("metadatas", [])
            
            if parent_docs:
                context_articles.append({
                    "title": parent_metas[0].get("title", "No Title"),
                    "source": parent_metas[0].get("source", "Unknown"),
                    "url": parent_metas[0].get("url", ""),
                    "content": parent_docs[0]
                })
                
    return context_articles

# =========================================================================
# 4. RAG Execution Loop
# =========================================================================
async def query_news_rag(user_query: str) -> str:
    """
    RAG coordinator:
    1. Translates the query.
    2. Executes hybrid retrieval.
    3. Triggers grounded LLM generation.
    """
    # 1. Translate Query
    translated = await translate_query(user_query)
    
    # 2. Retrieve Parent Context
    context_articles = await execute_hybrid_search(translated)
    
    if not context_articles:
        return "I do not have this information in my ingested feeds."
        
    # 3. Format context string
    context_text = ""
    for idx, art in enumerate(context_articles):
        context_text += f"\nDocument {idx+1} | Source: {art['source']} | URL: {art['url']}\nContent: {art['content']}\n"
        
    # 4. Build RAG prompt
    prompt = RAG_PROMPT_TEMPLATE.format(
        context_text=context_text,
        query=user_query
    )
    
    # 5. Query LLM to generate answer
    try:
        if GEMINI_API_KEY:
            # We use Gemini for conversation since it has higher reasoning & citation adherence
            response = await asyncio.to_thread(
                gemini_client.models.generate_content,
                model=GEMINI_MODEL,
                contents=prompt
            )
            return response.text
        else:
            # Fallback to local Ollama
            response = await ollama_client.generate(
                model=OLLAMA_MODEL,
                prompt=prompt
            )
            return response["response"]
    except Exception as e:
        logger.error(f"RAG query generation failed: {str(e)}")
        return "Error: Failed to generate response from LLM."
