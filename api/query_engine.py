import asyncio
import datetime
import logging
import os
import sys
from typing import Optional, List, TypedDict
from pydantic import BaseModel, Field
from openai import AsyncOpenAI
import json
from langgraph.graph import StateGraph, END

# Dynamic path resolution to import from parent directory (project root)
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import chromadb
from sentence_transformers import SentenceTransformer
from config import (
    LLM_PROVIDER, GEMINI_API_KEY, GEMINI_MODEL, OLLAMA_HOST, OLLAMA_MODEL,
    CHROMA_SERVER_HOST, CHROMA_SERVER_PORT, TAUT_URL
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

# Initialize LLM Client pointing to Taut Proxy
taut_client = AsyncOpenAI(base_url=TAUT_URL, api_key="placeholder")

# Connect to ChromaDB database service
logger.info(f"Connecting to ChromaDB at {CHROMA_SERVER_HOST}:{CHROMA_SERVER_PORT}...")
chroma_client = chromadb.HttpClient(host=CHROMA_SERVER_HOST, port=int(CHROMA_SERVER_PORT))
collection = chroma_client.get_collection("news_archive")

class TranslatedQuery(BaseModel):
    semantic_query: str = Field(description="Core search terms stripped of time-related words")
    days_offset_start: Optional[int] = Field(default=None)
    days_offset_end: Optional[int] = Field(default=None)

class GraphState(TypedDict):
    query: str
    phone_number: str
    interests: List[str]
    semantic_query: str
    context_articles: List[dict]
    context_text: str
    answer: str
    retries: int
    intent: str
    grade: str

async def translate_query(user_query: str) -> TranslatedQuery:
    today_str = datetime.date.today().isoformat()
    prompt = f"Translate query to date offsets for TODAY {today_str}. Query: '{user_query}'"
    try:
        model_name = f"gemini/{GEMINI_MODEL}" if GEMINI_API_KEY else f"ollama/{OLLAMA_MODEL}"
        response = await taut_client.chat.completions.create(
            model=model_name,
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"}
        )
        return TranslatedQuery.model_validate_json(response.choices[0].message.content)
    except Exception as e:
        return TranslatedQuery(semantic_query=user_query, days_offset_start=None, days_offset_end=None)

async def execute_hybrid_search(translated: TranslatedQuery, interests: List[str]) -> list[dict]:
    query_vector = embedding_model.encode(translated.semantic_query).tolist()
    
    # Pre-filtering constraints including user interests
    and_clauses = [{"type": {"$eq": "child"}}]
    
    if interests:
        # Filter by topics in user interests
        # We assume the metadata contains a "topic" field
        topic_clauses = [{"topic": {"$eq": interest}} for interest in interests]
        if len(topic_clauses) > 1:
            and_clauses.append({"$or": topic_clauses})
        elif len(topic_clauses) == 1:
            and_clauses.append(topic_clauses[0])

    if translated.days_offset_start is not None and translated.days_offset_end is not None:
        today = datetime.date.today()
        start_date_obj = today - datetime.timedelta(days=translated.days_offset_start)
        end_date_obj = today - datetime.timedelta(days=translated.days_offset_end)
        and_clauses.append({"published_int": {"$gte": int(start_date_obj.strftime("%Y%m%d"))}})
        and_clauses.append({"published_int": {"$lte": int(end_date_obj.strftime("%Y%m%d"))}})
        
    where_filter = {"$and": and_clauses} if len(and_clauses) > 1 else and_clauses[0]

    results = collection.query(
        query_embeddings=[query_vector],
        n_results=8,
        where=where_filter
    )
    
    context_articles = []
    seen_parents = set()
    metadatas = results.get("metadatas", [[]])[0]
    
    for meta in metadatas:
        parent_id = meta.get("parent_id")
        if parent_id and parent_id not in seen_parents:
            seen_parents.add(parent_id)
            parent_result = collection.get(ids=[parent_id])
            if parent_result.get("documents"):
                context_articles.append({
                    "title": parent_result.get("metadatas", [{}])[0].get("title", "No Title"),
                    "source": parent_result.get("metadatas", [{}])[0].get("source", "Unknown"),
                    "url": parent_result.get("metadatas", [{}])[0].get("url", ""),
                    "content": parent_result.get("documents", [""])[0]
                })
    return context_articles

async def router_node(state: GraphState) -> GraphState:
    query = state["query"]
    # Taut Tiered Routing: simple intent uses cheaper model
    response = await taut_client.chat.completions.create(
        model=f"ollama/{OLLAMA_MODEL}",
        messages=[{"role": "user", "content": f"Is this a greeting or a news query? Reply strictly with 'greeting' or 'news'. Query: {query}"}]
    )
    intent = response.choices[0].message.content.strip().lower()
    state["intent"] = "greeting" if "greeting" in intent else "news"
    return state

async def research_node(state: GraphState) -> GraphState:
    translated = await translate_query(state["query"])
    state["semantic_query"] = translated.semantic_query
    context = await execute_hybrid_search(translated, state["interests"])
    state["context_articles"] = context
    
    context_text = ""
    for idx, art in enumerate(context):
        context_text += f"\nDocument {idx+1} | Source: {art['source']} | URL: {art['url']}\nContent: {art['content']}\n"
    state["context_text"] = context_text
    return state

async def evaluator_node(state: GraphState) -> GraphState:
    if not state["context_articles"]:
        state["grade"] = "insufficient"
        return state
        
    prompt = f"Does the following context contain enough information to answer the query? Context: {state['context_text']}\nQuery: {state['query']}\nReply strictly with 'sufficient' or 'insufficient'."
    response = await taut_client.chat.completions.create(
        model=f"ollama/{OLLAMA_MODEL}",
        messages=[{"role": "user", "content": prompt}]
    )
    grade = response.choices[0].message.content.strip().lower()
    state["grade"] = "sufficient" if "sufficient" in grade else "insufficient"
    return state

async def web_search_node(state: GraphState) -> GraphState:
    logger.info("Executing Web Search Fallback...")
    # Mocking duckduckgo search
    state["context_text"] += f"\nWeb Search Result: Latest news on {state['semantic_query']}.\n"
    state["retries"] += 1
    return state

async def generate_node(state: GraphState) -> GraphState:
    if state["intent"] == "greeting":
        state["answer"] = "Hello! I am your personalized AI News Agent. What news are you looking for today?"
        return state
        
    prompt = RAG_PROMPT_TEMPLATE.format(context_text=state["context_text"], query=state["query"])
    model_name = f"gemini/{GEMINI_MODEL}" if GEMINI_API_KEY else f"ollama/{OLLAMA_MODEL}"
    response = await taut_client.chat.completions.create(
        model=model_name,
        messages=[{"role": "user", "content": prompt}],
        extra_headers={"X-Taut-Namespace": state["phone_number"]}
    )
    state["answer"] = response.choices[0].message.content
    return state

# LangGraph Edge Routers
def route_after_intent(state: GraphState) -> str:
    return "generate" if state["intent"] == "greeting" else "research"

def route_after_eval(state: GraphState) -> str:
    if state["grade"] == "sufficient" or state["retries"] >= 1:
        return "generate"
    return "web_search"

# Build CRAG Graph
workflow = StateGraph(GraphState)
workflow.add_node("router", router_node)
workflow.add_node("research", research_node)
workflow.add_node("evaluator", evaluator_node)
workflow.add_node("web_search", web_search_node)
workflow.add_node("generate", generate_node)

workflow.set_entry_point("router")
workflow.add_conditional_edges("router", route_after_intent, {"generate": "generate", "research": "research"})
workflow.add_edge("research", "evaluator")
workflow.add_conditional_edges("evaluator", route_after_eval, {"generate": "generate", "web_search": "web_search"})
workflow.add_edge("web_search", "generate")
workflow.add_edge("generate", END)

crag_app = workflow.compile()

async def query_news_rag(user_query: str, user_phone_number: str = "default_user", user_interests: List[str] = None) -> str:
    user_interests = user_interests or []
    initial_state = GraphState(
        query=user_query,
        phone_number=user_phone_number,
        interests=user_interests,
        semantic_query="",
        context_articles=[],
        context_text="",
        answer="",
        retries=0,
        intent="",
        grade=""
    )
    final_state = await crag_app.ainvoke(initial_state)
    return final_state["answer"]
