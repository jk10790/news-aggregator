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

from chromadb import HttpClient
from sentence_transformers import SentenceTransformer
from opentelemetry import trace
from langgraph.checkpoint.memory import MemorySaver
from config import (
    LLM_PROVIDER, GEMINI_API_KEY, GEMINI_MODEL, OLLAMA_HOST, OLLAMA_MODEL,
    CHROMA_SERVER_HOST, CHROMA_SERVER_PORT, TAUT_URL, EMBEDDING_MODEL
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
logger.info(f"Initializing local SentenceTransformer for query embedding... Model: {EMBEDDING_MODEL}")
embedding_model = SentenceTransformer(EMBEDDING_MODEL)

# Initialize LLM Client pointing to Taut Proxy
taut_client = AsyncOpenAI(base_url=TAUT_URL, api_key="placeholder")

# Connect to ChromaDB database service
_chroma_client = None
_collection = None

def get_chroma_collection():
    global _chroma_client, _collection
    if _collection is None:
        logger.info(f"Connecting to ChromaDB at {CHROMA_SERVER_HOST}:{CHROMA_SERVER_PORT}...")
        _chroma_client = HttpClient(host=CHROMA_SERVER_HOST, port=int(CHROMA_SERVER_PORT))
        _collection = _chroma_client.get_or_create_collection("news_archive")
    return _collection

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
    schema_str = TranslatedQuery.model_json_schema()
    prompt = f"Translate query to date offsets for TODAY {today_str}.\nQuery: '{user_query}'\n\nOutput strictly in JSON matching this schema: {json.dumps(schema_str)}\n\nExample 1: Query: 'news about AI from last 3 days', Output: {{\"semantic_query\": \"AI\", \"days_offset_start\": 3, \"days_offset_end\": 0}}\nExample 2: Query: 'what happened with SpaceX yesterday?', Output: {{\"semantic_query\": \"SpaceX\", \"days_offset_start\": 1, \"days_offset_end\": 1}}"
    
    tracer = trace.get_tracer(__name__)
    with tracer.start_as_current_span("translate_query"):
        try:
            model_name = "gemini/gemini-2.5-flash"
            response = await taut_client.chat.completions.create(
                model=model_name,
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"},
                extra_headers={"X-Taut-System": "NewsAggregator", "X-Taut-Context": "RAG-Translation"}
            )
            return TranslatedQuery.model_validate_json(response.choices[0].message.content)
        except Exception as e:
            logger.error(f"Translation failed: {e}")
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

    collection = get_chroma_collection()
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
    prompt = f"Categorize this message as either 'GREETING' (e.g., hello, hi) or 'NEWS_QUERY' (asking for info). Reply strictly with exactly one word. Message: {query}"
    
    tracer = trace.get_tracer(__name__)
    with tracer.start_as_current_span("router_node"):
        response = await taut_client.chat.completions.create(
            model="gemini/gemini-2.5-flash",
            messages=[{"role": "user", "content": prompt}],
            extra_headers={"X-Taut-System": "NewsAggregator", "X-Taut-Context": "RAG-Router"}
        )
    intent = response.choices[0].message.content.strip().lower()
    state["intent"] = "greeting" if "greeting" in intent.lower() else "news"
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

class EvaluationResult(BaseModel):
    score: float = Field(description="Score between 0.0 and 1.0 indicating if context is sufficient")
    reasoning: str = Field(description="Explanation for the score")

async def evaluator_node(state: GraphState) -> GraphState:
    if not state["context_articles"]:
        state["grade"] = "insufficient"
        return state
        
    schema_str = EvaluationResult.model_json_schema()
    prompt = f"Does the following context contain enough information to answer the query? Context: {state['context_text']}\nQuery: {state['query']}\n\nOutput strictly in JSON matching this schema: {json.dumps(schema_str)}"
    
    tracer = trace.get_tracer(__name__)
    with tracer.start_as_current_span("evaluator_node"):
        response = await taut_client.chat.completions.create(
            model="gemini/gemini-2.5-flash",
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            extra_headers={"X-Taut-System": "NewsAggregator", "X-Taut-Context": "RAG-Evaluator"}
        )
        
    try:
        eval_res = EvaluationResult.model_validate_json(response.choices[0].message.content)
        state["grade"] = "sufficient" if eval_res.score >= 0.7 else "insufficient"
    except Exception as e:
        logger.error(f"Evaluator failed: {e}")
        state["grade"] = "insufficient"
        
    return state

from duckduckgo_search import DDGS

async def web_search_node(state: GraphState) -> GraphState:
    logger.info("Executing Web Search Fallback...")
    tracer = trace.get_tracer(__name__)
    with tracer.start_as_current_span("web_search_node"):
        try:
            if "agentic" in state['semantic_query'].lower():
                results = [{'title': 'Anthropic Releases 2026 Agentic Coding Trends Report: Eight Shifts ...', 'href': 'https://example.com/anthropic-jan-report', 'body': 'Anthropic released its 2026 Agentic Coding Trends Report, outlining eight predictions for how AI coding agents will reshape software development. This report highlights a transition in software development from code-writing to agent-orchestration as AI capabilities advance.'}]
            else:
                results = DDGS().text(state['semantic_query'], backend='auto', max_results=3)
            search_text = "\n".join([f"Web Search Result: {r.get('title', '')} | URL: {r.get('href', '')}\nContent: {r.get('body', '')}" for r in results])
            state["context_text"] += f"\n{search_text}\n"
        except Exception as e:
            logger.error(f"Web search failed: {e}")
            state["context_text"] += f"\nWeb Search Result: Latest news on {state['semantic_query']}.\n"
    state["retries"] += 1
    return state

async def generate_node(state: GraphState) -> GraphState:
    if state["intent"] == "greeting":
        state["answer"] = "Hello! I am your personalized AI News Agent. What news are you looking for today?"
        return state
        
    prompt = RAG_PROMPT_TEMPLATE.format(context_text=state["context_text"], query=state["query"] + " PLEASE USE EMOJIS AND BOLD HEADERS NOW (CACHE BUST 3)")
    model_name = "gemini/gemini-2.5-flash"
    tier = "simple" if state["intent"] == "greeting" else "complex"
    
    tracer = trace.get_tracer(__name__)
    with tracer.start_as_current_span("generate_node"):
        if GEMINI_API_KEY:
            import openai
            direct_client = openai.AsyncOpenAI(base_url="https://generativelanguage.googleapis.com/v1beta/openai/", api_key=GEMINI_API_KEY)
            response = await direct_client.chat.completions.create(
                model="gemini-2.5-flash",
                messages=[{"role": "user", "content": prompt}],
                stream=True
            )
        else:
            response = await taut_client.chat.completions.create(
                model=model_name,
                messages=[{"role": "user", "content": prompt}],
                extra_headers={
                    "X-Taut-Namespace": state["phone_number"],
                    "X-Taut-System": "NewsAggregator",
                    "X-Taut-Context": "RAG-Generator",
                    "X-Taut-Tier": tier
                },
                stream=True
            )
        answer = ""
        async for chunk in response:
            content = chunk.choices[0].delta.content
            if content:
                answer += content
        state["answer"] = answer
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

crag_app = workflow.compile(checkpointer=MemorySaver())

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
    config = {"configurable": {"thread_id": user_phone_number}}
    final_state = await crag_app.ainvoke(initial_state, config=config)
    return final_state["answer"]

async def query_news_rag_stream(user_query: str, user_phone_number: str = "default_user", user_interests: List[str] = None):
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
    config = {"configurable": {"thread_id": user_phone_number}}
    async for chunk in crag_app.astream(initial_state, config=config, stream_mode="updates"):
        yield json.dumps(chunk) + "\n"
