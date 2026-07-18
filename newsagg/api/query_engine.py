"""Conversational RAG (CRAG) engine.

Corrective-RAG shape: route intent -> translate query -> vector retrieve ->
grade context -> (optional) web-search fallback -> generate. All LLM calls
go through newsagg.core.llm.complete (ADR-3); all embeddings go through
newsagg.core.embeddings (ADR-8). Retrieval filters on vector similarity +
published_int range + type ONLY — no user-interest filtering (ADR-12).

Nothing heavy (SentenceTransformer, Chroma client) is instantiated at
import time — this module must import cleanly with docker down.
"""
import datetime
import json
import logging
import os
from typing import Optional, List, TypedDict

from pydantic import BaseModel, Field
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver
from duckduckgo_search import DDGS

from newsagg.config import CHROMA_SERVER_HOST, CHROMA_SERVER_PORT
from newsagg.core.llm import complete
from newsagg.core.embeddings import embed

logger = logging.getLogger(__name__)

# Load RAG Prompt template
RAG_PROMPT_PATH = os.path.join(os.path.dirname(__file__), "prompts", "rag_prompt.txt")
try:
    with open(RAG_PROMPT_PATH, "r") as f:
        RAG_PROMPT_TEMPLATE = f.read()
except FileNotFoundError:
    logger.error("RAG prompt template file not found. Exiting.")
    raise

# =========================================================================
# Lazy Chroma accessor (ADR-8: never connect / never instantiate at import)
# =========================================================================
_chroma_client = None
_collection = None


def get_chroma_collection():
    global _chroma_client, _collection
    if _collection is None:
        import chromadb

        logger.info(f"Connecting to ChromaDB at {CHROMA_SERVER_HOST}:{CHROMA_SERVER_PORT}...")
        _chroma_client = chromadb.HttpClient(host=CHROMA_SERVER_HOST, port=int(CHROMA_SERVER_PORT))
        _collection = _chroma_client.get_or_create_collection("news_archive")
    return _collection


# =========================================================================
# Structured output schemas
# =========================================================================
class TranslatedQuery(BaseModel):
    semantic_query: str = Field(description="Core search terms stripped of time-related words")
    days_offset_start: Optional[int] = Field(default=None, description="Days before today the search window starts, or null for no date filter")
    days_offset_end: Optional[int] = Field(default=None, description="Days before today the search window ends, or null for no date filter")


class ContextGrade(BaseModel):
    score: float = Field(description="Score between 0.0 and 1.0 indicating if context is sufficient to answer the query")
    reasoning: str = Field(description="Brief explanation for the score")


SUFFICIENT_SCORE_THRESHOLD = 0.7


class GraphState(TypedDict):
    query: str
    chat_id: str
    semantic_query: str
    context_articles: List[dict]
    context_text: str
    answer: str
    retries: int
    intent: str
    grade: str


# =========================================================================
# Nodes
# =========================================================================
async def translate_query(user_query: str, chat_id: str) -> TranslatedQuery:
    today_str = datetime.date.today().isoformat()
    prompt = (
        f"Translate query to date offsets for TODAY {today_str}.\n"
        f"Query: '{user_query}'\n\n"
        "Example 1: Query: 'news about AI from last 3 days' -> "
        "semantic_query='AI', days_offset_start=3, days_offset_end=0\n"
        "Example 2: Query: 'what happened with SpaceX yesterday?' -> "
        "semantic_query='SpaceX', days_offset_start=1, days_offset_end=1\n"
        "Example 3: Query: 'tell me about the latest on Kubernetes' (no time reference) -> "
        "semantic_query='Kubernetes', days_offset_start=null, days_offset_end=null"
    )

    try:
        return await complete(
            tier="simple",
            system="You translate a user's news query into a semantic search term plus a date range.",
            user=prompt,
            response_model=TranslatedQuery,
            namespace=str(chat_id),
            context="translate",
        )
    except Exception as e:
        logger.error(f"Translation failed: {e}")
        return TranslatedQuery(semantic_query=user_query, days_offset_start=None, days_offset_end=None)


async def execute_hybrid_search(translated: TranslatedQuery) -> list[dict]:
    """Vector similarity + published_int range + type filter ONLY (ADR-12:
    no user-interest filtering in retrieval — the Observer's interest
    tracking is a separate concern from what gets retrieved here)."""
    query_vector = embed([translated.semantic_query])[0]

    and_clauses = [{"type": {"$eq": "child"}}]

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
        where=where_filter,
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
                    "content": parent_result.get("documents", [""])[0],
                })
    return context_articles


async def router_node(state: GraphState) -> GraphState:
    query = state["query"]
    prompt = f"Categorize this message as either 'GREETING' (e.g., hello, hi) or 'NEWS_QUERY' (asking for info). Reply strictly with exactly one word. Message: {query}"

    intent = await complete(
        tier="simple",
        system="You are a message router.",
        user=prompt,
        namespace=str(state["chat_id"]),
        context="router",
    )
    intent = intent.strip().lower()
    state["intent"] = "greeting" if "greeting" in intent else "news"
    return state


async def research_node(state: GraphState) -> GraphState:
    translated = await translate_query(state["query"], state["chat_id"])
    state["semantic_query"] = translated.semantic_query
    context = await execute_hybrid_search(translated)
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

    prompt = f"Does the following context contain enough information to answer the query? Context: {state['context_text']}\nQuery: {state['query']}"

    try:
        grade_result = await complete(
            tier="simple",
            system="You grade whether retrieved context is sufficient to answer a query.",
            user=prompt,
            response_model=ContextGrade,
            namespace=str(state["chat_id"]),
            context="evaluator",
        )
        state["grade"] = "sufficient" if grade_result.score >= SUFFICIENT_SCORE_THRESHOLD else "insufficient"
    except Exception as e:
        logger.error(f"Evaluator failed: {e}")
        state["grade"] = "insufficient"

    return state


async def web_search_node(state: GraphState) -> GraphState:
    logger.info("Executing Web Search Fallback...")
    try:
        results = DDGS().text(state["semantic_query"], backend="auto", max_results=3)
        search_text = "\n".join(
            f"Web Search Result: {r.get('title', '')} | URL: {r.get('href', '')}\nContent: {r.get('body', '')}"
            for r in results
        )
        state["context_text"] += f"\n{search_text}\n"
    except Exception as e:
        logger.error(f"Web search failed: {e}")
    state["retries"] += 1
    return state


async def generate_node(state: GraphState) -> GraphState:
    if state["intent"] == "greeting":
        state["answer"] = "Hello! I am your personalized AI News Agent. What news are you looking for today?"
        return state

    prompt = RAG_PROMPT_TEMPLATE.format(context_text=state["context_text"], query=state["query"])

    response = await complete(
        tier="complex",
        system="You are a factual, conversational AI news assistant.",
        user=prompt,
        namespace=str(state["chat_id"]),
        context="generate",
        stream=True,
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


def _initial_state(query: str, chat_id: str) -> GraphState:
    return GraphState(
        query=query,
        chat_id=str(chat_id),
        semantic_query="",
        context_articles=[],
        context_text="",
        answer="",
        retries=0,
        intent="",
        grade="",
    )


async def query_news_rag(query: str, chat_id: str) -> str:
    config = {"configurable": {"thread_id": str(chat_id)}}
    final_state = await crag_app.ainvoke(_initial_state(query, chat_id), config=config)
    return final_state["answer"]


async def query_news_rag_stream(query: str, chat_id: str):
    config = {"configurable": {"thread_id": str(chat_id)}}
    async for chunk in crag_app.astream(_initial_state(query, chat_id), config=config, stream_mode="updates"):
        yield json.dumps(chunk) + "\n"
