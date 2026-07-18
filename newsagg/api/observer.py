"""Observer agent: passively infers implicit topic interests from a user's
conversational messages and updates the interests table.

Extraction is constrained to the fixed taxonomy (newsagg.core.taxonomy) via
a Literal type on the structured-output schema — the LLM cannot invent
free-form topic strings. All LLM calls go through newsagg.core.llm.complete
(ADR-3).
"""
import datetime
import logging
from typing import List, Literal, Optional, TypedDict

from pydantic import BaseModel, Field
from langgraph.graph import StateGraph, END

from newsagg.db.database import SessionLocal
from newsagg.db.schema import User, Interest
from newsagg.core import taxonomy
from newsagg.core.llm import complete

logger = logging.getLogger(__name__)

CONFIDENCE_THRESHOLD = 0.9
ENGAGEMENT_BUMP = 0.1

# Slugs the Observer may assign — excludes the "top" pseudo-topic, which is
# an importance-based bucket, not a genuine interest a user can hold.
_TOPIC_SLUGS = tuple(t.slug for t in taxonomy.CLASSIFIABLE)
_TAXONOMY_HINT = ", ".join(f"{t.slug} ({t.label})" for t in taxonomy.CLASSIFIABLE)


# =========================================================================
# Observer Agent LangGraph State & Models
# =========================================================================
class ObserverState(TypedDict):
    chat_id: str
    message_history: List[str]
    topic: Optional[str]
    confidence: float


class InterestExtraction(BaseModel):
    topic: Optional[Literal[_TOPIC_SLUGS]] = Field(
        default=None,
        description="Single taxonomy topic slug implied by the user's latest message, or null if none applies.",
    )
    confidence: float = Field(default=0.0, description="Confidence 0.0-1.0 that this is a genuine interest.")


# =========================================================================
# Nodes
# =========================================================================
async def extract_interests_node(state: ObserverState) -> ObserverState:
    """Extracts at most one taxonomy-constrained topic from the latest message."""
    latest_msg = state["message_history"][-1]
    system_prompt = "You are an AI observing a user's conversational history to infer their genuine interests."
    user_prompt = f"""
Based on the user's latest message, decide if it implies interest in exactly one of these topics:
{_TAXONOMY_HINT}

Latest Message: "{latest_msg}"

Rules:
- Only pick a topic if the message shows genuine positive interest in it (asking about it, sharing enthusiasm, following up on it).
- If the user is complaining about, criticizing, or expressing negative/annoyed sentiment toward a topic, do NOT extract it — return topic: null.
- If no topic clearly applies, return topic: null and confidence: 0.0.
"""

    try:
        extraction = await complete(
            tier="simple",
            system=system_prompt,
            user=user_prompt,
            response_model=InterestExtraction,
            namespace=str(state["chat_id"]),
            context="extract",
        )
        state["topic"] = extraction.topic
        state["confidence"] = extraction.confidence
    except Exception as e:
        logger.warning(f"Interest extraction failed or produced an invalid topic: {e}")
        state["topic"] = None
        state["confidence"] = 0.0

    return state


async def update_db_node(state: ObserverState) -> ObserverState:
    """Persists the observed topic if confidence clears CONFIDENCE_THRESHOLD.

    New topic -> implicit Interest seeded at engagement_score=confidence.
    Existing topic (either source) -> last_interacted_at refreshed and
    engagement_score bumped by ENGAGEMENT_BUMP, capped at 1.0.
    """
    db = SessionLocal()
    try:
        user = db.query(User).filter(User.telegram_chat_id == str(state["chat_id"])).first()
        if not user:
            return state

        topic = state["topic"]
        existing = db.query(Interest).filter(Interest.user_id == user.id, Interest.topic == topic).first()
        if existing:
            existing.last_interacted_at = datetime.datetime.utcnow()
            existing.engagement_score = min(1.0, (existing.engagement_score or 0.0) + ENGAGEMENT_BUMP)
            logger.info(f"Observer refreshed existing interest '{topic}' for chat {state['chat_id']}")
        else:
            db.add(Interest(
                user_id=user.id,
                topic=topic,
                source="implicit",
                engagement_score=state["confidence"],
            ))
            logger.info(f"Observer added implicit interest '{topic}' (confidence={state['confidence']}) for chat {state['chat_id']}")
        db.commit()
    finally:
        db.close()
    return state


def route_after_extraction(state: ObserverState) -> str:
    if not state.get("topic") or state.get("confidence", 0.0) < CONFIDENCE_THRESHOLD:
        return "end"
    return "update"


# =========================================================================
# Graph Construction
# =========================================================================
workflow = StateGraph(ObserverState)
workflow.add_node("extract", extract_interests_node)
workflow.add_node("update", update_db_node)

workflow.set_entry_point("extract")
workflow.add_conditional_edges("extract", route_after_extraction, {"update": "update", "end": END})
workflow.add_edge("update", END)

observer_app = workflow.compile()


async def observe_conversation(chat_id: str, text: str):
    """Fire-and-forget observation workflow, keyed on the Telegram chat id."""
    initial_state = ObserverState(
        chat_id=str(chat_id),
        message_history=[text],
        topic=None,
        confidence=0.0,
    )
    await observer_app.ainvoke(initial_state)
