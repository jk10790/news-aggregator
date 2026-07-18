import logging
import os
import sys
from typing import List, TypedDict
from pydantic import BaseModel, Field
from langgraph.graph import StateGraph, END
import json
from openai import AsyncOpenAI

# Dynamic path resolution to import from parent directory (project root)
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import SessionLocal, User, Interest
from config import OLLAMA_MODEL, TAUT_URL

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Initialize LLM Client pointing to Taut Proxy
taut_client = AsyncOpenAI(base_url=TAUT_URL, api_key="placeholder")

# =========================================================================
# Observer Agent LangGraph State & Models
# =========================================================================
class ObserverState(TypedDict):
    phone_number: str
    message_history: List[str]
    current_interests: List[str]
    proposed_interests: List[str]
    confidence_scores: List[float]

class InterestExtraction(BaseModel):
    topics: List[str] = Field(description="Implied interests extracted from the user's latest message")
    confidence: List[float] = Field(description="Confidence score for each topic (0.0 to 1.0)")

# =========================================================================
# Nodes
# =========================================================================
async def extract_interests_node(state: ObserverState) -> ObserverState:
    """Extracts potential interests from the latest message."""
    latest_msg = state["message_history"][-1]
    prompt = f"""
    You are an AI observing a user's conversational history.
    Based on their latest message, extract any implied topics of interest.
    CRITICAL: You must explicitly calculate intent and sentiment negative constraints.
    Distinguish between inquiry/curiosity ("Tell me more about X") and rejection/fatigue ("I don't care about Y", "stop showing me X").
    ONLY extract topics the user has a POSITIVE sentiment or active curiosity towards.
    If the user expresses fatigue or rejection of a topic (e.g., "I am sick of hearing about Elon Musk"), do NOT include it, or give it a 0.0 confidence score.
    
    Current Interests: {state['current_interests']}
    Latest Message: "{latest_msg}"
    
    Output strictly in JSON. You must return a JSON object with exactly two keys: "topics" (a list of strings) and "confidence" (a list of floats between 0.0 and 1.0). Only include topics with positive sentiment. Example: {{"topics": ["Aerospace"], "confidence": [0.95]}}
    """
    
    try:
        response = await taut_client.chat.completions.create(
            model=f"ollama/{OLLAMA_MODEL}",
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            extra_headers={"X-Taut-System": "NewsAggregator", "X-Taut-Context": "Observer"}
        )
        extraction = InterestExtraction.model_validate_json(response.choices[0].message.content)
        state["proposed_interests"] = extraction.topics
        state["confidence_scores"] = extraction.confidence
    except Exception as e:
        logger.error(f"Interest extraction failed: {e}")
        state["proposed_interests"] = []
        state["confidence_scores"] = []
        
    return state

async def update_db_node(state: ObserverState) -> ObserverState:
    """Updates the SQLite DB if confidence threshold > 0.90."""
    import datetime
    db = SessionLocal()
    try:
        user = db.query(User).filter(User.phone_number == state["phone_number"]).first()
        if user:
            for topic, conf in zip(state["proposed_interests"], state["confidence_scores"]):
                logger.info(f"Observer evaluated topic '{topic}' with confidence {conf}")
                if conf >= 0.90:
                    if topic not in state["current_interests"]:
                        logger.info(f"Observer Agent adding new interest '{topic}' (Confidence: {conf}) to user {state['phone_number']}")
                        new_interest = Interest(topic=topic, user_id=user.id)
                        db.add(new_interest)
                    else:
                        existing = db.query(Interest).filter(Interest.user_id == user.id, Interest.topic == topic).first()
                        if existing:
                            existing.last_interacted_at = datetime.datetime.utcnow()
            db.commit()
    finally:
        db.close()
    return state

def route_after_extraction(state: ObserverState) -> str:
    if not state["proposed_interests"]:
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

async def observe_conversation(phone_number: str, incoming_msg: str):
    """
    Fire-and-forget observation workflow.
    """
    db = SessionLocal()
    current_interests = []
    try:
        user = db.query(User).filter(User.phone_number == phone_number).first()
        if user:
            current_interests = [i.topic for i in user.interests]
    finally:
        db.close()
        
    initial_state = ObserverState(
        phone_number=phone_number,
        message_history=[incoming_msg],
        current_interests=current_interests,
        proposed_interests=[],
        confidence_scores=[]
    )
    
    # Run the observer flow
    await observer_app.ainvoke(initial_state)
