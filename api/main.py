import json
import logging
import os
import sys
from fastapi import FastAPI, HTTPException, Request, Form
from pydantic import BaseModel, Field
from typing import Optional

# Dynamic path resolution to import from parent directory (project root)
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from api.query_engine import query_news_rag
from api.observer import observe_conversation
from database import SessionLocal, User, Interest

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Initialize FastAPI App
app = FastAPI(
    title="Personalized AI News API",
    description="Conversational discovery and RAG queries over your ingested news archive.",
    version="1.0.0"
)

# =========================================================================
# Request & Response Schemas
# =========================================================================
class QueryRequest(BaseModel):
    query: str = Field(description="The natural language question or search criteria.")
    phone_number: str = Field(default="system", description="The tenant ID for routing and caching.")

class QueryResponse(BaseModel):
    answer: str = Field(description="The fact-grounded, cited answer from the LLM.")

# =========================================================================
# API Endpoint Routers
# =========================================================================
@app.get("/")
def read_root():
    """Root endpoint to check API service health status."""
    return {
        "status": "healthy",
        "service": "Personalized AI News Agent API",
        "endpoints": {
            "POST /query": "Submit natural language queries",
            "GET /brief": "Retrieve the pre-compiled Daily Brief JSON",
            "POST /webhook/twilio": "Twilio WhatsApp Webhook"
        }
    }

@app.post("/webhook/twilio")
async def twilio_webhook(request: Request):
    """
    Receives incoming WhatsApp messages from Twilio.
    Hydrates state from SQLite and triggers LangGraph CRAG.
    """
    import asyncio
    form_data = await request.form()
    incoming_msg = form_data.get('Body', '').strip()
    sender_number = form_data.get('From', '').replace('whatsapp:', '')
    
    if not incoming_msg or not sender_number:
        return "<Response></Response>" # Empty valid TwiML
        
    logger.info(f"Received WhatsApp message from {sender_number}: '{incoming_msg}'")
    
    # Trigger background observer agent to extract zero-shot interests
    asyncio.create_task(observe_conversation(sender_number, incoming_msg))
    
    # 1. State Hydration from SQLite
    db = SessionLocal()
    user_interests = []
    try:
        user = db.query(User).filter(User.phone_number == sender_number).first()
        if user:
            user_interests = [i.topic for i in user.interests]
        else:
            # Auto-register new user
            new_user = User(phone_number=sender_number)
            db.add(new_user)
            db.commit()
    finally:
        db.close()
        
    # 2. Execute RAG / LangGraph
    try:
        # Pass interests to the query engine for metadata filtering
        answer = await query_news_rag(incoming_msg, sender_number, user_interests)
    except Exception as e:
        logger.error(f"Error executing RAG query: {str(e)}")
        answer = "Sorry, I encountered an error processing your news request."

    # 3. Return TwiML response
    from twilio.twiml.messaging_response import MessagingResponse
    resp = MessagingResponse()
    resp.message(answer)
    return str(resp)

@app.post("/query", response_model=QueryResponse)
async def handle_query(request: QueryRequest):
    """
    Submits a conversational query, runs semantic query translation,
    queries the vector store, and returns a grounded answer.
    """
    query_str = request.query.strip()
    if not query_str:
        raise HTTPException(status_code=400, detail="Query string cannot be empty.")
        
    logger.info(f"Received query request: '{query_str}'")
    
    db = SessionLocal()
    user_interests = []
    try:
        user = db.query(User).filter(User.phone_number == request.phone_number).first()
        if user:
            user_interests = [i.topic for i in user.interests]
    finally:
        db.close()
        
    try:
        answer = await query_news_rag(query_str, request.phone_number, user_interests)
        return QueryResponse(answer=answer)
    except Exception as e:
        logger.error(f"Error executing RAG query: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error executing query.")

@app.get("/brief")
def get_daily_brief():
    """Reads and returns the local daily_brief.json compiled by the consolidation engine."""
    brief_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "daily_brief.json")
    
    if not os.path.exists(brief_path):
        raise HTTPException(
            status_code=404, 
            detail="Daily Brief has not been generated yet. Run processor/daily_brief.py first."
        )
        
    try:
        with open(brief_path, "r") as f:
            data = json.load(f)
        return data
    except Exception as e:
        logger.error(f"Failed to read daily brief file: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to read the daily brief document.")

if __name__ == "__main__":
    import uvicorn
    # Start the async ASGI web server on port 8050
    uvicorn.run("api.main:app", host="0.0.0.0", port=8050, reload=True)

