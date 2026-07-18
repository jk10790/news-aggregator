import json
import logging
import os
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, Field

from newsagg.api.query_engine import query_news_rag, query_news_rag_stream
from newsagg.api.observer import observe_conversation
from newsagg.db.database import SessionLocal
from newsagg.db.schema import User, Interest

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
            "POST /webhook/telegram": "Telegram webhook (deploy-time alternative to long-polling)"
        }
    }

# NOTE: Twilio/WhatsApp webhook removed (ADR-1 — Telegram is the only
# delivery channel in v1; Twilio code paths deleted in Phase 0).

@app.post("/webhook/telegram")
async def telegram_webhook(request: Request):
    """
    Receives incoming messages from Telegram.
    Hydrates state from SQLite and triggers LangGraph CRAG.

    NOTE: this still hydrates/creates users keyed on the retired
    `User.phone_number` column. The full ADR-1/ADR-2 rewrite — routing
    through newsagg.bot.handlers.handle_update() and keying users on
    telegram_chat_id — is Phase 3 work.
    """
    import asyncio
    try:
        data = await request.json()
    except Exception:
        return {"status": "ok"}

    message_obj = data.get('message')
    if not message_obj or 'text' not in message_obj:
        return {"status": "ok"}

    incoming_msg = message_obj['text'].strip()
    sender_number = str(message_obj['chat']['id'])

    logger.info(f"Received Telegram message from {sender_number}: '{incoming_msg}'")

    asyncio.create_task(observe_conversation(sender_number, incoming_msg))

    db = SessionLocal()
    user_interests = []
    try:
        user = db.query(User).filter(User.phone_number == sender_number).first()
        if user:
            user_interests = [i.topic for i in user.interests]
        else:
            new_user = User(phone_number=sender_number)
            db.add(new_user)
            db.commit()
            default_interest = Interest(topic="Top News", user_id=new_user.id)
            db.add(default_interest)
            db.commit()
    finally:
        db.close()

    try:
        answer = await query_news_rag(incoming_msg, sender_number, user_interests)
    except Exception as e:
        logger.error(f"Error executing RAG query: {str(e)}")
        answer = "Sorry, I encountered an error processing your news request."

    from newsagg.config import TELEGRAM_BOT_TOKEN
    if TELEGRAM_BOT_TOKEN:
        import httpx
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {"chat_id": sender_number, "text": answer}
        logger.info(f"Sending to Telegram: {answer}")
        async def send_reply():
            async with httpx.AsyncClient() as client:
                await client.post(url, json=payload)
        asyncio.create_task(send_reply())

    return {"status": "ok"}

@app.post("/query")
async def handle_query(request: QueryRequest):
    """
    Submits a conversational query, runs semantic query translation,
    queries the vector store, and returns a grounded answer (streamed).
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
        from fastapi.responses import StreamingResponse
        return StreamingResponse(
            query_news_rag_stream(query_str, request.phone_number, user_interests),
            media_type="application/x-ndjson"
        )
    except Exception as e:
        logger.error(f"Error executing RAG query: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error executing query.")

@app.get("/brief")
def get_daily_brief():
    """Reads and returns the local daily_brief.json compiled by the consolidation engine.

    NOTE: Phase 6 replaces this with GET /brief/{chat_id} reading the
    Brief table in Postgres (ADR-7) — this file-based endpoint is
    retired then.
    """
    brief_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "daily_brief.json")

    if not os.path.exists(brief_path):
        raise HTTPException(
            status_code=404,
            detail="Daily Brief has not been generated yet."
        )

    try:
        with open(brief_path, "r") as f:
            data = json.load(f)
        return data
    except Exception as e:
        logger.error(f"Failed to read daily brief file: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to read the daily brief document.")


def main():
    """Entry point for the `newsagg-api` console script (wraps uvicorn.run)."""
    import uvicorn
    uvicorn.run("newsagg.api.main:app", host="0.0.0.0", port=8050, reload=True)


if __name__ == "__main__":
    main()
