"""FastAPI app (Phase 3 rewrite).

Endpoints:
    POST /query             NDJSON-streamed CRAG answer (unchanged behavior,
                             minus the retired phone_number/interest lookup)
    GET  /brief/{chat_id}    latest Brief row for the user with that
                             telegram_chat_id (ADR-7 — briefs live in
                             Postgres, not JSON files)
    POST /webhook/telegram   deploy-time alternative to long-polling;
                             delegates to newsagg.bot.handlers.handle_update
                             (ADR-2 — same handlers, different transport)
    GET  /health             liveness probe
"""
import logging

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from newsagg import config
from newsagg.api.query_engine import query_news_rag_stream
from newsagg.bot import handlers
from newsagg.bot.telegram_api import TelegramAPI
from newsagg.db.database import SessionLocal
from newsagg.db.schema import Brief, User

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Personalized AI News API",
    description="Conversational discovery and RAG queries over your ingested news archive.",
    version="1.0.0",
)


# =========================================================================
# Request schema
# =========================================================================
class QueryRequest(BaseModel):
    query: str = Field(description="The natural language question or search criteria.")
    chat_id: str = Field(
        default="system",
        description="Routing/session id for RAG memory (e.g. a Telegram chat id).",
    )


# =========================================================================
# Telegram webhook (deploy-time alternative to bot/poller.py long-polling)
# =========================================================================
_telegram_api: TelegramAPI | None = None


def _get_telegram_api() -> TelegramAPI:
    global _telegram_api
    if _telegram_api is None:
        _telegram_api = TelegramAPI(config.TELEGRAM_BOT_TOKEN)
    return _telegram_api


@app.post("/webhook/telegram")
async def telegram_webhook(request: Request):
    """Same handlers as the long-poll loop, just a different transport."""
    update = await request.json()
    await handlers.handle_update(_get_telegram_api(), update)
    return {"ok": True}


# =========================================================================
# Conversational RAG query (NDJSON stream)
# =========================================================================
@app.post("/query")
async def handle_query(request: QueryRequest):
    """Runs the CRAG graph and streams the answer as NDJSON chunks.

    ADR-12: retrieval uses vector similarity + date/type filters only —
    no interest-based filtering, so there is no per-request DB lookup
    here anymore (the old phone_number-keyed interest fetch is gone).
    """
    query_str = request.query.strip()
    if not query_str:
        raise HTTPException(status_code=400, detail="Query string cannot be empty.")

    logger.info("Received query request: '%s'", query_str)

    try:
        return StreamingResponse(
            query_news_rag_stream(query_str, request.chat_id),
            media_type="application/x-ndjson",
        )
    except Exception as e:
        logger.error("Error executing RAG query: %s", e)
        raise HTTPException(status_code=500, detail="Internal server error executing query.")


# =========================================================================
# Brief lookup (ADR-7 — Postgres, not JSON files)
# =========================================================================
@app.get("/brief/{chat_id}")
def get_brief(chat_id: str):
    db = SessionLocal()
    try:
        user = db.query(User).filter(User.telegram_chat_id == chat_id).first()
        if user is None:
            raise HTTPException(status_code=404, detail="No user with that chat id.")

        brief = (
            db.query(Brief)
            .filter(Brief.user_id == user.id)
            .order_by(Brief.brief_date.desc())
            .first()
        )
        if brief is None:
            raise HTTPException(
                status_code=404, detail="No brief has been generated for this user yet."
            )

        return {
            "chat_id": chat_id,
            "brief_date": brief.brief_date.isoformat(),
            "content": brief.content,
            "delivered_at": brief.delivered_at.isoformat() if brief.delivered_at else None,
        }
    finally:
        db.close()


# =========================================================================
# Health
# =========================================================================
@app.get("/health")
def health():
    return {"status": "ok"}


def main():
    """Entry point for the `newsagg-api` console script (wraps uvicorn.run)."""
    import uvicorn

    uvicorn.run("newsagg.api.main:app", host="0.0.0.0", port=8050, reload=True)


if __name__ == "__main__":
    main()
