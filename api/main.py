import json
import logging
import os
import sys
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

# Dynamic path resolution to import from parent directory (project root)
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from query_engine import query_news_rag

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
            "GET /brief": "Retrieve the pre-compiled Daily Brief JSON"
        }
    }

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
    try:
        answer = await query_news_rag(query_str)
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
    uvicorn.run("main:app", host="0.0.0.0", port=8050, reload=True)
