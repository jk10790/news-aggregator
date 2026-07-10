import pytest
import asyncio
import os
import sys
import json
from unittest.mock import patch, MagicMock
from fastapi.testclient import TestClient

# Adjust path so we can import our application modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Set a dummy DB URL for import, though we will mock it
os.environ["DATABASE_URL"] = "postgresql://postgres:postgres@localhost:5432/test_db"

from api.main import app
from database import User, Interest
from processor.daily_brief import DailyBrief

client = TestClient(app)

@pytest.fixture(autouse=True)
def mock_db_session():
    with patch("api.observer.SessionLocal") as mock_obs_session, \
         patch("processor.daily_brief.SessionLocal") as mock_db_session:
        
        mock_db = MagicMock()
        mock_obs_session.return_value = mock_db
        mock_db_session.return_value = mock_db
        
        mock_user = User(id=1, phone_number="+1234567890", name="Test User")
        mock_user.interests = [Interest(topic="AI"), Interest(topic="Rust")]
        
        mock_db.query.return_value.filter.return_value.first.return_value = mock_user
        mock_db.query.return_value.all.return_value = [mock_user]
        
        yield mock_db

@pytest.fixture(autouse=True)
def mock_chroma():
    with patch("chromadb.HttpClient") as mock_client:
        mock_collection = MagicMock()
        mock_collection.get.return_value = {
            "documents": [
                "Anthropic has just released a new AI model that outperforms GPT-4.",
                "Rust is increasingly being used in the Linux kernel for better memory safety."
            ],
            "metadatas": [
                {"type": "parent", "title": "New AI Model", "source": "TechCrunch", "url": "http://example.com/ai"},
                {"type": "parent", "title": "Rust in Linux", "source": "LinuxWeekly", "url": "http://example.com/rust"}
            ]
        }
        mock_client.return_value.get_collection.return_value = mock_collection
        mock_client.return_value.get_or_create_collection.return_value = mock_collection
        yield mock_client

@pytest.fixture(autouse=True)
def mock_llm_pipeline():
    with patch("taut.TautPipeline.run") as mock_run:
        async def mock_run_coro(request):
            response = MagicMock()
            if request.intent == "extract_article_bullets":
                response.content = "Bullet 1\nBullet 2"
            elif request.intent == "compile_daily_brief":
                response.content = json.dumps({
                    "date": "2026-07-06",
                    "headline_summary": "AI and Rust updates.",
                    "categories": [
                        {
                            "name": "Technology",
                            "articles": [
                                {
                                    "title": "New AI Model",
                                    "url": "http://example.com/ai",
                                    "key_insights": ["Bullet 1", "Bullet 2"]
                                }
                            ]
                        }
                    ]
                })
            else:
                response.content = "Mocked LLM Response"
            return response
            
        mock_run.side_effect = mock_run_coro
        
        # We also need to mock taut.create_pipeline in case it's called elsewhere
        with patch("api.query_engine.pipeline.run", side_effect=mock_run_coro), \
             patch("api.observer.pipeline.run", side_effect=mock_run_coro), \
             patch("processor.daily_brief.pipeline.run", side_effect=mock_run_coro):
            yield mock_run

@pytest.mark.asyncio
async def test_e2e_whatsapp_webhook():
    """
    Tests the Twilio webhook end-to-end with mocked services.
    """
    response = client.post(
        "/webhook/twilio",
        data={
            "From": "whatsapp:+1234567890",
            "Body": "Tell me the latest news about AI."
        }
    )
    
    assert response.status_code == 200
    assert "<Response><Message>" in response.text
    assert "Sorry, I encountered an error" not in response.text

@pytest.mark.asyncio
async def test_e2e_observer_agent(mock_db_session, mock_llm_pipeline):
    """
    Tests the Observer Agent with mocked LLM.
    """
    from api.observer import observe_conversation
    
    # Let's override the mock LLM pipeline to return extracted interests
    async def mock_run_coro(request):
        response = MagicMock()
        response.content = json.dumps({"topics": ["Aerospace", "SpaceX", "Rockets"]})
        return response
    mock_llm_pipeline.side_effect = mock_run_coro
    
    await observe_conversation("+1234567890", "I'm really starting to get interested in SpaceX and rocket launches.")
    
    assert mock_db_session.add.called
    assert mock_db_session.commit.called

@pytest.mark.asyncio
async def test_e2e_daily_brief_prefect_flow():
    """
    Tests the Prefect Map-Reduce workflow with mocked services.
    """
    from processor.daily_brief import process_all_users
    
    await process_all_users()
    
    expected_file = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "daily_brief_+1234567890.json")
    assert os.path.exists(expected_file), f"Daily brief file not found at {expected_file}"
    
    with open(expected_file, "r") as f:
        data = json.load(f)
        
    assert "headline_summary" in data
    assert "categories" in data
    assert len(data["categories"]) > 0
