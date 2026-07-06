import pytest
import asyncio
import os
import sys
import json
from fastapi.testclient import TestClient

# Adjust path so we can import our application modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from api.main import app
from database import SessionLocal, User, Interest, Base, engine
import chromadb
from config import CHROMA_SERVER_HOST, CHROMA_SERVER_PORT

client = TestClient(app)

@pytest.fixture(scope="module", autouse=True)
def setup_databases():
    """
    Sets up real databases with test data. No mocking!
    """
    # 1. Setup SQLite Database
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)
    
    db = SessionLocal()
    # Insert Test User
    test_user = User(phone_number="+1234567890", name="Test User")
    test_user.interests.append(Interest(topic="AI"))
    test_user.interests.append(Interest(topic="Rust"))
    db.add(test_user)
    db.commit()
    db.close()
    
    # 2. Setup ChromaDB Data
    chroma_client = chromadb.HttpClient(host=CHROMA_SERVER_HOST, port=int(CHROMA_SERVER_PORT))
    try:
        # Create or get test collection
        collection = chroma_client.get_or_create_collection("news_archive")
        
        # Insert a Parent Document (which represents the full article)
        collection.upsert(
            ids=["doc_parent_1", "doc_parent_2"],
            documents=[
                "Anthropic has just released a new AI model that outperforms GPT-4.",
                "Rust is increasingly being used in the Linux kernel for better memory safety."
            ],
            metadatas=[
                {"type": "parent", "title": "New AI Model", "source": "TechCrunch", "url": "http://example.com/ai", "topic": "AI"},
                {"type": "parent", "title": "Rust in Linux", "source": "LinuxWeekly", "url": "http://example.com/rust", "topic": "Rust"}
            ]
        )
        
        # Insert Child Chunks for semantic search
        collection.upsert(
            ids=["doc_child_1", "doc_child_2"],
            documents=[
                "Anthropic has just released a new AI model that outperforms GPT-4.",
                "Rust is increasingly being used in the Linux kernel for better memory safety."
            ],
            metadatas=[
                {"type": "child", "parent_id": "doc_parent_1", "topic": "AI", "published_int": 20260706},
                {"type": "child", "parent_id": "doc_parent_2", "topic": "Rust", "published_int": 20260706}
            ]
        )
    except Exception as e:
        print(f"Warning: Failed to connect to ChromaDB or upsert data: {e}")
        
    yield
    
    # Cleanup SQLite
    Base.metadata.drop_all(bind=engine)

@pytest.mark.asyncio
async def test_e2e_whatsapp_webhook():
    """
    Tests the Twilio webhook end-to-end. It sends a message, hydrates state from SQLite,
    routes to the CRAG workflow, executes hybrid search, evaluates, and returns the LLM answer.
    """
    response = client.post(
        "/webhook/twilio",
        data={
            "From": "whatsapp:+1234567890",
            "Body": "Tell me the latest news about AI."
        }
    )
    
    assert response.status_code == 200
    # Response should be a TwiML string containing the answer
    assert "<Response><Message>" in response.text
    assert "Sorry, I encountered an error" not in response.text

@pytest.mark.asyncio
async def test_e2e_observer_agent():
    """
    Tests the Observer Agent in isolation. It should read a message and update the DB if confident.
    """
    from api.observer import observe_conversation
    
    # Trigger observer directly with a strong signal message
    await observe_conversation("+1234567890", "I'm really starting to get interested in SpaceX and rocket launches.")
    
    # Check if the DB was updated
    db = SessionLocal()
    user = db.query(User).filter(User.phone_number == "+1234567890").first()
    interests = [i.topic for i in user.interests]
    db.close()
    
    # The LLM should extract 'Aerospace' or 'SpaceX' or 'Rockets'
    assert len(interests) > 2, f"Expected >2 interests, got {len(interests)}: {interests}"

@pytest.mark.asyncio
async def test_e2e_daily_brief_prefect_flow():
    """
    Tests the Prefect Map-Reduce workflow. It reads from ChromaDB, loops over SQLite users,
    and runs the LLM summarization.
    """
    from processor.daily_brief import process_all_users
    
    # Run the Prefect flow
    await process_all_users()
    
    # Assert that the brief file was created for the user
    expected_file = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "daily_brief_+1234567890.json")
    assert os.path.exists(expected_file), f"Daily brief file not found at {expected_file}"
    
    # Read and validate JSON
    with open(expected_file, "r") as f:
        data = json.load(f)
        
    assert "headline_summary" in data
    assert "categories" in data
    assert len(data["categories"]) > 0
