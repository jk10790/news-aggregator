import sys
import datetime
from database import SessionLocal, User, Interest

def create_user(phone_number: str):
    db = SessionLocal()
    
    # Check if user already exists
    user = db.query(User).filter_by(phone_number=phone_number).first()
    if user:
        print(f"User {phone_number} already exists! Replacing interests...")
        db.query(Interest).filter_by(user_id=user.id).delete()
    else:
        user = User(phone_number=phone_number, name="Test User")
        db.add(user)
        db.commit()
        db.refresh(user)
        print(f"Created new user: {phone_number}")

    # Add interests
    topics = ["AI", "Startups", "Programming"]
    for topic in topics:
        interest = Interest(
            user_id=user.id,
            topic=topic,
            engagement_score=1.0,
            last_interacted_at=datetime.datetime.utcnow()
        )
        db.add(interest)
    
    db.commit()
    print(f"Successfully added interests: {topics}")
    db.close()

if __name__ == "__main__":
    phone = input("Enter your phone number (e.g., +1234567890 for Twilio or your Chat ID for Telegram): ").strip()
    if phone:
        create_user(phone)
    else:
        print("Phone number cannot be empty.")
