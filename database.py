import datetime
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, Float, DateTime, Boolean
from sqlalchemy.orm import declarative_base, sessionmaker, relationship

Base = declarative_base()

class User(Base):
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True)

    # --- Identity (used for login/OTP auth) ---
    phone_number = Column(String, unique=True, nullable=True)   # nullable: Telegram-only users may not have one
    name = Column(String, nullable=True)
    timezone = Column(String, default="UTC")

    # --- Telegram delivery channel (populated via deep-link /start webhook) ---
    telegram_chat_id = Column(String, nullable=True, unique=True)
    telegram_bot_token = Column(String, nullable=True)  # Per-user bot token; falls back to system default if null

    # --- Delivery preferences ---
    delivery_cadence = Column(String, default="daily")    # daily | weekly | real-time | paused
    delivery_time_utc = Column(Integer, default=8)        # Hour of day (0-23) in UTC

    # --- Feature flags ---
    is_premium = Column(Boolean, default=False)

    interests = relationship("Interest", back_populates="user", cascade="all, delete-orphan")

class Interest(Base):
    __tablename__ = 'interests'
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id'))
    topic = Column(String, nullable=False)
    engagement_score = Column(Float, default=1.0)
    last_interacted_at = Column(DateTime, default=datetime.datetime.utcnow)
    source = Column(String, default="explicit")  # explicit | implicit (AI-discovered)

    user = relationship("User", back_populates="interests")

from config import DATABASE_URL

engine = create_engine(
    DATABASE_URL,
    pool_size=5,
    max_overflow=10
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
