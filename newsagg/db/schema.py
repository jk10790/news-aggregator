"""Final SQLAlchemy schema (Phase 2). Postgres via Alembic only — no create_all.

users are rebuilt around telegram_chat_id as the sole identity (ADR-1/ADR-2:
Telegram is the only delivery channel in v1, one product bot, no per-user
bot tokens). interests carries an explicit/implicit source with decay rules
enforced in code (ADR-13). topic_modules + briefs replace the old
files-on-disk daily_brief.json (ADR-5/ADR-7).
"""
import datetime

from sqlalchemy import (
    Column, Integer, String, ForeignKey, Float, DateTime, Date, JSON,
    UniqueConstraint, func,
)
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    telegram_chat_id = Column(String, unique=True, nullable=False)  # sole identity in v1
    first_name = Column(String, nullable=True)          # from Telegram, for greeting
    timezone = Column(String, default="UTC")            # IANA name; v1 stores, doesn't convert
    delivery_cadence = Column(String, default="daily")  # 'daily' | 'weekly' | 'paused'
    delivery_hour_utc = Column(Integer, default=7)      # 0-23
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    interests = relationship("Interest", back_populates="user", cascade="all, delete-orphan")


class Interest(Base):
    __tablename__ = "interests"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    topic = Column(String, nullable=False)              # taxonomy slug — enforce in code
    source = Column(String, nullable=False, default="explicit")  # 'explicit' | 'implicit'
    engagement_score = Column(Float, default=1.0)
    last_interacted_at = Column(DateTime(timezone=True), server_default=func.now())

    user = relationship("User", back_populates="interests")

    __table_args__ = (UniqueConstraint("user_id", "topic", name="uq_interest_user_topic"),)


class TopicModule(Base):
    __tablename__ = "topic_modules"
    id = Column(Integer, primary_key=True)
    topic = Column(String, nullable=False)              # taxonomy slug
    module_date = Column(Date, nullable=False)          # UTC date
    content = Column(JSON, nullable=False)              # TopicModuleContent JSON (Phase 6)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (UniqueConstraint("topic", "module_date", name="uq_module_topic_date"),)


class Brief(Base):
    __tablename__ = "briefs"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    brief_date = Column(Date, nullable=False)
    content = Column(JSON, nullable=False)              # assembled brief (topics + text)
    delivered_at = Column(DateTime(timezone=True), nullable=True)

    __table_args__ = (UniqueConstraint("user_id", "brief_date", name="uq_brief_user_date"),)
