import datetime
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, Float, DateTime
from sqlalchemy.orm import declarative_base, sessionmaker, relationship

Base = declarative_base()

class User(Base):
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True)
    phone_number = Column(String, unique=True, nullable=False)
    name = Column(String, nullable=True)
    timezone = Column(String, default="UTC")
    
    interests = relationship("Interest", back_populates="user", cascade="all, delete-orphan")

class Interest(Base):
    __tablename__ = 'interests'
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id'))
    topic = Column(String, nullable=False)
    engagement_score = Column(Float, default=1.0)
    last_interacted_at = Column(DateTime, default=datetime.datetime.utcnow)
    
    user = relationship("User", back_populates="interests")

from config import DATABASE_URL

engine = create_engine(
    DATABASE_URL,
    pool_size=5,
    max_overflow=10
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


