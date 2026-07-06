import os
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey
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
    
    user = relationship("User", back_populates="interests")

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'news_aggregator.db')
engine = create_engine(f'sqlite:///{DB_PATH}', connect_args={'check_same_thread': False})

# In SQLite, WAL mode allows concurrent reads and writes
with engine.connect() as conn:
    conn.exec_driver_sql("PRAGMA journal_mode=WAL")

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def init_db():
    Base.metadata.create_all(bind=engine)

if __name__ == "__main__":
    init_db()
    print("Database initialized.")
