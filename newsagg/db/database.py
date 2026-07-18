"""Engine + session factory. Table/model definitions live in newsagg.db.schema."""
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from newsagg.config import DATABASE_URL
from newsagg.db.schema import Base  # re-exported for convenience/back-compat

engine = create_engine(
    DATABASE_URL,
    pool_size=5,
    max_overflow=10
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
