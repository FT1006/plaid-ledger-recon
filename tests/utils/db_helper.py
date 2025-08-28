"""Database helper utilities for tests."""

from sqlalchemy import Engine, create_engine


def create_test_engine(database_url: str) -> Engine:
    """Create SQLAlchemy engine with correct psycopg driver."""
    if database_url.startswith("postgresql://"):
        database_url = database_url.replace("postgresql://", "postgresql+psycopg://", 1)
    return create_engine(database_url)
