"""Database connection and session management for Cook County scraper"""

import os

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from models import Base


def get_database_url(local: bool = False) -> str:
    """
    Get database URL from environment variables or default to local postgres

    Returns:
        Database connection URL string
    """
    if local:
        db_host = os.getenv("DB_HOST", "postgres")
        db_port = os.getenv("DB_PORT", "5432")
        db_name = os.getenv("DB_NAME", "postgres")
        db_user = os.getenv("DB_USER", "postgres")
        db_password = os.getenv("DB_PASSWORD", "postgres")

        return f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    else:
        db_uri = os.getenv("REMOTE_POSTGRES_URI")
        return db_uri


def create_engine_and_session():
    """
    Create database engine and session factory

    Returns:
        Tuple of (engine, SessionLocal)
    """
    database_url = get_database_url()
    engine = create_engine(database_url, echo=True)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

    return engine, SessionLocal


def create_tables(engine) -> None:
    """
    Create database tables using SQLAlchemy's ORM Base.metadata

    Parameters:
        engine: SQLAlchemy engine instance

    Returns:
        None
    """
    Base.metadata.create_all(engine)


if __name__ == "__main__":
    database_url = get_database_url()
    engine = create_engine(database_url, echo=True, pool_pre_ping=True)

    print("Testing database connection...")
    conn = engine.connect()
    conn.close()
    print("Connection successful")
