from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from src.config import Config

def get_db_engine() -> Engine:
    """
    Creates and returns a SQLAlchemy engine for the database.
    """
    try:
        # Check if we are using the default "password" which likely implies no real DB is set up yet
        # For demonstration purposes, we might default to SQLite if Postgres fails, but the prompt asks for "Real project"
        # We will assume Postgres is the target, but maybe fallback to sqlite for local demo if needed.
        # However, strictly following the tech stack "PostgreSQL", we will configure for that.
        
        engine = create_engine(Config.DATABASE_URL)
        return engine
    except Exception as e:
        print(f"Error creating database engine: {e}")
        raise
