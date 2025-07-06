import os
import time
import logging
from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool
from sqlalchemy.exc import OperationalError, DisconnectionError
import pandas as pd

logger = logging.getLogger(__name__)

def create_robust_engine(db_url: str):
    """Create database engine optimized for Neon DB with connection pooling"""
    return create_engine(
        db_url,
        poolclass=QueuePool,
        pool_size=2,                    # Small pool for Neon free tier
        max_overflow=3,
        pool_recycle=1800,              # 30 minutes - refresh connections
        pool_pre_ping=True,             # Test connections before use
        pool_reset_on_return='commit',  # Clean up connections
        connect_args={
            'connect_timeout': 30,
            'application_name': 'retail_ai',
            'sslmode': 'require'
            # Removed statement_timeout completely for Neon compatibility
        }
    )

def execute_query_with_retry(engine, query: str, max_retries=3) -> pd.DataFrame:
    """Execute database query with automatic retry on connection failure"""
    for attempt in range(max_retries):
        try:
            return pd.read_sql(query, engine)
        except (OperationalError, DisconnectionError) as e:
            logger.warning(f"Database connection attempt {attempt + 1} failed: {str(e)[:100]}...")
            if attempt == max_retries - 1:
                logger.error(f"All database attempts failed after {max_retries} retries, returning empty DataFrame")
                return pd.DataFrame()
            # Exponential backoff: 1s, 2s, 4s
            time.sleep(2 ** attempt)
        except Exception as e:
            logger.error(f"Unexpected database query error: {str(e)[:100]}...")
            return pd.DataFrame()
    return pd.DataFrame() 