from typing import Optional, Dict, Any, Tuple, List
import sqlalchemy
from sqlalchemy import text
from sqlshield.models import MDatabase
from .base import BaseConnector, DEFAULT_POOL_SIZE, DEFAULT_MAX_OVERFLOW, DEFAULT_POOL_TIMEOUT, DEFAULT_POOL_RECYCLE
import logging

logger = logging.getLogger(__name__)


class SnowflakeConnector(BaseConnector):

    def __init__(self, connection_string: str, credentials: Optional[Dict[str, Any]] = None,
                 pool_size: int = DEFAULT_POOL_SIZE,
                 max_overflow: int = DEFAULT_MAX_OVERFLOW,
                 pool_timeout: int = DEFAULT_POOL_TIMEOUT,
                 pool_recycle: int = DEFAULT_POOL_RECYCLE,
                 use_pool: bool = True):
        super().__init__(connection_string, credentials, pool_size, max_overflow,
                        pool_timeout, pool_recycle, use_pool)

    def get_metadata(self) -> MDatabase:
        engine = self.get_engine()
        return MDatabase.from_snowflake_dialect(engine)

    def get_dialect_info(self) -> Tuple[str, str]:
        engine = self.get_engine()
        with engine.connect():
            dialect_name = engine.dialect.name
            dialect_version = str(engine.dialect.server_version_info)

        return (dialect_name, dialect_version)

    def get_table_row_counts(
        self, schema: Optional[str] = None, tables: Optional[List[str]] = None
    ) -> Dict[str, int]:
        engine = self.get_engine()
        if not schema:
            db_part = engine.url.database or ""
            parts = db_part.split("/")
            if len(parts) >= 2 and parts[-1]:
                schema = parts[-1]

        with self.get_connection() as conn:
            # Fall back to the session's current schema
            if not schema:
                try:
                    schema = conn.execute(text("SELECT CURRENT_SCHEMA()")).scalar()
                except Exception as e:
                    logger.warning(f"Could not determine Snowflake schema: {e}")
                    return {}
            if not schema:
                return {}

            query = text(
                "SELECT TABLE_NAME, ROW_COUNT "
                "FROM INFORMATION_SCHEMA.TABLES "
                "WHERE TABLE_SCHEMA = :schema "
                "  AND TABLE_TYPE IN ('BASE TABLE', 'VIEW')"
            )
            rows = conn.execute(query, {"schema": schema.upper()}).fetchall()
        return {row[0]: int(row[1]) for row in rows if row[1] is not None}
