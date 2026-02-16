from typing import Optional, Dict, Any, Tuple, List
import sqlalchemy
from sqlalchemy import text
from sqlshield.models import MDatabase
from .base import BaseConnector, DEFAULT_POOL_SIZE, DEFAULT_MAX_OVERFLOW, DEFAULT_POOL_TIMEOUT, DEFAULT_POOL_RECYCLE
import logging

logger = logging.getLogger(__name__)


class PostgresConnector(BaseConnector):

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
        metadata = self._reflect_metadata(engine)
        return MDatabase.from_inspector(metadata)

    def get_dialect_info(self) -> Tuple[str, str]:
        engine = self.get_engine()
        with engine.connect():
            dialect_name = engine.dialect.name
            dialect_version = str(engine.dialect.server_version_info)

        if dialect_name == 'postgresql':
            dialect_name = 'postgres'

        return (dialect_name, dialect_version)

    def get_table_row_counts(
        self, schema: Optional[str] = None, tables: Optional[List[str]] = None
    ) -> Dict[str, int]:
        with self.get_connection() as conn:
            if not schema:
                try:
                    schema = conn.execute(text("SELECT current_schema()")).scalar()
                except Exception as e:
                    logger.warning(
                        f"Could not determine current schema, defaulting to public: {e}"
                    )
                    schema = "public"

            if tables:
                bare_names = []
                for t in tables:
                    parts = t.rsplit('.', 1)
                    bare_names.append(parts[-1])

                query = text(
                    "SELECT c.relname, c.reltuples::bigint "
                    "FROM pg_class c "
                    "JOIN pg_namespace n ON n.oid = c.relnamespace "
                    "WHERE c.relkind IN ('r', 'v', 'm') "
                    "  AND n.nspname = :schema "
                    "  AND c.relname = ANY(:tables)"
                )
                rows = conn.execute(
                    query, {"schema": schema, "tables": bare_names}
                ).fetchall()
            else:
                query = text(
                    "SELECT c.relname, c.reltuples::bigint "
                    "FROM pg_class c "
                    "JOIN pg_namespace n ON n.oid = c.relnamespace "
                    "WHERE c.relkind IN ('r', 'v', 'm') "
                    "  AND n.nspname = :schema"
                )
                rows = conn.execute(query, {"schema": schema}).fetchall()

        # reltuples is -1 for tables never analyzed; skip those
        return {row[0]: int(row[1]) for row in rows if int(row[1]) >= 0}
