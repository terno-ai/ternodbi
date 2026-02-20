from typing import Optional, Dict, Any, Tuple, List
import sqlalchemy
from sqlalchemy import text
from sqlalchemy import MetaData, inspect
from sqlalchemy.engine.url import make_url
from sqlshield.models import MDatabase
from .base import BaseConnector, DEFAULT_POOL_SIZE, DEFAULT_MAX_OVERFLOW, DEFAULT_POOL_TIMEOUT, DEFAULT_POOL_RECYCLE
import logging

logger = logging.getLogger(__name__)


class DatabricksConnector(BaseConnector):

    def __init__(self, connection_string: str, credentials: Optional[Dict[str, Any]] = None,
                 pool_size: int = DEFAULT_POOL_SIZE,
                 max_overflow: int = DEFAULT_MAX_OVERFLOW,
                 pool_timeout: int = DEFAULT_POOL_TIMEOUT,
                 pool_recycle: int = DEFAULT_POOL_RECYCLE,
                 use_pool: bool = True):
        super().__init__(connection_string, credentials, pool_size, max_overflow,
                        pool_timeout, pool_recycle, use_pool)

        url = make_url(connection_string)
        self._schema = url.database or "default"

    def get_metadata(self) -> MDatabase:
        engine = self.get_engine()
        metadata = self._safe_reflect_metadata(engine, self._schema)
        return MDatabase.from_inspector(metadata)

    def get_dialect_info(self) -> Tuple[str, str]:
        engine = self.get_engine()
        with engine.connect():
            dialect_name = engine.dialect.name
            dialect_version = str(engine.dialect.server_version_info)

        return (dialect_name, dialect_version)

    def _safe_reflect_metadata(
        self,
        engine: sqlalchemy.Engine,
        schema: Optional[str] = None,
        only: Optional[list] = None
    ) -> MetaData:
        metadata = MetaData(schema=schema)
        inspector = inspect(engine)

        try:
            table_names = only or inspector.get_table_names(schema=schema)
            logger.info(f"Tables found: {table_names}")
            for table_name in table_names:
                logger.debug(f"Processing table: {table_name}")
                try:
                    metadata.reflect(
                        bind=engine,
                        only=[table_name],
                        schema=schema,
                        extend_existing=True
                    )
                except Exception as e:
                    logger.error(f"Failed to reflect {table_name}: {e}")
        except Exception as e:
            logger.error(f"Error during metadata inspection: {e}")

        return metadata

    # skippng row count for databricks for now as Databricks DESCRIBE DETAIL is per-table and too slow (~10s each).
    def get_table_row_counts(
        self, schema: Optional[str] = None, tables: Optional[List[str]] = None
    ) -> Dict[str, int]:
        return {}

        # if not tables:
        #     return {}
        # schema = schema or self._schema
        # counts: Dict[str, int] = {}
        # with self.get_connection() as conn:
        #     for table_name in tables:
        #         try:
        #             qualified = f"`{schema}`.`{table_name}`"
        #             result = conn.execute(text(f"DESCRIBE DETAIL {qualified}"))
        #             row = result.fetchone()
        #             if row:
        #                 col_names = list(result.keys())
        #                 row_dict = dict(zip(col_names, row))
        #                 num_rows = row_dict.get("numRows")
        #                 if num_rows is not None:
        #                     counts[table_name] = int(num_rows)
        #         except Exception as e:
        #             logger.warning(f"Could not get row count for {table_name}: {e}")
        # return counts
