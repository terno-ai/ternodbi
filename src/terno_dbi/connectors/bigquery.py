from typing import Optional, Dict, Any, Tuple, List
import sqlalchemy
from sqlalchemy import text
from sqlalchemy.pool import QueuePool, NullPool
from sqlshield.models import MDatabase
from .base import BaseConnector, DEFAULT_POOL_SIZE, DEFAULT_MAX_OVERFLOW, DEFAULT_POOL_TIMEOUT, DEFAULT_POOL_RECYCLE
import logging

logger = logging.getLogger(__name__)


class BigQueryConnector(BaseConnector):

    def __init__(self, connection_string: str, credentials: Optional[Dict[str, Any]] = None,
                 pool_size: int = DEFAULT_POOL_SIZE,
                 max_overflow: int = DEFAULT_MAX_OVERFLOW,
                 pool_timeout: int = DEFAULT_POOL_TIMEOUT,
                 pool_recycle: int = DEFAULT_POOL_RECYCLE,
                 use_pool: bool = True):
        if not credentials:
            raise ValueError("BigQuery requires credentials (service account JSON)")
        super().__init__(connection_string, credentials, pool_size, max_overflow,
                        pool_timeout, pool_recycle, use_pool)

    def _create_engine(self) -> sqlalchemy.Engine:
        """Override to pass BigQuery-specific credentials."""
        pool_kwargs = {}

        if self.use_pool:
            pool_kwargs = {
                'poolclass': QueuePool,
                'pool_size': self.pool_size,
                'max_overflow': self.max_overflow,
                'pool_timeout': self.pool_timeout,
                'pool_recycle': self.pool_recycle,
                'pool_pre_ping': False,
            }
        else:
            pool_kwargs = {'poolclass': NullPool}

        return sqlalchemy.create_engine(
            self.connection_string,
            credentials_info=self.credentials,
            **pool_kwargs
        )

    def get_metadata(self) -> MDatabase:
        engine = self.get_engine()
        metadata = self._reflect_metadata(engine)
        return MDatabase.from_inspector(metadata)

    def get_dialect_info(self) -> Tuple[str, str]:
        engine = self.get_engine()
        with engine.connect():
            dialect_name = engine.dialect.name
            dialect_version = str(getattr(engine.dialect, 'server_version_info', ('unknown',)))

        return (dialect_name, dialect_version)

    def get_table_row_counts(
        self, schema: Optional[str] = None, tables: Optional[List[str]] = None
    ) -> Dict[str, int]:
        engine = self.get_engine()
        project = engine.url.host or ""
        dataset = schema or engine.url.database or ""
        if not project or not dataset:
            logger.warning(
                f"BigQuery row counts skipped: project='{project}', dataset='{dataset}'"
            )
            return {}

        try:
            from google.cloud import bigquery as bq
            from google.oauth2 import service_account

            creds = service_account.Credentials.from_service_account_info(
                self.credentials
            )
            billing_project = self.credentials.get("project_id", project)
            client = bq.Client(credentials=creds, project=billing_project)

            query = (
                f"SELECT table_id, row_count "
                f"FROM `{project}.{dataset}.__TABLES__`"
            )
            rows = client.query_and_wait(query)
            result = {row.table_id: int(row.row_count) for row in rows if row.row_count is not None}
            logger.info(f"BigQuery row counts: {len(result)} tables from {project}.{dataset}")
            return result
        except Exception as e:
            logger.warning(f"BigQuery row counts failed: {e}")
            return {}
