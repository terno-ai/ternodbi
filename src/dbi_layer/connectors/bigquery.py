from typing import Optional, Dict, Any, Tuple
import sqlalchemy
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
