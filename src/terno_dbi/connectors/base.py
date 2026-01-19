from abc import ABC, abstractmethod
from typing import Tuple, Optional, Dict, Any, Generator
from contextlib import contextmanager
import sqlalchemy
from sqlalchemy.pool import QueuePool, NullPool
from sqlalchemy.exc import TimeoutError as PoolTimeoutError
from sqlshield.models import MDatabase
import logging
import time

logger = logging.getLogger(__name__)

DEFAULT_POOL_SIZE = 20
DEFAULT_MAX_OVERFLOW = 30
DEFAULT_POOL_TIMEOUT = 60
DEFAULT_POOL_RECYCLE = 1800

DEFAULT_MAX_RETRIES = 3
DEFAULT_RETRY_BASE_DELAY = 1


class BaseConnector(ABC):

    def __init__(self, connection_string: str, credentials: Optional[Dict[str, Any]] = None,
                 pool_size: int = DEFAULT_POOL_SIZE,
                 max_overflow: int = DEFAULT_MAX_OVERFLOW,
                 pool_timeout: int = DEFAULT_POOL_TIMEOUT,
                 pool_recycle: int = DEFAULT_POOL_RECYCLE,
                 use_pool: bool = True):
        self.connection_string = connection_string
        self.credentials = credentials
        self._engine = None
        self.pool_size = pool_size
        self.max_overflow = max_overflow
        self.pool_timeout = pool_timeout
        self.pool_recycle = pool_recycle
        self.use_pool = use_pool

        logger.info(f"Connector initialized - pool_size={pool_size}, max_overflow={max_overflow}, "
                    f"pool_timeout={pool_timeout}s, pool_recycle={pool_recycle}s, use_pool={use_pool}")

    def get_engine(self) -> sqlalchemy.Engine:
        if self._engine is None:
            self._engine = self._create_engine()
            logger.info(f"Engine created for {self.__class__.__name__}")
        return self._engine

    def _create_engine(self) -> sqlalchemy.Engine:
        return self._create_base_engine()

    @contextmanager
    def get_connection(self) -> Generator[sqlalchemy.Connection, None, None]:
        engine = self.get_engine()
        conn = engine.connect()
        logger.debug("Connection acquired from pool")
        try:
            yield conn
        finally:
            conn.close()
            logger.debug("Connection returned to pool")

    @abstractmethod
    def get_metadata(self) -> MDatabase:
        pass

    @abstractmethod
    def get_dialect_info(self) -> Tuple[str, str]:
        pass

    def close(self):
        if self._engine:
            logger.info("Disposing engine and releasing all pool connections")
            self._engine.dispose()
            self._engine = None
            logger.debug("Engine disposed successfully")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def _create_base_engine(self, **kwargs) -> sqlalchemy.Engine:
        pool_kwargs = {}

        if self.use_pool:
            pool_kwargs = {
                'poolclass': QueuePool,
                'pool_size': self.pool_size,
                'max_overflow': self.max_overflow,
                'pool_timeout': self.pool_timeout,
                'pool_recycle': self.pool_recycle,
                'pool_pre_ping': True,
            }
            logger.info(f"Creating engine with QueuePool - max connections: {self.pool_size + self.max_overflow}")
        else:
            pool_kwargs = {'poolclass': NullPool}
            logger.info("Creating engine with NullPool (no connection pooling)")

        engine = sqlalchemy.create_engine(
            self.connection_string,
            **pool_kwargs,
            **kwargs
        )
        logger.debug(f"Engine created: {engine.url.get_backend_name()}")
        return engine

    def _reflect_metadata(self, engine: sqlalchemy.Engine, schema: Optional[str] = None) -> sqlalchemy.MetaData:
        logger.debug(f"Reflecting metadata for schema: {schema or 'default'}")
        metadata = sqlalchemy.MetaData(schema=schema)
        metadata.reflect(bind=engine)
        logger.info(f"Metadata reflected: {len(metadata.tables)} tables found")
        return metadata

    def execute_with_retry(self, func, max_retries: int = DEFAULT_MAX_RETRIES):
        last_error = None

        for attempt in range(max_retries):
            try:
                return func()
            except PoolTimeoutError as e:
                last_error = e
                delay = DEFAULT_RETRY_BASE_DELAY * (2 ** attempt)
                logger.warning(f"Pool timeout on attempt {attempt + 1}/{max_retries}. "
                              f"Retrying in {delay}s (exponential backoff)...")
                time.sleep(delay)
            except Exception as e:
                logger.error(f"Non-retryable error: {e}")
                raise e

        logger.error(f"All {max_retries} retry attempts failed due to pool timeout")
        raise last_error
