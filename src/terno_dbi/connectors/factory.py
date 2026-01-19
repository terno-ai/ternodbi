from typing import Optional, Dict, Any
from .base import BaseConnector
import logging

logger = logging.getLogger(__name__)


class UnsupportedDatabaseError(Exception):
    def __init__(self, db_type: str):
        super().__init__(f"Unsupported database type: {db_type}")
        self.db_type = db_type
        logger.error(f"Attempted to create connector for unsupported database: {db_type}")


class ConnectorFactory:
    _connectors = {}

    @classmethod
    def register_connector(cls, db_type: str, connector_class: type):
        cls._connectors[db_type.lower()] = connector_class
        logger.info(
            f"Registered custom connector for '{db_type}': "
            f"{connector_class.__name__}"
        )

    @classmethod
    def create_connector(
        cls,
        db_type: str,
        connection_string: str,
        credentials: Optional[Dict[str, Any]] = None
    ) -> BaseConnector:
        db_type_lower = db_type.lower()
        logger.info(f"Creating connector for database type: '{db_type}'")

        # Lazy import to avoid circular imports and register connectors
        if not cls._connectors:
            logger.debug("First connector request - registering all connectors")
            cls._register_all_connectors()

        connector_class = cls._connectors.get(db_type_lower)

        if connector_class is None:
            logger.error(f"No connector found for database type: '{db_type}'")
            raise UnsupportedDatabaseError(db_type)

        logger.debug(f"Using connector class: {connector_class.__name__}")

        masked_conn = cls._mask_connection_string(connection_string)
        logger.info(f"Initializing {connector_class.__name__} with connection: {masked_conn}")

        connector = connector_class(connection_string, credentials)
        logger.info(f"Successfully created {connector_class.__name__}")

        return connector

    @classmethod
    def _mask_connection_string(cls, connection_string: str) -> str:
        try:
            if '://' in connection_string and '@' in connection_string:
                scheme_end = connection_string.index('://') + 3
                at_pos = connection_string.index('@')
                prefix = connection_string[:scheme_end]
                suffix = connection_string[at_pos:]
                user_pass = connection_string[scheme_end:at_pos]
                if ':' in user_pass:
                    user = user_pass.split(':')[0]
                    return f"{prefix}{user}:****{suffix}"
            return connection_string[:50] + '...' if len(connection_string) > 50 else connection_string
        except:
            return '***masked***'

    @classmethod
    def _register_all_connectors(cls):
        logger.info("Registering all database connectors...")

        from .postgres import PostgresConnector
        from .mysql import MySQLConnector
        from .snowflake import SnowflakeConnector
        from .bigquery import BigQueryConnector
        from .databricks import DatabricksConnector
        from .oracle import OracleConnector
        from .sqlite import SQLiteConnector

        cls._connectors = {
            'generic': SQLiteConnector,  # For admin UI (hidden from non-superusers)
            'postgres': PostgresConnector,
            'postgresql': PostgresConnector,
            'mysql': MySQLConnector,
            'snowflake': SnowflakeConnector,
            'bigquery': BigQueryConnector,
            'databricks': DatabricksConnector,
            'oracle': OracleConnector,
            'sqlite': SQLiteConnector,
        }

        logger.info(f"Registered {len(set(cls._connectors.values()))} connector types: "
                   f"{', '.join(set(c.__name__ for c in cls._connectors.values()))}")

    @classmethod
    def get_supported_databases(cls) -> list:
        if not cls._connectors:
            cls._register_all_connectors()
        supported = list(set(cls._connectors.keys()))
        logger.debug(f"Supported databases: {supported}")
        return supported
