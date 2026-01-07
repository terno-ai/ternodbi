"""
TernoDBI - Database Interface Layer

An open-source database interface layer that provides:
- Multi-database connectivity (PostgreSQL, MySQL, SQLite, BigQuery, Snowflake, etc.)
- SQLShield integration for SQL translation and security
- Access control for tables and columns
- MCP interface for AI agents
- Django models for database metadata

Migrated from TernoAI for open-source use.
"""

__version__ = "0.1.0"

# Connectors (no Django dependency)
from dbi_layer.connectors import (
    BaseConnector,
    ConnectorFactory,
    PostgresConnector,
    MySQLConnector,
    SQLiteConnector,
    BigQueryConnector,
    SnowflakeConnector,
    DatabricksConnector,
    OracleConnector,
)

# Lazy imports for Django-dependent services
# These will only be imported when actually used
def _lazy_import_services():
    """Import services lazily to avoid Django configuration requirement at import time."""
    from dbi_layer.services import (
        prepare_mdb,
        generate_mdb,
        generate_native_sql,
        execute_native_sql,
        execute_native_sql_return_df,
        get_all_group_tables,
        get_all_group_columns,
    )
    return {
        'prepare_mdb': prepare_mdb,
        'generate_mdb': generate_mdb,
        'generate_native_sql': generate_native_sql,
        'execute_native_sql': execute_native_sql,
        'execute_native_sql_return_df': execute_native_sql_return_df,
        'get_all_group_tables': get_all_group_tables,
        'get_all_group_columns': get_all_group_columns,
    }

__all__ = [
    # Version
    "__version__",
    
    # Connectors (always available)
    "BaseConnector",
    "ConnectorFactory",
    "PostgresConnector",
    "MySQLConnector",
    "SQLiteConnector",
    "BigQueryConnector",
    "SnowflakeConnector",
    "DatabricksConnector",
    "OracleConnector",
]
