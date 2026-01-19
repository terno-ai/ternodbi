__version__ = "0.1.0"

from terno_dbi.connectors import (
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


def _lazy_import_services():
    from terno_dbi.services import (
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
    "__version__",
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
