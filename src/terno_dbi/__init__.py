from importlib.metadata import PackageNotFoundError, version as _pkg_version

try:
    # Read from installed package metadata so this cannot drift from
    # pyproject.toml, as it had (it read 0.1.0 through the 0.1.34 release).
    __version__ = _pkg_version("terno-dbi")
except PackageNotFoundError:  # running from a source tree with no install
    __version__ = "0.0.0.dev0"

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
