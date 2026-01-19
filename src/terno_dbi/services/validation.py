import json
import logging
from sqlalchemy.engine.url import make_url
from sqlalchemy.exc import ArgumentError

from terno_dbi.connectors import ConnectorFactory

logger = logging.getLogger(__name__)


DIALECT_PREFIXES = {
    'mysql': ['mysql+pymysql://', 'mysql://'],
    'postgres': ['postgresql+psycopg2://', 'postgresql://'],
    'bigquery': ['bigquery://'],
    'oracle': ['oracle+oracledb://'],
    'databricks': ['databricks://'],
    'snowflake': ['snowflake://'],
}


def validate_datasource_input(type_, conn_str, connection_json=None):
    if not conn_str or not type_:
        return "Connection string and source type are required."

    conn_str = conn_str.strip()

    if '://' not in conn_str:
        return "Connection string must be in format: dialect://[username:password@]host[:port]/database"

    if conn_str.count('://') > 1:
        return "Connection string contains multiple protocol separators (://)"

    try:
        url = make_url(conn_str)
    except ArgumentError:
        return "Could not parse SQLAlchemy URL from the connection string."

    drivername = url.drivername.lower()
    type_key = type_.lower()
    allowed_prefixes = DIALECT_PREFIXES.get(type_key)

    if allowed_prefixes and not any(conn_str.lower().startswith(p) for p in allowed_prefixes):
        return (
            f"Connection string does not match the selected database type ({type_}). "
            f"Expected one of: {', '.join(allowed_prefixes)}"
        )

    if drivername.startswith('bigquery'):
        if not connection_json:
            return "BigQuery requires service account credentials in `connection_json`."
        try:
            if isinstance(connection_json, str):
                connection_json = json.loads(connection_json)

            parts = conn_str.replace('bigquery://', '').split('/')
            if len(parts) != 2:
                return "BigQuery connection string must be in format: bigquery://project_id/dataset_id"

            project_id, dataset_id = parts

            try:
                from google.cloud import bigquery
                from google.cloud import exceptions

                client = bigquery.Client.from_service_account_info(connection_json)

                try:
                    client.list_datasets(project=project_id, max_results=1)
                except exceptions.NotFound:
                    return f"BigQuery project '{project_id}' does not exist."
                except exceptions.PermissionDenied:
                    return f"Permission denied: Cannot access BigQuery project '{project_id}'."

                try:
                    dataset_ref = client.dataset(dataset_id, project=project_id)
                    client.get_dataset(dataset_ref)
                except exceptions.NotFound:
                    return f"Dataset '{dataset_id}' does not exist in project '{project_id}'."
                except exceptions.PermissionDenied:
                    return f"Permission denied: Cannot access dataset '{dataset_id}'."

            except ImportError:
                logger.warning("google-cloud-bigquery not installed, skipping detailed BigQuery validation")

        except Exception as e:
            return f"Error validating BigQuery connection: {str(e)}"

    try:
        connector = ConnectorFactory.create_connector(
            type_,
            conn_str,
            credentials=connection_json
        )
        with connector.get_connection():
            pass
        connector.close()
    except Exception as e:
        return f"Could not connect to database: {str(e)}"

    return None
