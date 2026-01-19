
import io
import math
import csv
import logging
import base64
import sqlalchemy
import pandas as pd
from django.http import HttpResponse
from django.utils import timezone

from dbi_layer.connectors import ConnectorFactory

logger = logging.getLogger(__name__)


def execute_native_sql(datasource, native_sql, page=1, per_page=50):
    try:
        connector = ConnectorFactory.create_connector(
            datasource.type,
            datasource.connection_str,
            credentials=datasource.connection_json
        )
        
        with connector.get_connection() as con:
            execute_result = con.execute(sqlalchemy.text(native_sql))
            table_data = _prepare_table_data(execute_result, page, per_page)
            return {
                'status': 'success',
                'table_data': table_data
            }
    except Exception as e:
        logger.exception(f"Query execution error: {e}")
        return {
            'status': 'error',
            'error': str(e)
        }


def execute_native_sql_return_df(datasource, native_sql):
    try:
        connector = ConnectorFactory.create_connector(
            datasource.type,
            datasource.connection_str,
            credentials=datasource.connection_json
        )
        
        with connector.get_connection() as con:
            execute_result = con.execute(sqlalchemy.text(native_sql))
            fetch_result = execute_result.fetchall()
            buffer = io.BytesIO()
            df = pd.DataFrame(fetch_result, columns=list(execute_result.keys()))
            df.to_parquet(buffer, engine='pyarrow', index=False)
            buffer.seek(0)
            parquet_b64 = base64.b64encode(buffer.read()).decode('utf-8')
            return {
                'status': 'success',
                'parquet_b64': parquet_b64
            }
    except Exception as e:
        logger.exception(f"Query execution error: {e}")
        return {
            'status': 'error',
            'error': str(e)
        }


def export_native_sql_result(datasource, native_sql):
    connector = ConnectorFactory.create_connector(
        datasource.type,
        datasource.connection_str,
        credentials=datasource.connection_json
    )
    
    utc_time = timezone.now().strftime('%Y-%m-%d_%H-%M-%S')
    file_name = f'dbi_{datasource.display_name}_{utc_time}.csv'
    
    with connector.get_connection() as con:
        execute_result = con.execute(sqlalchemy.text(native_sql))
        response = HttpResponse(content_type='text/csv')
        response['Content-Disposition'] = f'attachment; filename={file_name}'
        writer = csv.writer(response)
        writer.writerow(execute_result.keys())
        writer.writerows(execute_result)
        return response


def _make_json_safe(value):
    """Convert a value to be JSON serializable."""
    if value is None:
        return None
    if isinstance(value, (bytearray, bytes)):
        return base64.b64encode(value).decode('utf-8')
    if isinstance(value, (int, float, str, bool)):
        return value
    return str(value)


def _prepare_table_data(execute_result, page, per_page):
    table_data = {}
    table_data['columns'] = list(execute_result.keys())

    fetch_result = execute_result.fetchall()

    total_count = execute_result.rowcount
    if total_count <= 0:
        total_count = len(fetch_result)
    total_pages = math.ceil(total_count // per_page)
    table_data['total_pages'] = total_pages
    table_data['row_count'] = total_count
    table_data['page'] = page

    offset = (page - 1) * per_page
    paginated_results = fetch_result[offset:offset+per_page]
    table_data['data'] = []

    for row in paginated_results:
        data = {}
        for i, column in enumerate(table_data['columns']):
            data[column] = _make_json_safe(row[i])
        table_data['data'].append(data)
    return table_data

