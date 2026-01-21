import io
import math
import csv
import logging
import base64
import sqlalchemy
import pandas as pd
from django.http import HttpResponse
from django.utils import timezone
from typing import Optional, List, Dict, Any

from terno_dbi.connectors import ConnectorFactory
from terno_dbi.services.pagination import (
    PaginationService,
    PaginationConfig,
    PaginationMode,
    OrderColumn,
)

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


def execute_paginated_query(
    datasource,
    native_sql: str,
    pagination_mode: str = "offset",
    page: int = 1,
    per_page: int = 50,
    cursor: Optional[str] = None,
    direction: str = "forward",
    order_by: Optional[List[Dict[str, str]]] = None
) -> Dict[str, Any]:
    try:
        connector = ConnectorFactory.create_connector(
            datasource.type,
            datasource.connection_str,
            credentials=datasource.connection_json
        )

        mode = PaginationMode(pagination_mode)

        order_columns = None
        if order_by:
            order_columns = [
                OrderColumn(
                    column=o.get("column", "id"),
                    direction=o.get("direction", "DESC"),
                    nulls=o.get("nulls", "LAST")
                )
                for o in order_by
            ]

        config = PaginationConfig(
            mode=mode,
            page=page,
            per_page=per_page,
            cursor=cursor,
            direction=direction,
            order_by=order_columns or [OrderColumn("id", "DESC")]
        )

        service = PaginationService(
            connector=connector,
            dialect=datasource.dialect_name or datasource.type
        )

        result = service.paginate(native_sql, config)

        table_data = {
            'columns': result.columns,
            'data': [
                {
                    col: _make_json_safe(row[i])
                    for i, col in enumerate(result.columns)
                }
                for row in result.data
            ],
            'page': result.page,
            'per_page': result.per_page,
            'row_count': result.total_count,
            'total_pages': result.total_pages,
            'has_next': result.has_next,
            'has_prev': result.has_prev,
            'next_cursor': result.next_cursor,
            'prev_cursor': result.prev_cursor,
        }

        response = {
            'status': 'success',
            'table_data': table_data
        }

        if result.warnings:
            response['warnings'] = result.warnings

        return response

    except ValueError as e:
        logger.warning(f"Pagination error: {e}")
        return {
            'status': 'error',
            'error': str(e)
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


def export_native_sql_streaming(datasource, native_sql):
    connector = ConnectorFactory.create_connector(
        datasource.type,
        datasource.connection_str,
        credentials=datasource.connection_json
    )

    service = PaginationService(
        connector=connector,
        dialect=datasource.dialect_name or datasource.type
    )

    utc_time = timezone.now().strftime('%Y-%m-%d_%H-%M-%S')
    file_name = f'dbi_{datasource.display_name}_{utc_time}.csv'

    def generate_csv():
        first_batch = True
        for batch in service.stream_all(native_sql):
            buffer = io.StringIO()
            writer = csv.writer(buffer)

            if first_batch and batch:
                writer.writerow([f"col_{i}" for i in range(len(batch[0]))])
                first_batch = False

            for row in batch:
                writer.writerow(row)

            yield buffer.getvalue()

    from django.http import StreamingHttpResponse
    response = StreamingHttpResponse(generate_csv(), content_type='text/csv')
    response['Content-Disposition'] = f'attachment; filename={file_name}'
    return response


def _make_json_safe(value):
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
    total_pages = math.ceil(total_count / per_page) if per_page > 0 else 1
    table_data['total_pages'] = total_pages
    table_data['row_count'] = total_count
    table_data['page'] = page
    table_data['has_next'] = page < total_pages
    table_data['has_prev'] = page > 1

    offset = (page - 1) * per_page
    paginated_results = fetch_result[offset:offset+per_page]
    table_data['data'] = []

    for row in paginated_results:
        data = {}
        for i, column in enumerate(table_data['columns']):
            data[column] = _make_json_safe(row[i])
        table_data['data'].append(data)
    return table_data
