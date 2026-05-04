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
import sqlglot
from terno_dbi.core import models

logger = logging.getLogger(__name__)


def _infer_order_from_sql(sql: str) -> List:
    """
    Extract ORDER BY columns from SQL using sqlglot.
    Returns empty list if parsing fails, no ORDER BY found,
    or ORDER BY contains only non-column expressions (e.g., RANDOM()).
    """
    try:
        parsed = sqlglot.parse_one(sql)
        order = parsed.find(sqlglot.exp.Order)
        if not order:
            return []
        columns = []
        for expr in order.expressions:
            col_name = expr.this.alias_or_name
            if not col_name or isinstance(expr.this, sqlglot.exp.Anonymous):
                continue
            columns.append(
                OrderColumn(
                    column=col_name,
                    direction="DESC" if expr.args.get("desc") else "ASC"
                )
            )
        return columns
    except Exception:
        return []


def _find_primary_key_order(sql: str, datasource_id: int) -> List:
    """
    When no ORDER BY is present, try to auto-detect the table's primary key
    from metadata and use it for cursor pagination.
    """
    try:
        parsed = sqlglot.parse_one(sql)

        from_clause = parsed.find(sqlglot.exp.From)
        if not from_clause:
            return []

        table_expr = from_clause.this
        table_name = table_expr.alias_or_name
        if not table_name:
            return []

        table_obj = models.Table.objects.filter(
            data_source_id=datasource_id,
            name=table_name
        ).first()

        if not table_obj:
            table_obj = models.Table.objects.filter(
                data_source_id=datasource_id,
                public_name=table_name
            ).first()

        if not table_obj:
            return []

        pk_columns = models.TableColumn.objects.filter(
            table=table_obj,
            primary_key=True
        ).values_list('name', flat=True)

        if not pk_columns:
            return []

        return [
            OrderColumn(column=col, direction="ASC")
            for col in pk_columns
        ]
    except Exception:
        return []


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
    order_by: Optional[List[Dict[str, str]]] = None,
    include_count: bool = False
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

        per_page = min(per_page, 500)

        auto_injected_order = False
        if mode == PaginationMode.CURSOR and not order_columns:
            order_columns = _infer_order_from_sql(native_sql)
            if order_columns:
                logger.info(
                    "Cursor mode: auto-detected ORDER BY from SQL: %s",
                    [(o.column, o.direction) for o in order_columns]
                )
            else:
                order_columns = _find_primary_key_order(
                    native_sql, datasource.id
                )
                if order_columns:
                    auto_injected_order = True
                    logger.info(
                        "Cursor mode: auto-injected primary key ORDER BY "
                        "for datasource=%s: %s",
                        datasource.id,
                        [(o.column, o.direction) for o in order_columns]
                    )
                else:
                    logger.info(
                        "Cursor mode: no ORDER BY and no primary key found "
                        "for datasource=%s, falling back to offset.",
                        datasource.id
                    )
                    mode = PaginationMode.OFFSET

        config = PaginationConfig(
            mode=mode,
            page=page,
            per_page=per_page,
            cursor=cursor,
            direction=direction,
            order_by=order_columns if order_columns else [],
            include_count=include_count
        )

        service = PaginationService(
            connector=connector,
            dialect=datasource.dialect_name or datasource.type
        )

        try:
            result = service.paginate(native_sql, config)
        except Exception as cursor_err:
            if auto_injected_order:
                logger.warning(
                    "Cursor pagination failed with auto-injected ORDER BY "
                    "(likely an aggregate/complex query), falling back to "
                    "offset. Error: %s", cursor_err
                )
                config = PaginationConfig(
                    mode=PaginationMode.OFFSET,
                    page=page,
                    per_page=per_page,
                    cursor=None,
                    direction=direction,
                    order_by=[],
                    include_count=include_count
                )
                result = service.paginate(native_sql, config)
            else:
                raise

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
            'table_data': table_data,
            'pagination_mode_used': config.mode.value
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
