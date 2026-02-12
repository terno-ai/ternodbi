import json
import logging
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
from django.views.decorators.csrf import csrf_exempt

from terno_dbi.core import models
from terno_dbi.core import conf
from terno_dbi.services.query import (
    execute_native_sql,
    execute_paginated_query,
    export_native_sql_result
)
from terno_dbi.services.shield import prepare_mdb, generate_native_sql
from terno_dbi.services.access import get_admin_config_object
from terno_dbi.services.resolver import resolve_datasource
from terno_dbi.decorators import require_service_auth

logger = logging.getLogger(__name__)


def health(request):
    return JsonResponse({
        "status": "ok",
        "service": "terno_dbi.query_service",
        "version": "1.0.0",
    })


def info(request):
    from terno_dbi.connectors import ConnectorFactory

    return JsonResponse({
        "service": "terno_dbi.query_service",
        "version": "1.0.0",
        "supported_databases": ConnectorFactory.get_supported_databases(),
    })



@require_service_auth()
@require_http_methods(["GET"])
def list_datasources(request):
    datasources = request.allowed_datasources
    logger.debug("List datasources requested: count=%d", len(datasources))

    data = []
    for ds in datasources:
        data.append({
            'id': ds.id,
            'name': ds.display_name,
            'type': ds.type,
            'description': ds.description,
            'is_erp': ds.is_erp,
            'dialect_name': ds.dialect_name,
            'dialect_version': ds.dialect_version,
        })

    return JsonResponse({
        "status": "success",
        "datasources": data,
        "count": len(data)
    })



@require_service_auth()
@require_http_methods(["GET"])
def list_tables(request, datasource_identifier):
    ds = request.resolved_datasource
    logger.debug("List tables requested: datasource='%s'", ds.display_name)

    role_ids_str = request.GET.get('roles', '')

    if role_ids_str:
        from django.contrib.auth.models import Group
        role_ids = [int(r) for r in role_ids_str.split(',') if r.strip()]
        roles = Group.objects.filter(id__in=role_ids)
        tables, columns = get_admin_config_object(ds, roles)
    else:
        tables = models.Table.objects.filter(data_source=ds)
        columns = models.TableColumn.objects.filter(table__in=tables)

    tables_list = []
    for table in tables:
        tables_list.append({
            'id': table.id,
            'name': table.public_name,
            'description': table.description or ""
        })

    table_data = []
    for table in tables:
        table_columns = columns.filter(table_id=table.id)
        column_data = list(table_columns.values('public_name', 'data_type'))
        table_data.append({
            'table_name': table.public_name,
            'table_description': table.description,
            'column_data': column_data
        })

    return JsonResponse({
        'status': 'success',
        'tables': tables_list,
        'table_data': table_data,

    })


@require_service_auth()
@require_http_methods(["GET"])
def list_table_columns(request, datasource_identifier, table_identifier):
    ds = request.resolved_datasource

    try:
        table = models.Table.objects.get(id=int(table_identifier), data_source=ds)
    except (ValueError, models.Table.DoesNotExist):
        try:
            table = models.Table.objects.get(public_name=table_identifier, data_source=ds)
        except models.Table.DoesNotExist:
            return JsonResponse({
                "status": "error",
                "error": f"Table '{table_identifier}' not found"
            }, status=404)

    columns = models.TableColumn.objects.filter(table=table)

    return JsonResponse({
        "status": "success",
        "table_id": table.id,
        "table_name": table.public_name,
        "columns": [
            {
                'id': c.id,
                'name': c.public_name,
                'data_type': c.data_type,
                'description': c.description or ""
            }
            for c in columns
        ]
    })



@require_service_auth()
@require_http_methods(["GET"])
def list_foreign_keys(request, datasource_identifier):
    ds = request.resolved_datasource
    logger.debug("List foreign keys requested: datasource='%s'", ds.display_name)

    fks = models.ForeignKey.objects.filter(
        constrained_table__data_source=ds
    ).select_related('constrained_table', 'referred_table')

    fk_data = []
    for fk in fks:
        fk_data.append({
            "id": fk.id,
            "constrained_table": fk.constrained_table.public_name,
            "constrained_column": fk.constrained_columns.public_name if fk.constrained_columns else None,
            "referred_table": fk.referred_table.public_name,
            "referred_column": fk.referred_columns.public_name if fk.referred_columns else None,
        })

    return JsonResponse({
        "status": "success",
        "datasource_id": datasource_identifier,
        "foreign_keys": fk_data
    })


@require_service_auth()
@require_http_methods(["GET"])
def get_sample_data(request, table_id):
    table = request.resolved_table
    ds = table.data_source
    logger.info("Sample data requested: table='%s', datasource='%s'", table.public_name, ds.display_name)

    try:
        rows = int(request.GET.get("rows", 10))
        rows = min(rows, 100)
    except ValueError:
        rows = 10

    sql = f"SELECT * FROM {table.name} LIMIT {rows}"

    try:
        result = execute_native_sql(ds, sql, page=1, per_page=rows)

        if result.get("status") == "error":
            return JsonResponse({
                "status": "error",
                "error": result.get("error", "Query failed")
            }, status=500)

        table_data = result.get("table_data", {})
        return JsonResponse({
            "status": "success",
            "table_id": table_id,
            "columns": table_data.get("columns", []),
            "data": table_data.get("data", [])
        })
    except Exception as e:
        logger.exception(f"Sample data error: {e}")
        return JsonResponse({
            "status": "error",
            "error": str(e)
        }, status=500)



@csrf_exempt
@require_service_auth()
@require_http_methods(["POST"])
def execute_query(request, datasource_identifier=None):
    try:
        body = json.loads(request.body)

        if datasource_identifier and hasattr(request, 'resolved_datasource'):
            ds = request.resolved_datasource
        else:
            ds_identifier = body.get("datasource") or body.get("datasourceId")
            if not ds_identifier:
                return JsonResponse({
                    "status": "error",
                    "error": "Datasource name or ID required"
                }, status=400)

            try:
                ds = resolve_datasource(ds_identifier)

                if not request.allowed_datasources.filter(id=ds.id).exists():
                    return JsonResponse({
                        "status": "error",
                        "error": "Access denied to datasource"
                    }, status=403)
            except Exception as e:
                return JsonResponse({
                    "status": "error",
                    "error": str(e)
                }, status=404)

        sql = body.get("sql")
        if not sql:
            return JsonResponse({
                "status": "error",
                "error": "Missing 'sql' in request body"
            }, status=400)

        pagination_mode = body.get("pagination_mode", "offset")
        page = body.get("page", 1)
        per_page = min(
            body.get("per_page", conf.get("DEFAULT_PAGE_SIZE")),
            conf.get("MAX_PAGE_SIZE")
        )
        cursor = body.get("cursor")
        direction = body.get("direction", "forward")
        order_by = body.get("order_by")  # List of {"column": "name", "direction": "DESC"}

        role_ids = body.get("roles", [])
        from django.contrib.auth.models import Group

        if role_ids:
            roles = Group.objects.filter(id__in=role_ids)
        else:
            roles = Group.objects.none()

        mDb = prepare_mdb(ds, roles)
        logger.info("Execute query: datasource='%s', pagination=%s", ds.display_name, pagination_mode)
        transform_result = generate_native_sql(mDb, sql, ds.dialect_name)

        if transform_result.get('status') == 'error':
            return JsonResponse({
                "status": "error",
                "error": transform_result.get('error', 'SQL transformation failed')
            }, status=400)

        native_sql = transform_result.get('native_sql', sql)
        logger.debug("Resolved Native SQL: %s", native_sql)

        # Use new paginated query API
        result = execute_paginated_query(
            datasource=ds,
            native_sql=native_sql,
            pagination_mode=pagination_mode,
            page=page,
            per_page=per_page,
            cursor=cursor,
            direction=direction,
            order_by=order_by
        )
        return JsonResponse(result)

    except json.JSONDecodeError:
        return JsonResponse({
            "status": "error",
            "error": "Invalid JSON in request body"
        }, status=400)
    except Exception as e:
        logger.exception(f"Query execution error: {e}")
        return JsonResponse({
            "status": "error",
            "error": str(e)
        }, status=500)


@csrf_exempt
@require_service_auth()
@require_http_methods(["POST"])
def export_query(request, datasource_identifier=None):
    try:
        body = json.loads(request.body)

        if datasource_identifier and hasattr(request, 'resolved_datasource'):
            ds = request.resolved_datasource
        else:
            ds_identifier = body.get("datasource") or body.get("datasourceId")
            if not ds_identifier:
                return JsonResponse({
                    "status": "error",
                    "error": "Datasource name or ID required"
                }, status=400)

            try:
                ds = resolve_datasource(ds_identifier)
                if not request.allowed_datasources.filter(id=ds.id).exists():
                    return JsonResponse({
                        "status": "error",
                        "error": "Access denied to datasource"
                    }, status=403)
            except Exception as e:
                return JsonResponse({
                    "status": "error",
                    "error": str(e)
                }, status=404)

        sql = body.get("sql")
        if not sql:
            return JsonResponse({
                "status": "error",
                "error": "Missing 'sql' in request body"
            }, status=400)
        role_ids = body.get("roles", [])
        from django.contrib.auth.models import Group

        if role_ids:
            roles = Group.objects.filter(id__in=role_ids)
        else:
            roles = Group.objects.none()

        mDb = prepare_mdb(ds, roles)
        transform_result = generate_native_sql(mDb, sql, ds.dialect_name)

        if transform_result.get('status') == 'error':
            return JsonResponse({
                "status": "error",
                "error": transform_result.get('error', 'SQL transformation failed')
            }, status=400)

        native_sql = transform_result.get('native_sql', sql)
        logger.info("Export query: datasource='%s'", ds.display_name)
        return export_native_sql_result(ds, native_sql)

    except json.JSONDecodeError:
        return JsonResponse({
            "status": "error",
            "error": "Invalid JSON in request body"
        }, status=400)
    except Exception as e:
        logger.exception(f"Export error: {e}")
        return JsonResponse({
            "status": "error",
            "error": str(e)
        }, status=500)
