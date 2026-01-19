import json
import logging
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
from django.views.decorators.csrf import csrf_exempt
from django.shortcuts import get_object_or_404

from dbi_layer.django_app import models
from dbi_layer.django_app import conf
from dbi_layer.services.query import execute_native_sql, export_native_sql_result
from dbi_layer.services.shield import prepare_mdb, generate_native_sql
from dbi_layer.services.access import get_admin_config_object
from dbi_layer.decorators import require_service_auth

logger = logging.getLogger(__name__)


def health(request):
    return JsonResponse({
        "status": "ok",
        "service": "dbi_layer.query_service",
        "version": "1.0.0",
    })


def info(request):
    from dbi_layer.connectors import ConnectorFactory

    return JsonResponse({
        "service": "dbi_layer.query_service",
        "version": "1.0.0",
        "supported_databases": ConnectorFactory.get_supported_databases(),
    })



@require_service_auth()
@require_http_methods(["GET"])
def list_datasources(request):
    datasources = models.DataSource.objects.filter(enabled=True)

    data = []
    for ds in datasources:
        suggestions = list(
            models.DatasourceSuggestions.objects.filter(data_source=ds)
            .values_list('suggestion', flat=True)
        )
        data.append({
            'id': ds.id,
            'name': ds.display_name,
            'type': ds.type,
            'suggestions': suggestions,
        })

    return JsonResponse({
        "status": "success",
        "datasources": data
    })


@require_service_auth()
@require_http_methods(["GET"])
def get_datasource(request, datasource_id):
    try:
        ds = models.DataSource.objects.get(id=datasource_id, enabled=True)
        return JsonResponse({
            "status": "success",
            "datasource_name": ds.display_name,
            "type": ds.type,
        })
    except models.DataSource.DoesNotExist:
        return JsonResponse({
            "status": "error",
            "error": f"DataSource {datasource_id} not found"
        }, status=404)


@require_service_auth()
@require_http_methods(["GET"])
def list_tables(request, datasource_id):
    try:
        ds = models.DataSource.objects.get(id=datasource_id, enabled=True)
    except models.DataSource.DoesNotExist:
        return JsonResponse({
            "status": "error",
            "error": f"DataSource {datasource_id} not found"
        }, status=404)

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

    suggestions = list(
        models.DatasourceSuggestions.objects.filter(data_source=ds)
        .values_list('suggestion', flat=True)
    )

    return JsonResponse({
        'status': 'success',
        'tables': tables_list,
        'table_data': table_data,
        'suggestions': suggestions
    })


@require_service_auth()
@require_http_methods(["GET"])
def list_columns(request, datasource_id, table_id):
    return get_table_columns(request, table_id)


@require_service_auth()
@require_http_methods(["GET"])
def get_table_columns(request, table_id):
    try:
        table = models.Table.objects.get(id=table_id)
    except models.Table.DoesNotExist:
        return JsonResponse({
            "status": "error",
            "error": f"Table {table_id} not found"
        }, status=404)

    columns = models.TableColumn.objects.filter(table=table)

    return JsonResponse({
        "status": "success",
        "table_id": table_id,
        "table_name": table.public_name,
        "columns": [
            {
                'id': c.id,
                'name': c.public_name,
                'data_type': c.data_type
            }
            for c in columns
        ]
    })


@require_service_auth()
@require_http_methods(["GET"])
def get_schema(request, datasource_id):
    try:
        datasource = models.DataSource.objects.get(id=datasource_id)
    except models.DataSource.DoesNotExist:
        return JsonResponse({"error": "Datasource not found"}, status=404)

    tables = models.Table.objects.filter(data_source=datasource)
    schema = []

    for table in tables:
        columns = models.TableColumn.objects.filter(table=table)
        schema.append({
            "id": table.id,
            "table_name": table.public_name,
            "description": table.description or "",
            "columns": [
                {
                    "id": c.id,
                    "name": c.public_name,
                    "type": c.data_type
                }
                for c in columns
            ]
        })

    return JsonResponse({
        "datasource": datasource.display_name,
        "schema": schema,
        "table_count": len(schema)
    })


@require_service_auth()
@require_http_methods(["GET"])
def list_foreign_keys(request, datasource_id):
    try:
        ds = models.DataSource.objects.get(id=datasource_id, enabled=True)
    except models.DataSource.DoesNotExist:
        return JsonResponse({
            "status": "error",
            "error": f"DataSource {datasource_id} not found"
        }, status=404)

    fks = models.ForeignKey.objects.filter(
        constrained_table__data_source=ds
    ).select_related('constrained_table', 'referred_table')

    fk_data = []
    for fk in fks:
        fk_data.append({
            "id": fk.id,
            "constrained_table": fk.constrained_table.public_name,  # Use public_name
            "constrained_column": fk.constrained_columns.public_name if fk.constrained_columns else None,
            "referred_table": fk.referred_table.public_name,  # Use public_name
            "referred_column": fk.referred_columns.public_name if fk.referred_columns else None,
        })

    return JsonResponse({
        "status": "success",
        "datasource_id": datasource_id,
        "foreign_keys": fk_data
    })


@require_service_auth()
@require_http_methods(["GET"])
def get_sample_data(request, table_id):
    try:
        table = models.Table.objects.get(id=table_id)
        ds = table.data_source
    except models.Table.DoesNotExist:
        return JsonResponse({
            "status": "error",
            "error": f"Table {table_id} not found"
        }, status=404)

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
def execute_query(request, datasource_id=None):
    try:
        body = json.loads(request.body)
        ds_id = datasource_id or body.get("datasourceId")
        if not ds_id:
            return JsonResponse({
                "status": "error",
                "error": "Datasource ID required"
            }, status=400)

        try:
            ds = models.DataSource.objects.get(id=ds_id, enabled=True)
        except models.DataSource.DoesNotExist:
            return JsonResponse({
                "status": "error",
                "error": f"DataSource {ds_id} not found"
            }, status=404)
        
        sql = body.get("sql")
        page = body.get("page", 1)
        per_page = min(
            body.get("per_page", conf.get("DEFAULT_PAGE_SIZE")),
            conf.get("MAX_PAGE_SIZE")
        )

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
        result = execute_native_sql(ds, native_sql, page=page, per_page=per_page)
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
def export_query(request, datasource_id=None):
    try:
        body = json.loads(request.body)
        ds_id = datasource_id or body.get("datasourceId")
        if not ds_id:
            return JsonResponse({
                "status": "error",
                "error": "Datasource ID required"
            }, status=400)

        try:
            ds = models.DataSource.objects.get(id=ds_id, enabled=True)
        except models.DataSource.DoesNotExist:
            return JsonResponse({
                "status": "error",
                "error": f"DataSource {ds_id} not found"
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
