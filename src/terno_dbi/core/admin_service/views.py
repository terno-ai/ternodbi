import json
import logging
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
from django.views.decorators.csrf import csrf_exempt

from terno_dbi.core import models
from terno_dbi.services.validation import validate_datasource_input
from terno_dbi.services.resolver import resolve_datasource
from terno_dbi.decorators import require_service_auth
from terno_dbi.core.models import ServiceToken

logger = logging.getLogger(__name__)

@csrf_exempt
@require_service_auth(allowed_types=[ServiceToken.TokenType.ADMIN])
@require_http_methods(["POST"])
def create_datasource(request):
    try:
        body = json.loads(request.body)
        name = body.get("display_name") or body.get("name")
        db_type = body.get("type")
        connection_str = body.get("connection_str") or body.get("connection_string")
        connection_json = body.get("connection_json")
        description = body.get("description", "")

        if not name or not db_type or not connection_str:
            return JsonResponse({
                "status": "error",
                "error": "Missing required fields: display_name, type, connection_str"
            }, status=400)

        # BigQuery special handling
        if db_type.lower() == 'bigquery':
            if not connection_json:
                return JsonResponse({
                    "status": "error",
                    "error": "connection_json is required for BigQuery"
                }, status=400)
            if isinstance(connection_json, str):
                try:
                    connection_json = json.loads(connection_json)
                except json.JSONDecodeError:
                    return JsonResponse({
                        "status": "error",
                        "error": "Invalid connection_json format"
                    }, status=400)
        error = validate_datasource_input(db_type, connection_str, connection_json)
        if error:
            return JsonResponse({
                "status": "error",
                "error": f"Connection validation failed: {error}"
            }, status=400)
        ds = models.DataSource.objects.create(
            display_name=name,
            type=db_type.lower(),
            connection_str=connection_str,
            connection_json=connection_json,
            description=description,
            dialect_name=db_type.lower(),
            enabled=True,
        )
        from terno_dbi.services.schema_utils import sync_metadata
        sync_result = sync_metadata(ds.id)

        return JsonResponse({
            "status": "success",
            "datasource_id": ds.id,
            "datasource": {
                "id": ds.id,
                "name": ds.display_name,
                "type": ds.type,
                "enabled": ds.enabled,
            },
            "sync_result": {
                "tables_created": sync_result.get("tables_created", 0),
                "columns_created": sync_result.get("columns_created", 0),
            }
        }, status=201)

    except json.JSONDecodeError:
        return JsonResponse({
            "status": "error",
            "error": "Invalid JSON in request body"
        }, status=400)
    except Exception as e:
        logger.exception(f"Datasource creation error: {e}")
        return JsonResponse({
            "status": "error",
            "error": str(e)
        }, status=500)


@csrf_exempt
@require_service_auth(allowed_types=[ServiceToken.TokenType.ADMIN])
@require_http_methods(["PATCH"])
def update_datasource(request, datasource_identifier):
    try:
        ds = resolve_datasource(datasource_identifier, enabled_only=False)
    except Exception as e:
        return JsonResponse({
            "status": "error",
            "error": str(e)
        }, status=404)

    try:
        body = json.loads(request.body)

        updated = []
        if "name" in body or "display_name" in body:
            ds.display_name = body.get("name") or body.get("display_name")
            updated.append("display_name")
        if "description" in body:
            ds.description = body["description"]
            updated.append("description")
        if "enabled" in body:
            ds.enabled = body["enabled"]
            updated.append("enabled")

        if updated:
            ds.save(update_fields=updated)

        return JsonResponse({
            "status": "success",
            "message": f"Datasource updated: {', '.join(updated)}",
            "datasource": {
                "id": ds.id,
                "name": ds.display_name,
                "type": ds.type,
                "description": ds.description,
                "enabled": ds.enabled,
            }
        })

    except json.JSONDecodeError:
        return JsonResponse({
            "status": "error",
            "error": "Invalid JSON in request body"
        }, status=400)


@csrf_exempt
@require_service_auth(allowed_types=[ServiceToken.TokenType.ADMIN])
@require_http_methods(["DELETE"])
def delete_datasource(request, datasource_identifier):
    try:
        ds = resolve_datasource(datasource_identifier, enabled_only=False)
    except Exception as e:
        return JsonResponse({
            "status": "error",
            "error": str(e)
        }, status=404)

    name = ds.display_name
    ds.delete()

    return JsonResponse({
        "status": "success",
        "message": f"Datasource '{name}' and all its metadata have been deleted"
    })


@csrf_exempt
@require_service_auth(allowed_types=[ServiceToken.TokenType.ADMIN])
@require_http_methods(["PATCH"])
def update_table(request, table_id):
    try:
        table = models.Table.objects.get(id=table_id)
    except models.Table.DoesNotExist:
        return JsonResponse({
            "status": "error",
            "error": f"Table {table_id} not found"
        }, status=404)

    try:
        body = json.loads(request.body)

        updated = []
        if "public_name" in body:
            table.public_name = body["public_name"]
            updated.append("public_name")
        if "description" in body:
            table.description = body["description"]
            updated.append("description")

        if updated:
            table.save(update_fields=updated)

        return JsonResponse({
            "status": "success",
            "message": f"Table updated: {', '.join(updated)}",
            "table": {
                "id": table.id,
                "name": table.public_name,
                "description": table.description,
            }
        })

    except json.JSONDecodeError:
        return JsonResponse({
            "status": "error",
            "error": "Invalid JSON in request body"
        }, status=400)


@csrf_exempt
@require_service_auth(allowed_types=[ServiceToken.TokenType.ADMIN])
@require_http_methods(["PATCH"])
def update_column(request, column_id):
    try:
        column = models.TableColumn.objects.get(id=column_id)
    except models.TableColumn.DoesNotExist:
        return JsonResponse({
            "status": "error",
            "error": f"Column {column_id} not found"
        }, status=404)

    try:
        body = json.loads(request.body)

        updated = []
        if "public_name" in body:
            column.public_name = body["public_name"]
            updated.append("public_name")
        if "description" in body:
            column.description = body["description"]
            updated.append("description")

        if updated:
            column.save(update_fields=updated)

        return JsonResponse({
            "status": "success",
            "message": f"Column updated: {', '.join(updated)}",
            "column": {
                "id": column.id,
                "name":column.public_name,
                "data_type": column.data_type,
            }
        })

    except json.JSONDecodeError:
        return JsonResponse({
            "status": "error",
            "error": "Invalid JSON in request body"
        }, status=400)





    except json.JSONDecodeError:
        return JsonResponse({
            "status": "error",
            "error": "Invalid JSON in request body"
        }, status=400)




@csrf_exempt
@require_service_auth(allowed_types=[ServiceToken.TokenType.ADMIN])
@require_http_methods(["POST"])
def validate_connection(request):
    try:
        body = json.loads(request.body)
        db_type = body.get("type")
        conn_str = body.get("connection_str") or body.get("connection_string")
        conn_json = body.get("connection_json")

        if not db_type or not conn_str:
            return JsonResponse({
                "status": "error",
                "error": "Missing 'type' or 'connection_str'"
            }, status=400)

        error = validate_datasource_input(db_type, conn_str, conn_json)

        if error:
            return JsonResponse({
                "status": "error",
                "valid": False,
                "error": error
            })
        else:
            return JsonResponse({
                "status": "success",
                "valid": True,
                "message": "Connection validated successfully"
            })

    except json.JSONDecodeError:
        return JsonResponse({
            "status": "error",
            "error": "Invalid JSON in request body"
        }, status=400)
    except Exception as e:
        logger.exception(f"Validation error: {e}")
        return JsonResponse({
            "status": "error",
            "error": str(e)
        }, status=500)


@csrf_exempt
@require_service_auth(allowed_types=[ServiceToken.TokenType.ADMIN])
@require_http_methods(["POST"])
def sync_metadata(request, datasource_identifier):
    try:
        ds = resolve_datasource(datasource_identifier, enabled_only=False)
    except Exception as e:
        return JsonResponse({
            "status": "error",
            "error": str(e)
        }, status=404)

    try:
        body = json.loads(request.body)
        overwrite = body.get("overwrite", False)
    except json.JSONDecodeError:
        overwrite = False

    try:
        from terno_dbi.services.schema_utils import sync_metadata
        sync_result = sync_metadata(ds.id, overwrite)

        return JsonResponse({
            "status": "success",
            "datasource_id": ds.id,
            "sync_result": sync_result
        })
    except Exception as e:
        logger.exception(f"Sync metadata error: {e}")
        return JsonResponse({
            "status": "error",
            "error": str(e)
        }, status=500)


@require_service_auth(allowed_types=[ServiceToken.TokenType.ADMIN])
@require_http_methods(["GET"])
def get_table_info(request, datasource_identifier, table_name):
    try:
        ds = resolve_datasource(datasource_identifier)
    except Exception as e:
        return JsonResponse({"error": str(e)}, status=404)

    try:
        table = models.Table.objects.get(data_source=ds, name=table_name)
    except models.Table.DoesNotExist:
        return JsonResponse({"error": f"Table {table_name} not found"}, status=404)

    columns = models.TableColumn.objects.filter(table=table).values(
        'public_name', 'data_type', 'description'
    )
    try:
        from terno_dbi.services.query import execute_native_sql
        sql = f"SELECT * FROM {table.name} LIMIT 3" 
        sample_result = execute_native_sql(ds, sql, page=1, per_page=3)
        sample_rows = sample_result.get('data', [])
    except Exception as e:
        logger.warning(f"Could not fetch sample data for {table_name}: {e}")
        sample_rows = []

    return JsonResponse({
        "status": "success",
        "table": {
            "name": table.public_name,
            "description": table.description,
            "columns": list(columns),
            "sample_rows": sample_rows
        }
    })


@csrf_exempt
@require_service_auth(allowed_types=[ServiceToken.TokenType.ADMIN])
@require_http_methods(["POST"])
def get_all_tables_info(request, datasource_identifier):
    try:
        ds = resolve_datasource(datasource_identifier)
    except Exception as e:
        return JsonResponse({"error": str(e)}, status=404)

    try:
        body = json.loads(request.body)
        table_names = body.get("table_names", [])
    except:
        table_names = []

    qs = models.Table.objects.filter(data_source=ds)
    if table_names:
        qs = qs.filter(name__in=table_names)

    qs = qs.prefetch_related('columns')

    results = []

    for table in qs:
        columns = list(table.columns.all().values('public_name', 'data_type', 'description'))
        results.append({
            "name": table.public_name,
            "description": table.description,
            "columns": columns
        })

    return JsonResponse({
        "status": "success",
        "tables": results
    })
