import json
import logging
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
from django.views.decorators.csrf import csrf_exempt

from terno_dbi.core import models
from terno_dbi.services.validation import validate_datasource_input
from terno_dbi.services.resolver import resolve_datasource
from terno_dbi.decorators import require_service_auth, require_scope
from terno_dbi.core.models import ServiceToken

logger = logging.getLogger(__name__)

@csrf_exempt
@require_service_auth()
@require_scope('admin:write')
@require_http_methods(["POST"])
def create_datasource(request):
    try:
        body = json.loads(request.body)
        name = body.get("display_name") or body.get("name")
        db_type = body.get("type")
        connection_str = body.get("connection_str") or body.get("connection_string")
        connection_json = body.get("connection_json")
        description = body.get("description", "")

        logger.info("Create datasource request: name='%s', type='%s'", name, db_type)

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

        organisation = getattr(request, 'token_organisation', None)

        ds = models.DataSource.objects.create(
            display_name=name,
            type=db_type.lower(),
            connection_str=connection_str,
            connection_json=connection_json,
            description=description,
            dialect_name=db_type.lower(),
            organisation=organisation,
            enabled=True,
        )
        logger.info("Datasource created: id=%d, name='%s', type='%s'", ds.id, ds.display_name, ds.type)
        
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
@require_service_auth()
@require_scope('admin:write')
@require_http_methods(["PATCH"])
def update_datasource(request, datasource_identifier):
    ds = request.resolved_datasource
    logger.info("Update datasource request: id=%d, name='%s'", ds.id, ds.display_name)

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
@require_service_auth()
@require_scope('admin:write')
@require_http_methods(["DELETE"])
def delete_datasource(request, datasource_identifier):
    ds = request.resolved_datasource
    logger.info("Delete datasource request: id=%d, name='%s'", ds.id, ds.display_name)

    name = ds.display_name
    ds.delete()

    return JsonResponse({
        "status": "success",
        "message": f"Datasource '{name}' and all its metadata have been deleted"
    })


@csrf_exempt
@require_service_auth()
@require_scope('admin:write')
@require_http_methods(["PATCH"])
def update_table(request, table_id):
    table = request.resolved_table
    logger.info("Update table request: id=%d, name='%s'", table.id, table.public_name)

    try:
        body = json.loads(request.body)

        updated = []
        if "public_name" in body:
            val = body["public_name"]
            if val is not None and isinstance(val, str) and not val.strip():
                return JsonResponse({
                    "status": "error",
                    "error": "public_name cannot be an empty string. Use null to clear it."
                }, status=400)
            table.public_name = val
            updated.append("public_name")
        if "description" in body:
            table.description = body["description"]
            updated.append("description")
        if "is_hidden" in body:
            table.is_hidden = bool(body["is_hidden"])
            updated.append("is_hidden")

        if updated:
            table.save(update_fields=updated + ["metadata_updated_at"])
            # Invalidate cache so changes are reflected immediately
            from terno_dbi.services.shield import delete_cache
            delete_cache(table.data_source)

        return JsonResponse({
            "status": "success",
            "message": f"Table updated: {', '.join(updated)}",
            "table": {
                "id": table.id,
                "name": table.public_name,
                "description": table.description,
                "is_hidden": table.is_hidden,
            }
        })

    except json.JSONDecodeError:
        return JsonResponse({
            "status": "error",
            "error": "Invalid JSON in request body"
        }, status=400)


@csrf_exempt
@require_service_auth()
@require_scope('admin:write')
@require_http_methods(["PATCH"])
def update_column(request, column_id):
    column = request.resolved_column
    logger.info("Update column request: id=%d, name='%s'", column.id, column.public_name)

    try:
        body = json.loads(request.body)

        updated = []
        if "public_name" in body:
            val = body["public_name"]
            if val is not None and isinstance(val, str) and not val.strip():
                return JsonResponse({
                    "status": "error",
                    "error": "public_name cannot be an empty string. Use null to clear it."
                }, status=400)
            column.public_name = val
            updated.append("public_name")
        if "description" in body:
            column.description = body["description"]
            updated.append("description")
        if "is_hidden" in body:
            column.is_hidden = bool(body["is_hidden"])
            updated.append("is_hidden")

        if updated:
            column.save(update_fields=updated + ["metadata_updated_at"])
            # Invalidate cache so changes are reflected immediately
            from terno_dbi.services.shield import delete_cache
            delete_cache(column.table.data_source)

        return JsonResponse({
            "status": "success",
            "message": f"Column updated: {', '.join(updated)}",
            "column": {
                "id": column.id,
                "name": column.public_name,
                "data_type": column.data_type,
                "description": column.description,
                "is_hidden": column.is_hidden,
            }
        })

    except json.JSONDecodeError:
        return JsonResponse({
            "status": "error",
            "error": "Invalid JSON in request body"
        }, status=400)

@require_service_auth()
@require_scope('admin:read')
@require_http_methods(["GET"])
def list_hidden(request, datasource_identifier):
    ds = request.resolved_datasource
    logger.info("List hidden request: datasource_id=%d", ds.id)

    hidden_tables = models.Table.objects.filter(
        data_source=ds, is_hidden=True
    ).values('id', 'name', 'public_name', 'description', 'metadata_updated_at')

    hidden_columns = models.TableColumn.objects.filter(
        table__data_source=ds, is_hidden=True
    ).select_related('table').values(
        'id', 'name', 'public_name', 'data_type', 'table__name', 'metadata_updated_at'
    )

    return JsonResponse({
        "status": "success",
        "datasource_id": ds.id,
        "hidden_tables": [
            {
                "id": t["id"],
                "name": t["name"],
                "public_name": t["public_name"],
                "description": t["description"],
                "hidden_at": t["metadata_updated_at"].isoformat() if t["metadata_updated_at"] else None,
            }
            for t in hidden_tables
        ],
        "hidden_columns": [
            {
                "id": c["id"],
                "name": c["name"],
                "public_name": c["public_name"],
                "data_type": c["data_type"],
                "table": c["table__name"],
                "hidden_at": c["metadata_updated_at"].isoformat() if c["metadata_updated_at"] else None,
            }
            for c in hidden_columns
        ],
    })


@csrf_exempt
@require_service_auth()
@require_scope('admin:write')
@require_http_methods(["POST"])
def validate_connection(request):
    try:
        body = json.loads(request.body)
        db_type = body.get("type")
        conn_str = body.get("connection_str") or body.get("connection_string")
        conn_json = body.get("connection_json")

        logger.debug("Validate connection request: type='%s'", db_type)

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
@require_service_auth()
@require_scope('admin:sync')
@require_http_methods(["POST"])
def sync_metadata(request, datasource_identifier):
    ds = request.resolved_datasource

    try:
        body = json.loads(request.body)
        overwrite = body.get("overwrite", False)
    except json.JSONDecodeError:
        overwrite = False

    try:
        from terno_dbi.services.schema_utils import sync_metadata
        logger.info("Starting metadata sync: datasource_id=%d, overwrite=%s", ds.id, overwrite)
        sync_result = sync_metadata(ds.id, overwrite)
        logger.info("Metadata sync completed: datasource_id=%d, tables=%d", ds.id, sync_result.get('tables_created', 0))

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


@require_service_auth()
@require_scope('admin:read')
@require_http_methods(["GET"])
def get_table_info(request, datasource_identifier, table_name):
    ds = request.resolved_datasource
    logger.debug("Get table info: datasource='%s', table='%s'", ds.display_name, table_name)

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


