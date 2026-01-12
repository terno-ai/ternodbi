"""
Admin Service Views for TernoDBI.

REST API endpoints for managing datasources, tables, and columns.
No authentication required - consuming apps should add their own auth layer.
"""

import json
import logging
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
from django.views.decorators.csrf import csrf_exempt

from dbi_layer.django_app import models
from dbi_layer.services.validation import validate_datasource_input

logger = logging.getLogger(__name__)


# =============================================================================
# DataSource Management
# =============================================================================

@csrf_exempt
@require_http_methods(["POST"])
def create_datasource(request):
    """
    Create a new datasource.
    
    Expects JSON body:
        {
            "display_name": "My Database",
            "type": "postgres",
            "connection_str": "postgresql://...",
            "connection_json": {},  // Optional, for BigQuery
            "description": "..."  // Optional
        }
        
    Returns:
        JSON with created datasource ID
    """
    try:
        body = json.loads(request.body)
        
        # Support both field name styles
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
        
        # Validate connection
        error = validate_datasource_input(db_type, connection_str, connection_json)
        if error:
            return JsonResponse({
                "status": "error",
                "error": f"Connection validation failed: {error}"
            }, status=400)
        
        # Create datasource
        ds = models.DataSource.objects.create(
            display_name=name,
            type=db_type.lower(),
            connection_str=connection_str,
            connection_json=connection_json,
            description=description,
            dialect_name=db_type.lower(),
            enabled=True,
        )
        
        # Auto-sync metadata to discover tables and columns
        from dbi_layer.services.schema_utils import sync_metadata
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
@require_http_methods(["PATCH"])
def update_datasource(request, datasource_id):
    """
    Update a datasource.
    
    Request body:
        {
            "name": "New Name",
            "description": "Updated description",
            "enabled": false
        }
    """
    try:
        ds = models.DataSource.objects.get(id=datasource_id)
    except models.DataSource.DoesNotExist:
        return JsonResponse({
            "status": "error",
            "error": f"DataSource {datasource_id} not found"
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
@require_http_methods(["DELETE"])
def delete_datasource(request, datasource_id):
    """Delete a datasource and all its metadata."""
    try:
        ds = models.DataSource.objects.get(id=datasource_id)
    except models.DataSource.DoesNotExist:
        return JsonResponse({
            "status": "error",
            "error": f"DataSource {datasource_id} not found"
        }, status=404)
    
    name = ds.display_name
    ds.delete()
    
    return JsonResponse({
        "status": "success",
        "message": f"Datasource '{name}' and all its metadata have been deleted"
    })


# =============================================================================
# Table Management
# =============================================================================

@csrf_exempt
@require_http_methods(["PATCH"])
def update_table(request, table_id):
    """
    Update table metadata.
    
    Request body:
        {
            "public_name": "New Display Name",
            "description": "Updated description"
        }
    """
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
                "name": table.name,
                "public_name": table.public_name,
                "description": table.description,
            }
        })
        
    except json.JSONDecodeError:
        return JsonResponse({
            "status": "error",
            "error": "Invalid JSON in request body"
        }, status=400)


# =============================================================================
# Column Management
# =============================================================================

@csrf_exempt
@require_http_methods(["PATCH"])
def update_column(request, column_id):
    """
    Update column metadata.
    
    Request body:
        {
            "public_name": "New Display Name"
        }
    """
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
        
        if updated:
            column.save(update_fields=updated)
        
        return JsonResponse({
            "status": "success",
            "message": f"Column updated: {', '.join(updated)}",
            "column": {
                "id": column.id,
                "name": column.name,
                "public_name": column.public_name,
                "data_type": column.data_type,
            }
        })
        
    except json.JSONDecodeError:
        return JsonResponse({
            "status": "error",
            "error": "Invalid JSON in request body"
        }, status=400)


# =============================================================================
# Suggestions Management
# =============================================================================

@require_http_methods(["GET"])
def list_suggestions(request, datasource_id):
    """List all suggestions for a datasource."""
    try:
        ds = models.DataSource.objects.get(id=datasource_id)
    except models.DataSource.DoesNotExist:
        return JsonResponse({
            "status": "error",
            "error": f"DataSource {datasource_id} not found"
        }, status=404)
    
    suggestions = models.DatasourceSuggestions.objects.filter(data_source=ds)
    data = [{"id": s.id, "suggestion": s.suggestion} for s in suggestions]
    
    return JsonResponse({
        "status": "success",
        "datasource_id": datasource_id,
        "suggestions": data
    })


@csrf_exempt
@require_http_methods(["POST"])
def add_suggestion(request, datasource_id):
    """
    Add a suggestion to a datasource.
    
    Request body:
        {
            "suggestion": "Show me top 10 customers"
        }
    """
    try:
        ds = models.DataSource.objects.get(id=datasource_id)
    except models.DataSource.DoesNotExist:
        return JsonResponse({
            "status": "error",
            "error": f"DataSource {datasource_id} not found"
        }, status=404)
    
    try:
        body = json.loads(request.body)
        suggestion_text = body.get("suggestion")
        
        if not suggestion_text:
            return JsonResponse({
                "status": "error",
                "error": "Missing 'suggestion' in request body"
            }, status=400)
        
        suggestion = models.DatasourceSuggestions.objects.create(
            data_source=ds,
            suggestion=suggestion_text
        )
        
        return JsonResponse({
            "status": "success",
            "suggestion": {
                "id": suggestion.id,
                "suggestion": suggestion.suggestion,
                "datasource_id": ds.id
            }
        }, status=201)
        
    except json.JSONDecodeError:
        return JsonResponse({
            "status": "error",
            "error": "Invalid JSON in request body"
        }, status=400)


@csrf_exempt
@require_http_methods(["DELETE"])
def delete_suggestion(request, suggestion_id):
    """Delete a suggestion."""
    try:
        suggestion = models.DatasourceSuggestions.objects.get(id=suggestion_id)
    except models.DatasourceSuggestions.DoesNotExist:
        return JsonResponse({
            "status": "error",
            "error": f"Suggestion {suggestion_id} not found"
        }, status=404)
    
    suggestion.delete()
    
    return JsonResponse({
        "status": "success",
        "message": "Suggestion deleted"
    })


# =============================================================================
# Connection Validation
# =============================================================================

@csrf_exempt
@require_http_methods(["POST"])
def validate_connection(request):
    """
    Validate a database connection before creating datasource.
    
    Expects JSON body:
        {
            "type": "postgres",
            "connection_str": "postgresql://...",
            "connection_json": {}  # Optional, for BigQuery
        }
        
    Returns:
        JSON with validation result
    """
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
