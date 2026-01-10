"""
Views for DBI Layer Django App.

Provides REST API endpoints for database operations.
"""

import json
import logging
from django.http import JsonResponse
from django.shortcuts import render
from django.views.decorators.http import require_http_methods
from django.views.decorators.csrf import csrf_exempt

from dbi_layer.connectors import ConnectorFactory
from dbi_layer.django_app import models
from dbi_layer.django_app import conf

logger = logging.getLogger(__name__)


# =============================================================================
# Landing Page
# =============================================================================

def landing_page(request):
    """
    Landing page showing TernoDBI capabilities.
    """
    return render(request, 'dbi_layer/landing.html')


# =============================================================================
# Health & Status Endpoints
# =============================================================================


# =============================================================================
# Health & Status Endpoints
# =============================================================================

def health(request):
    """
    Health check endpoint.
    
    Returns:
        JSON response with service status
    """
    return JsonResponse({
        "status": "ok",
        "service": "dbi_layer",
        "version": "1.0.0",
    })


def info(request):
    """
    Service information endpoint.
    
    Returns:
        JSON response with configuration and supported databases
    """
    supported_dbs = ConnectorFactory.get_supported_databases()
    
    return JsonResponse({
        "service": "dbi_layer",
        "version": "1.0.0",
        "supported_databases": supported_dbs,
        "config": {
            "default_page_size": conf.get("DEFAULT_PAGE_SIZE"),
            "max_page_size": conf.get("MAX_PAGE_SIZE"),
            "cache_timeout": conf.get("CACHE_TIMEOUT"),
        }
    })


# =============================================================================
# DataSource Endpoints
# =============================================================================

@require_http_methods(["GET"])
def list_datasources(request):
    """
    List all datasources.
    
    Returns:
        JSON response with list of datasources
    """
    datasources = models.DataSource.objects.all().values(
        'id', 'display_name', 'type', 'created_at'
    )
    
    return JsonResponse({
        "status": "success",
        "count": len(datasources),
        "datasources": list(datasources)
    })


@require_http_methods(["GET"])
def get_datasource(request, datasource_id):
    """
    Get a specific datasource.
    
    Args:
        datasource_id: ID of the datasource
        
    Returns:
        JSON response with datasource details
    """
    try:
        ds = models.DataSource.objects.get(id=datasource_id)
        return JsonResponse({
            "status": "success",
            "datasource": {
                "id": ds.id,
                "display_name": ds.display_name,
                "type": ds.type,
                "dialect": ds.dialect,
                "created_at": ds.created_at.isoformat() if ds.created_at else None,
            }
        })
    except models.DataSource.DoesNotExist:
        return JsonResponse({
            "status": "error",
            "error": f"DataSource {datasource_id} not found"
        }, status=404)


@require_http_methods(["GET"])
def list_tables(request, datasource_id):
    """
    List tables for a datasource.
    
    Args:
        datasource_id: ID of the datasource
        
    Returns:
        JSON response with list of tables
    """
    try:
        ds = models.DataSource.objects.get(id=datasource_id)
        tables = models.Table.objects.filter(data_source=ds).values(
            'id', 'name', 'public_name', 'description'
        )
        
        return JsonResponse({
            "status": "success",
            "datasource_id": datasource_id,
            "count": len(tables),
            "tables": list(tables)
        })
    except models.DataSource.DoesNotExist:
        return JsonResponse({
            "status": "error",
            "error": f"DataSource {datasource_id} not found"
        }, status=404)


@require_http_methods(["GET"])
def list_columns(request, datasource_id, table_id):
    """
    List columns for a table.
    
    Args:
        datasource_id: ID of the datasource
        table_id: ID of the table
        
    Returns:
        JSON response with list of columns
    """
    try:
        table = models.Table.objects.get(id=table_id, data_source_id=datasource_id)
        columns = models.TableColumn.objects.filter(table=table).values(
            'id', 'name', 'public_name', 'data_type'
        )
        
        return JsonResponse({
            "status": "success",
            "table_id": table_id,
            "table_name": table.name,
            "count": len(columns),
            "columns": list(columns)
        })
    except models.Table.DoesNotExist:
        return JsonResponse({
            "status": "error",
            "error": f"Table {table_id} not found in datasource {datasource_id}"
        }, status=404)


# =============================================================================
# Query Endpoints
# =============================================================================

@csrf_exempt
@require_http_methods(["POST"])
def execute_query(request, datasource_id):
    """
    Execute a query against a datasource.
    
    Expects JSON body:
        {
            "sql": "SELECT * FROM users LIMIT 10",
            "page": 1,
            "per_page": 50
        }
        
    Returns:
        JSON response with query results
    """
    from dbi_layer.services.query import execute_native_sql
    
    try:
        ds = models.DataSource.objects.get(id=datasource_id)
    except models.DataSource.DoesNotExist:
        return JsonResponse({
            "status": "error",
            "error": f"DataSource {datasource_id} not found"
        }, status=404)
    
    try:
        body = json.loads(request.body)
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
        
        result = execute_native_sql(ds, sql, page=page, per_page=per_page)
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


# =============================================================================
# Validation Endpoints
# =============================================================================

@csrf_exempt
@require_http_methods(["POST"])
def validate_connection(request):
    """
    Validate a database connection.
    
    Expects JSON body:
        {
            "type": "postgres",
            "connection_string": "postgresql://...",
            "connection_json": {}  # Optional, for BigQuery
        }
        
    Returns:
        JSON response with validation result
    """
    from dbi_layer.services.validation import validate_datasource_input
    
    try:
        body = json.loads(request.body)
        db_type = body.get("type")
        conn_str = body.get("connection_string")
        conn_json = body.get("connection_json")
        
        if not db_type or not conn_str:
            return JsonResponse({
                "status": "error",
                "error": "Missing 'type' or 'connection_string'"
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
