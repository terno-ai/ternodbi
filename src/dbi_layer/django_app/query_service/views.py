"""
Query Service Views for TernoDBI.

Simple REST API endpoints for database operations.
No authentication required - consuming apps (like TernoAI) should add their own auth layer.
"""

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

logger = logging.getLogger(__name__)


# =============================================================================
# Health & Info Endpoints
# =============================================================================

def health(request):
    """Health check endpoint."""
    return JsonResponse({
        "status": "ok",
        "service": "dbi_layer.query_service",
        "version": "1.0.0",
    })


def info(request):
    """Service information endpoint."""
    from dbi_layer.connectors import ConnectorFactory
    
    return JsonResponse({
        "service": "dbi_layer.query_service",
        "version": "1.0.0",
        "supported_databases": ConnectorFactory.get_supported_databases(),
    })


# =============================================================================
# DataSource Endpoints
# =============================================================================

@require_http_methods(["GET"])
def list_datasources(request):
    """
    List all enabled datasources.
    
    Returns:
        JSON with list of datasources
    """
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


@require_http_methods(["GET"])
def get_datasource(request, datasource_id):
    """
    Get a specific datasource.
    
    Args:
        datasource_id: ID of the datasource
        
    Returns:
        JSON with datasource details
    """
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


# =============================================================================
# Table Endpoints
# =============================================================================

@require_http_methods(["GET"])
def list_tables(request, datasource_id):
    """
    List tables for a datasource.
    
    Optional query param:
        - roles: comma-separated group IDs for role-based filtering
    
    Returns:
        JSON with list of tables and their columns
    """
    try:
        ds = models.DataSource.objects.get(id=datasource_id, enabled=True)
    except models.DataSource.DoesNotExist:
        return JsonResponse({
            "status": "error",
            "error": f"DataSource {datasource_id} not found"
        }, status=404)
    
    # Get role IDs from query param (optional)
    role_ids_str = request.GET.get('roles', '')
    
    if role_ids_str:
        from django.contrib.auth.models import Group
        role_ids = [int(r) for r in role_ids_str.split(',') if r.strip()]
        roles = Group.objects.filter(id__in=role_ids)
        tables, columns = get_admin_config_object(ds, roles)
    else:
        # No role filtering - return all tables
        tables = models.Table.objects.filter(data_source=ds)
        columns = models.TableColumn.objects.filter(table__in=tables)
    
    # Build table_data
    table_data = []
    for table in tables:
        table_columns = columns.filter(table_id=table.id)
        column_data = list(table_columns.values('public_name', 'data_type'))
        table_data.append({
            'table_name': table.public_name,
            'table_description': table.description,
            'column_data': column_data
        })
    
    # Get suggestions
    suggestions = list(
        models.DatasourceSuggestions.objects.filter(data_source=ds)
        .values_list('suggestion', flat=True)
    )
    
    return JsonResponse({
        'status': 'success',
        'table_data': table_data,
        'suggestions': suggestions
    })


@require_http_methods(["GET"])
def list_columns(request, datasource_id, table_id):
    """List columns for a specific table."""
    try:
        table = models.Table.objects.get(id=table_id, data_source_id=datasource_id)
    except models.Table.DoesNotExist:
        return JsonResponse({
            "status": "error",
            "error": f"Table {table_id} not found"
        }, status=404)
    
    columns = models.TableColumn.objects.filter(table=table).values(
        'id', 'name', 'public_name', 'data_type'
    )
    
    return JsonResponse({
        "status": "success",
        "table_id": table_id,
        "table_name": table.name,
        "columns": list(columns)
    })


@require_http_methods(["GET"])
def list_foreign_keys(request, datasource_id):
    """List foreign key relationships for a datasource."""
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
            "constrained_table": fk.constrained_table.name,
            "constrained_column": fk.constrained_columns.name if fk.constrained_columns else None,
            "referred_table": fk.referred_table.name,
            "referred_column": fk.referred_columns.name if fk.referred_columns else None,
        })
    
    return JsonResponse({
        "status": "success",
        "datasource_id": datasource_id,
        "foreign_keys": fk_data
    })


# =============================================================================
# Query Execution Endpoints
# =============================================================================

@csrf_exempt
@require_http_methods(["POST"])
def execute_query(request, datasource_id=None):
    """
    Execute a SQL query against a datasource.
    
    Expects JSON body:
        {
            "sql": "SELECT * FROM users LIMIT 10",
            "datasourceId": 1,  // Optional if datasource_id in URL
            "page": 1,
            "per_page": 50,
            "roles": [1, 2]  // Optional: group IDs for role-based SQL transformation
        }
        
    Returns:
        JSON with query results
    """
    try:
        body = json.loads(request.body)
        
        # Resolve datasource ID
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
        
        # Optional role-based SQL transformation
        role_ids = body.get("roles", [])
        native_sql = sql
        
        if role_ids:
            from django.contrib.auth.models import Group
            roles = Group.objects.filter(id__in=role_ids)
            mDb = prepare_mdb(ds, roles)
            transform_result = generate_native_sql(mDb, sql, ds.dialect_name)
            
            if transform_result.get('status') == 'error':
                return JsonResponse({
                    "status": "error",
                    "error": transform_result.get('error', 'SQL transformation failed')
                }, status=400)
            
            native_sql = transform_result.get('native_sql', sql)
        
        # Execute query
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
@require_http_methods(["POST"])
def export_query(request, datasource_id=None):
    """
    Export query results as CSV.
    
    Expects JSON body:
        {
            "sql": "SELECT * FROM users",
            "datasourceId": 1,  // Optional if datasource_id in URL
            "roles": [1, 2]  // Optional: group IDs for role-based SQL transformation
        }
        
    Returns:
        CSV file download
    """
    try:
        body = json.loads(request.body)
        
        # Resolve datasource ID
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
        
        # Optional role-based SQL transformation
        role_ids = body.get("roles", [])
        native_sql = sql
        
        if role_ids:
            from django.contrib.auth.models import Group
            roles = Group.objects.filter(id__in=role_ids)
            mDb = prepare_mdb(ds, roles)
            transform_result = generate_native_sql(mDb, sql, ds.dialect_name)
            
            if transform_result.get('status') == 'error':
                return JsonResponse({
                    "status": "error",
                    "error": transform_result.get('error', 'SQL transformation failed')
                }, status=400)
            
            native_sql = transform_result.get('native_sql', sql)
        
        # Export as CSV
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
