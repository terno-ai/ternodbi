"""
Query Service Views.

Read-only endpoints for listing datasources, tables, columns, and executing queries.
Supports both token auth (API clients) and session auth (browser clients).

For session auth, SQL is transformed using user groups (role-based column/row filtering).
"""

import json
import logging
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
from django.views.decorators.csrf import csrf_exempt

from dbi_layer.django_app import models
from dbi_layer.django_app.auth import (
    require_token,
    require_auth,
    check_datasource_access,
    check_datasource_access_hybrid,
)
from dbi_layer.django_app import conf
from dbi_layer.services.query import execute_native_sql, export_native_sql_result
from dbi_layer.services.shield import prepare_mdb, generate_native_sql



logger = logging.getLogger(__name__)


# =============================================================================
# Health & Info
# =============================================================================

def health(request):
    """Health check endpoint (no auth required)."""
    return JsonResponse({
        "status": "ok",
        "service": "dbi_layer.query_service",
        "version": "1.0.0",
    })


@require_token()  # Any valid token
def info(request):
    """Service information (requires any valid token)."""
    from dbi_layer.connectors import ConnectorFactory
    
    return JsonResponse({
        "service": "dbi_layer.query_service",
        "version": "1.0.0",
        "token_type": request.service_token.token_type,
        "supported_databases": ConnectorFactory.get_supported_databases(),
    })


# =============================================================================
# DataSource Endpoints
# =============================================================================

@require_auth()
def get_datasources(request):
    """
    List all accessible datasources.
    
    Supports:
    - Token auth: Filter by token's datasource scope
    - Session auth: Filter by org_id (set by TernoAI middleware)
    """
    auth_type = getattr(request, 'auth_type', None)
    
    # Start with base queryset
    datasources = models.DataSource.objects.filter(enabled=True)
    
    # Filter based on auth type
    if auth_type == 'token':
        token = request.service_token
        if token.datasources.exists():
            datasources = token.datasources.filter(enabled=True)
    elif auth_type == 'session':
        # For session auth, use org_id if set by middleware
        org_id = getattr(request, 'org_id', None)
        if org_id:
            # Verify user belongs to this organization (same as TernoAI's check)
            if not conf.check_org_membership(request.user, org_id):
                return JsonResponse({
                    "status": "error",
                    "error": "You do not belong to this organisation."
                }, status=403)
            
            # Get datasource IDs from org (via conf helper)
            org_ds_ids = conf.get_org_datasources(org_id)
            if org_ds_ids:
                datasources = datasources.filter(id__in=org_ds_ids)
    
    # Build response
    data = []
    for ds in datasources:
        data.append({
            'id': ds.id,
            'name': ds.display_name,
            'type': ds.type,
        })
    
    return JsonResponse({
        "status": "success",
        "datasources": data
    })

@require_auth()
def get_datasource_name(request, ds_id):
    """
    Get datasource name and type (matches TernoAI's get_datasource_name).
    
    For session auth: Checks org membership and datasource belongs to org.
    For token auth: Checks token's datasource scope.
    """
    auth_type = getattr(request, 'auth_type', None)
    
    # Get datasource
    try:
        ds = models.DataSource.objects.get(id=ds_id, enabled=True)
    except models.DataSource.DoesNotExist:
        return JsonResponse({
            "status": "error",
            "error": f"DataSource {ds_id} not found"
        }, status=404)
    
    # Check access based on auth type
    if auth_type == 'token':
        allowed, error_response = check_datasource_access(request.service_token, ds)
        if not allowed:
            return error_response
    
    elif auth_type == 'session':
        # Verify org membership
        org_id = getattr(request, 'org_id', None)
        if org_id:
            if not conf.check_org_membership(request.user, org_id):
                return JsonResponse({
                    "status": "error",
                    "error": "You do not belong to this organisation."
                }, status=403)
            
            # Check datasource belongs to org
            org_ds_ids = conf.get_org_datasources(org_id)
            # Convert ds_id to int for comparison (URL may pass as str)
            ds_id_int = int(ds_id) if isinstance(ds_id, str) else ds_id
            if org_ds_ids and ds_id_int not in org_ds_ids:
                return JsonResponse({
                    "status": "error",
                    "error": "No Datasource found."
                }, status=404)
    
    else:
        return JsonResponse({
            "status": "error",
            "error": "Invalid authentication"
        }, status=401)
    
    # Return matching TernoAI's format exactly
    return JsonResponse({
        'datasource_name': ds.display_name,
        'type': ds.type
    })


# =============================================================================
# Table Endpoints
# =============================================================================

@require_auth()
def get_tables(request, datasource_id):
    """
    Get tables and columns for a datasource with role-based filtering.
    
    For session auth: Filters tables/columns based on user's Django groups.
    For token auth: Returns tables based on token's datasource scope.
    
    Response includes:
    - table_data: List of tables with columns
    - suggestions: Query suggestions for this datasource
    """
    auth_type = getattr(request, 'auth_type', None)
    
    # Get datasource
    try:
        ds = models.DataSource.objects.get(id=datasource_id, enabled=True)
    except models.DataSource.DoesNotExist:
        return JsonResponse({
            "status": "error",
            "error": f"DataSource {datasource_id} not found"
        }, status=404)
    
    # Check access based on auth type
    if auth_type == 'token':
        allowed, error_response = check_datasource_access(request.service_token, ds)
        if not allowed:
            return error_response
        # For token auth, get all tables (no role filtering)
        tables = models.Table.objects.filter(data_source=ds)
        columns = models.TableColumn.objects.filter(table__in=tables)
    
    elif auth_type == 'session':
        # Verify org membership
        org_id = getattr(request, 'org_id', None)
        if org_id:
            if not conf.check_org_membership(request.user, org_id):
                return JsonResponse({
                    "status": "error",
                    "error": "You do not belong to this organisation."
                }, status=403)
            
            # Check datasource belongs to org
            org_ds_ids = conf.get_org_datasources(org_id)
            if org_ds_ids and datasource_id not in org_ds_ids:
                return JsonResponse({
                    "status": "error",
                    "error": "No Datasource found."
                }, status=404)
        
        # Get role-based filtered tables and columns
        from dbi_layer.services.access import get_admin_config_object
        roles = request.user.groups.all()
        tables, columns = get_admin_config_object(ds, roles)
    
    else:
        return JsonResponse({
            "status": "error",
            "error": "Invalid authentication"
        }, status=401)
    
    # Build table_data with columns (matching TernoAI format)
    table_data = []
    for table in tables:
        table_columns = columns.filter(table_id=table)
        column_data = list(table_columns.values('public_name', 'data_type'))
        table_data.append({
            'table_name': table.public_name,
            'table_description': table.description,
            'column_data': column_data
        })
    
    # Get suggestions for this datasource
    suggestions = list(
        models.DatasourceSuggestions.objects.filter(data_source=ds)
        .values_list('suggestion', flat=True)
    )
    
    return JsonResponse({
        'status': 'success',
        'table_data': table_data,
        'suggestions': suggestions
    })



@require_token()
@require_http_methods(["GET"])
def list_columns(request, datasource_id, table_id):
    """List columns for a table."""
    try:
        table = models.Table.objects.get(id=table_id, data_source_id=datasource_id)
    except models.Table.DoesNotExist:
        return JsonResponse({
            "status": "error",
            "error": f"Table {table_id} not found"
        }, status=404)
    
    # Check access
    allowed, error_response = check_datasource_access(request.service_token, table.data_source)
    if not allowed:
        return error_response
    
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


@require_token()
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
    
    # Check access
    allowed, error_response = check_datasource_access(request.service_token, ds)
    if not allowed:
        return error_response
    
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
        "count": len(fk_data),
        "foreign_keys": fk_data
    })


# =============================================================================
# Query Execution
# =============================================================================

@csrf_exempt

@csrf_exempt
@require_auth()
@require_http_methods(["POST"])
def execute_sql(request, datasource_id=None):
    """
    Execute a SQL query against a datasource (renamed from execute_query).
    
    Supports:
    - URL param: /datasources/<id>/execute_sql/
    - JSON body: {"datasourceId": <id>} (Legacy TernoAI)
    - Hybrid Auth (Session + Token)
    - Audit Logging via 'query_executed' signal
    """
    from dbi_layer.django_app.services.query import prepare_mdb, generate_native_sql, execute_native_sql
    from dbi_layer.django_app import conf
    from dbi_layer.django_app.signals import query_executed
    
    try:
        body = json.loads(request.body)
        
        # 1. Resolve Datasource ID (URL priority, then Body)
        ds_id = datasource_id or body.get("datasourceId")
        if not ds_id:
            return JsonResponse({'status': 'error', 'error': 'Datasource ID required'}, status=400)
            
        try:
            ds = models.DataSource.objects.get(id=ds_id, enabled=True)
        except models.DataSource.DoesNotExist:
            return JsonResponse({'status': 'error', 'error': f'DataSource {ds_id} not found'}, status=404)
        
        # 2. Check Access (Hybrid)
        allowed, error_response = check_datasource_access_hybrid(request, ds)
        if not allowed:
            return error_response

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
        
        # 3. Transform SQL (Role-Based)
        # Session auth -> user.groups
        # Token auth -> token.created_by.groups
        native_sql = sql
        roles = None
        user = getattr(request, 'user', None)
        if user and not user.is_authenticated:
            user = None

        if user:
            # Session auth
            roles = user.groups.all()
        elif hasattr(request, 'service_token') and request.service_token.created_by:
            # Token auth
            roles = request.service_token.created_by.groups.all()
            user = request.service_token.created_by # For logging
        
        if roles is not None:
            mDb = prepare_mdb(ds, roles)
            transform_result = generate_native_sql(mDb, sql, ds.dialect_name)
            
            if transform_result.get('status') == 'error':
                error_msg = transform_result.get('error', 'SQL transformation failed')
                # Log failure
                query_executed.send(
                    sender=None, datasource=ds, user=user, user_sql=sql, 
                    native_sql=None, status='error', error=error_msg
                )
                return JsonResponse({"status": "error", "error": error_msg}, status=400)
                
            native_sql = transform_result.get('native_sql', sql)
        
        # 4. Execute Native SQL
        result = execute_native_sql(ds, native_sql, page=page, per_page=per_page)
        
        # 5. Audit Log (Signal)
        paged_result_status = result.get('status')
        query_executed.send(
            sender=None,
            datasource=ds,
            user=user,
            user_sql=sql,
            native_sql=native_sql,
            status=paged_result_status,
            error=result.get('error')
        )
        
        return JsonResponse(result)
        
    except json.JSONDecodeError:
        return JsonResponse({
            "status": "error",
            "error": "Invalid JSON in request body"
        }, status=400)
    except Exception as e:
        logger.exception(f"Query execution error: {e}")
        # Log unexpected exception
        query_executed.send(
             sender=None, datasource=None, user=getattr(request, 'user', None), 
             user_sql=None, native_sql=None, status='error', error=str(e)
        )
        return JsonResponse({
            "status": "error",
            "error": str(e)
        }, status=500)



# =============================================================================
# Export Endpoint
# =============================================================================


@csrf_exempt
@require_auth()
@require_http_methods(["POST"])
def export_sql_result(request, datasource_id=None):
    """
    Export query results as CSV file (renamed from export_query).
    
    Supports:
    - URL param: /datasources/<id>/export/
    - JSON body: {"datasourceId": <id>} (Legacy TernoAI)
    - Hybrid Auth (Session + Token)
    - Audit Logging via 'query_executed' signal
    """
    from dbi_layer.django_app.services.query import prepare_mdb, generate_native_sql, export_native_sql_result
    from dbi_layer.django_app import conf
    from dbi_layer.django_app.signals import query_executed
    
    try:
        body = json.loads(request.body)
        
        # 1. Resolve Datasource ID
        ds_id = datasource_id or body.get("datasourceId")
        if not ds_id:
            return JsonResponse({'status': 'error', 'error': 'Datasource ID required'}, status=400)
            
        try:
            ds = models.DataSource.objects.get(id=ds_id, enabled=True)
        except models.DataSource.DoesNotExist:
            return JsonResponse({'status': 'error', 'error': f'DataSource {ds_id} not found'}, status=404)
    
        # 2. Check Access (Hybrid)
        allowed, error_response = check_datasource_access_hybrid(request, ds)
        if not allowed:
            return error_response

        sql = body.get("sql")
        
        if not sql:
            return JsonResponse({
                "status": "error",
                "error": "Missing 'sql' in request body"
            }, status=400)
        
        # 3. Transform SQL (Role-Based)
        native_sql = sql
        roles = None
        user = getattr(request, 'user', None)
        if user and not user.is_authenticated:
            user = None

        if user:
            # Session auth
            roles = user.groups.all()
        elif hasattr(request, 'service_token') and request.service_token.created_by:
            # Token auth
            roles = request.service_token.created_by.groups.all()
            user = request.service_token.created_by # For logging
        
        if roles is not None:
            mDb = prepare_mdb(ds, roles)
            transform_result = generate_native_sql(mDb, sql, ds.dialect_name)
            
            if transform_result.get('status') == 'error':
                error_msg = transform_result.get('error', 'SQL transformation failed')
                # Log failure
                query_executed.send(
                    sender=None, datasource=ds, user=user, user_sql=sql, 
                    native_sql=None, status='error', error=error_msg
                )
                return JsonResponse({"status": "error", "error": error_msg}, status=400)
                
            native_sql = transform_result.get('native_sql', sql)
        
        # 4. Audit Log (Signal)
        # Note: We assume success if we reach here, as export handles the download.
        # Capturing stream errors is hard, but we log the attempt.
        query_executed.send(
            sender=None,
            datasource=ds,
            user=user,
            user_sql=sql,
            native_sql=native_sql,
            status='success', 
            error=None
        )
        
        # 5. Export CSV
        return export_native_sql_result(ds, native_sql)
        
    except json.JSONDecodeError:
        return JsonResponse({
            "status": "error",
            "error": "Invalid JSON in request body"
        }, status=400)
    except Exception as e:
        logger.exception(f"Export error: {e}")
        # Log unexpected exception
        query_executed.send(
             sender=None, datasource=None, user=getattr(request, 'user', None), 
             user_sql=None, native_sql=None, status='error', error=str(e)
        )
        return JsonResponse({
            "status": "error",
            "error": str(e)
        }, status=500)


