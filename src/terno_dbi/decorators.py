import logging
from functools import wraps
from django.http import JsonResponse
from terno_dbi.core.models import Table, TableColumn
from terno_dbi.core import conf

logger = logging.getLogger(__name__)


def require_service_auth(allowed_types=None):
    if allowed_types is None:
        allowed_types = []

    def decorator(view_func):
        @wraps(view_func)
        def _wrapped_view(request, *args, **kwargs):
            if not hasattr(request, "service_token"):
                logger.warning("Authentication required: request missing service_token")
                return JsonResponse({"error": "Authentication required"}, status=401)

            token = request.service_token

            if allowed_types:
                if token.token_type not in allowed_types:
                    logger.warning(
                        "Permission denied: token '%s' (type=%s) not in allowed_types=%s",
                        token.name, token.token_type, allowed_types
                    )
                    return JsonResponse(
                        {"error": f"Insufficient permissions. Token type '{token.token_type}' not allowed."},
                        status=403
                    )

            allowed_ds = token.get_accessible_datasources()
            request.allowed_datasources = allowed_ds
            request.token_organisation = token.organisation

            if conf.get('REQUIRE_TOKEN_SCOPE'):
                if not token.datasources.exists() and not token.organisation:
                    if not conf.get('ALLOW_SUPERTOKEN'):
                        logger.warning(
                            "Access denied: token '%s' has no datasource or organisation scope",
                            token.name
                        )
                        return JsonResponse(
                            {"error": "Token has no datasource or organisation scope. Access denied."},
                            status=403
                        )

            ds_identifier = (
                kwargs.get('datasource_identifier') or
                kwargs.get('datasource_id') or
                kwargs.get('pk')
            )

            if ds_identifier:
                try:
                    from terno_dbi.services.resolver import resolve_datasource
                    ds = resolve_datasource(ds_identifier)
                    if not allowed_ds.filter(id=ds.id).exists():
                        logger.warning(
                            "Datasource access denied: token '%s' attempted access to datasource '%s'",
                            token.name, ds_identifier
                        )
                        return JsonResponse(
                            {"error": "Access denied to datasource"},
                            status=403
                        )

                    request.resolved_datasource = ds
                    logger.debug("Datasource resolved: %s -> id=%d", ds_identifier, ds.id)
                except Exception as e:
                    logger.warning("Datasource not found: %s", ds_identifier)
                    return JsonResponse({"error": f"Datasource not found: {ds_identifier}"}, status=404)

            table_id = kwargs.get('table_id')
            if table_id:
                try:
                    table = Table.objects.select_related('data_source').get(id=table_id)
                    if not token.has_access_to_table(table):
                        logger.warning(
                            "Table access denied: token '%s' attempted access to private/unauthorized table_id=%s",
                            token.name, table_id
                        )
                        return JsonResponse(
                            {"error": "Access denied to table"},
                            status=403
                        )
                    request.resolved_table = table
                except Table.DoesNotExist:
                    logger.warning("Table not found: table_id=%s", table_id)
                    return JsonResponse({"error": f"Table not found: {table_id}"}, status=404)

            column_id = kwargs.get('column_id')
            if column_id:
                try:
                    column = TableColumn.objects.select_related('table__data_source').get(id=column_id)
                    if not token.has_access_to_column(column):
                        logger.warning(
                            "Column access denied: token '%s' attempted access to private/unauthorized column_id=%s",
                            token.name, column_id
                        )
                        return JsonResponse(
                            {"error": "Access denied to column"},
                            status=403
                        )
                    request.resolved_column = column
                except TableColumn.DoesNotExist:
                    logger.warning("Column not found: column_id=%s", column_id)
                    return JsonResponse({"error": f"Column not found: {column_id}"}, status=404)

            return view_func(request, *args, **kwargs)
        return _wrapped_view
    return decorator


def require_scope(*required_scopes):
    """
    Decorator to enforce that the service token has specific scope(s).
    
    Usage:
        @require_service_auth()
        @require_scope('query:execute')
        def execute_query(request):
            ...
        
        # Multiple scopes (requires ALL):
        @require_service_auth()
        @require_scope('admin:read', 'admin:write')
        def admin_action(request):
            ...
    
    Note: This decorator must be used AFTER @require_service_auth().
    """
    def decorator(view_func):
        @wraps(view_func)
        def _wrapped_view(request, *args, **kwargs):
            if not hasattr(request, "service_token"):
                logger.warning("require_scope: Missing service_token on request")
                return JsonResponse({"error": "Authentication required"}, status=401)

            token = request.service_token
            
            # Check all required scopes
            for scope in required_scopes:
                if not token.has_scope(scope):
                    logger.warning(
                        "Scope denied: token '%s' lacks scope '%s' (has: %s)",
                        token.name, scope, token.scopes
                    )
                    return JsonResponse(
                        {"error": f"Insufficient scope. Required: '{scope}'"},
                        status=403
                    )
            
            return view_func(request, *args, **kwargs)
        return _wrapped_view
    return decorator

