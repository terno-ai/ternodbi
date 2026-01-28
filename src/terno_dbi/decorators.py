from functools import wraps
from django.http import JsonResponse
from terno_dbi.core.models import Table, TableColumn
from terno_dbi.core import conf


def require_service_auth(allowed_types=None):
    if allowed_types is None:
        allowed_types = []

    def decorator(view_func):
        @wraps(view_func)
        def _wrapped_view(request, *args, **kwargs):
            if not hasattr(request, "service_token"):
                return JsonResponse({"error": "Authentication required"}, status=401)

            token = request.service_token

            if allowed_types:
                if token.token_type not in allowed_types:
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
                        return JsonResponse(
                            {"error": "Access denied to datasource"},
                            status=403
                        )

                    request.resolved_datasource = ds
                except Exception as e:
                    return JsonResponse({"error": f"Datasource not found: {ds_identifier}"}, status=404)

            table_id = kwargs.get('table_id')
            if table_id:
                try:
                    table = Table.objects.select_related('data_source').get(id=table_id)
                    if not allowed_ds.filter(id=table.data_source_id).exists():
                        return JsonResponse(
                            {"error": "Access denied to table"},
                            status=403
                        )
                    request.resolved_table = table
                except Table.DoesNotExist:
                    return JsonResponse({"error": f"Table not found: {table_id}"}, status=404)

            column_id = kwargs.get('column_id')
            if column_id:
                try:
                    column = TableColumn.objects.select_related('table__data_source').get(id=column_id)
                    if not allowed_ds.filter(id=column.table.data_source_id).exists():
                        return JsonResponse(
                            {"error": "Access denied to column"},
                            status=403
                        )
                    request.resolved_column = column
                except TableColumn.DoesNotExist:
                    return JsonResponse({"error": f"Column not found: {column_id}"}, status=404)

            return view_func(request, *args, **kwargs)
        return _wrapped_view
    return decorator
