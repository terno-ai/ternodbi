
from functools import wraps
from django.http import JsonResponse
from dbi_layer.django_app.models import ServiceToken


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
                if ServiceToken.TokenType.ADMIN in allowed_types:
                    if token.token_type != ServiceToken.TokenType.ADMIN:
                        return JsonResponse(
                            {"error": f"Insufficient permissions. Required scope: ADMIN"}, 
                            status=403
                        )

                if token.token_type not in allowed_types:
                    return JsonResponse(
                        {"error": f"Insufficient permissions. Token scope '{token.token_type}' not allowed."}, 
                        status=403
                    )

            datasource_id = kwargs.get('datasource_id') or kwargs.get('pk')

            if datasource_id:
                try:
                    ds_id = int(datasource_id)
                    if token.datasources.exists():
                        if not token.datasources.filter(id=ds_id).exists():
                            return JsonResponse(
                                {"error": f"Token does not have access to Datasource ID {ds_id}"}, 
                                status=403
                            )
                except (ValueError, TypeError):
                    pass

            return view_func(request, *args, **kwargs)
        return _wrapped_view
    return decorator
