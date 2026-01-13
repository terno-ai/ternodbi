
from functools import wraps
from django.http import JsonResponse
from dbi_layer.django_app.models import ServiceToken

def require_service_auth(allowed_types=None):
    """
    Decorator to enforce token type (scope).
    
    Args:
        allowed_types: List of allowed TokenType values (e.g. ['admin', 'query']).
                       If None, only validates that SOME token exists (middleware handles existence).
    """
    if allowed_types is None:
        allowed_types = []
        
    def decorator(view_func):
        @wraps(view_func)
        def _wrapped_view(request, *args, **kwargs):
            # Token is attached by Middleware. If missing, Middleware already 401'd (unless exemptions exist).
            if not hasattr(request, "service_token"):
                 # Fallback if middleware didn't run or path wasn't /api/
                 return JsonResponse({"error": "Authentication required"}, status=401)
            
            token = request.service_token
            
            # 1. Check Scope (Token Type)
            # ADMIN tokens effectively have all permissions, but for explicit checks:
            # If endpoint requires ADMIN, query token fails.
            if allowed_types:
                # If token is ADMIN, it usually passes most checks, but let's be strict if desired.
                # Logic: If token.type is ADMIN, it passes any check? Or strictly 'admin'?
                # Standard: ADMIN > QUERY.
                
                # If required is ADMIN, token must be ADMIN.
                if ServiceToken.TokenType.ADMIN in allowed_types:
                    if token.token_type != ServiceToken.TokenType.ADMIN:
                        return JsonResponse(
                            {"error": f"Insufficient permissions. Required scope: ADMIN"}, 
                            status=403
                        )
                
                # If required is QUERY, both ADMIN and QUERY work.
                # (Logic handled by calling decorator with allowed_types=['admin', 'query'] usually)
                if token.token_type not in allowed_types:
                     return JsonResponse(
                        {"error": f"Insufficient permissions. Token scope '{token.token_type}' not allowed."}, 
                        status=403
                    )
            
            # 2. Check Granular Datasource Access
            # If URL has 'datasource_id' or 'datasource_pk', verify token has access mechanism.
            # This requires parsing args/kwargs.
            datasource_id = kwargs.get('datasource_id') or kwargs.get('pk')
            
            # Only check if validation makes sense (integer ID)
            if datasource_id:
                try:
                    ds_id = int(datasource_id)
                    # Use the model method we defined
                    # But we need to fetch the datasource object to check? 
                    # The model method `has_access_to(datasource_obj)` requires obj.
                    # Or we can do: use exists() query.
                    
                    # Optimization: Check if token has ANY datasource restrictions.
                    if token.datasources.exists():
                        if not token.datasources.filter(id=ds_id).exists():
                             return JsonResponse(
                                {"error": f"Token does not have access to Datasource ID {ds_id}"}, 
                                status=403
                            )
                except (ValueError, TypeError):
                    pass # Not an integer ID, skip granular check
            
            return view_func(request, *args, **kwargs)
        return _wrapped_view
    return decorator
