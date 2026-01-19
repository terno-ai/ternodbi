"""
Authentication module for Terno DBI.

Provides token-based authentication decorators for views.
"""

import logging
from functools import wraps
from django.http import JsonResponse
from django.utils import timezone

from terno_dbi.core.models import ServiceToken

logger = logging.getLogger(__name__)


def get_token_from_request(request):
    """
    Extract token from Authorization header.
    
    Expects: Authorization: Bearer dbi_sk_xxx...
    """
    auth_header = request.headers.get('Authorization', '')
    
    if not auth_header.startswith('Bearer '):
        return None
    
    return auth_header[7:].strip()  # Remove 'Bearer ' prefix


def validate_token(token_key, required_type=None):
    """
    Validate a token and return the ServiceToken object.
    
    Args:
        token_key: The raw token key
        required_type: 'admin' or 'query' (None = any type)
        
    Returns:
        (ServiceToken, error_message) tuple
    """
    if not token_key:
        return None, "Authorization token required"
    
    if not token_key.startswith('dbi_sk_'):
        return None, "Invalid token format"
    
    # Hash the key and look it up
    key_hash = ServiceToken.hash_key(token_key)
    
    try:
        token = ServiceToken.objects.get(key_hash=key_hash)
    except ServiceToken.DoesNotExist:
        return None, "Invalid token"
    
    # Check if active
    if not token.is_active:
        return None, "Token has been revoked"
    
    # Check expiry
    if token.expires_at and token.expires_at < timezone.now():
        return None, "Token has expired"
    
    # Check type if required
    if required_type and token.token_type != required_type:
        return None, f"Token type '{token.token_type}' not allowed. Required: '{required_type}'"
    
    # Update last_used
    token.last_used = timezone.now()
    token.save(update_fields=['last_used'])
    
    return token, None


def require_token(token_type=None):
    """
    Decorator to require token authentication.
    
    Usage:
        @require_token()  # Any valid token
        @require_token('admin')  # Admin tokens only
        @require_token('query')  # Query tokens only
        
    Adds request.service_token with the validated token.
    """
    def decorator(view_func):
        @wraps(view_func)
        def _wrapped(request, *args, **kwargs):
            # Extract token from header
            token_key = get_token_from_request(request)
            
            # Validate token
            token, error = validate_token(token_key, token_type)
            
            if error:
                return JsonResponse({
                    'status': 'error',
                    'error': error
                }, status=401)
            
            # Attach token to request
            request.service_token = token
            
            return view_func(request, *args, **kwargs)
        
        return _wrapped
    return decorator


def require_admin_token(view_func):
    """Shortcut decorator for admin-only endpoints."""
    return require_token('admin')(view_func)


def require_query_token(view_func):
    """Shortcut decorator for query-only endpoints."""
    return require_token('query')(view_func)


def check_datasource_access(token, datasource):
    """
    Check if a token has access to a specific datasource.
    
    Returns:
        (allowed, error_response) tuple
    """
    if not token.has_access_to(datasource):
        return False, JsonResponse({
            'status': 'error',
            'error': f"Token does not have access to datasource {datasource.id}"
        }, status=403)
    
    return True, None


# =============================================================================
# Hybrid Auth (Token + Session)
# =============================================================================

def require_auth(admin_only=False):
    """
    Decorator that accepts both token auth AND session auth.
    
    This allows TernoDBI endpoints to be used by:
    1. API clients (via token)
    2. Browser clients (via session, e.g., TernoAI frontend)
    
    Usage:
        @require_auth()  # Any valid auth
        @require_auth(admin_only=True)  # Admin token or staff user
        
    Adds to request:
        - request.service_token (if token auth)
        - request.auth_type ('token' or 'session')
    """
    def decorator(view_func):
        @wraps(view_func)
        def _wrapped(request, *args, **kwargs):
            # Try token auth first
            token_key = get_token_from_request(request)
            if token_key:
                required_type = 'admin' if admin_only else None
                token, error = validate_token(token_key, required_type)
                
                if error:
                    return JsonResponse({
                        'status': 'error',
                        'error': error
                    }, status=401)
                
                request.service_token = token
                request.auth_type = 'token'
                return view_func(request, *args, **kwargs)
            
            # Try session auth (logged-in user)
            if hasattr(request, 'user') and request.user.is_authenticated:
                # For admin-only endpoints, check if user is staff
                if admin_only and not request.user.is_staff:
                    return JsonResponse({
                        'status': 'error',
                        'error': 'Staff privileges required'
                    }, status=403)
                
                request.service_token = None  # No token, using session
                request.auth_type = 'session'
                return view_func(request, *args, **kwargs)
            
            # No valid auth
            return JsonResponse({
                'status': 'error',
                'error': 'Authentication required. Provide Authorization header or log in.'
            }, status=401)
        
        return _wrapped
    return decorator


def check_datasource_access_hybrid(request, datasource):
    """
    Check datasource access for both token and session auth.
    
    For token auth: Uses token's datasource scope
    For session auth: Uses org_id from request (set by TernoAI middleware)
    
    Returns:
        (allowed, error_response) tuple
    """
    auth_type = getattr(request, 'auth_type', None)
    
    if auth_type == 'token':
        token = request.service_token
        return check_datasource_access(token, datasource)
    
    elif auth_type == 'session':
        # For session auth, check org membership if org_id is set
        org_id = getattr(request, 'org_id', None)
        if org_id:
            # Check if datasource belongs to org
            # This requires OrganisationDataSource model from TernoAI
            # TernoDBI doesn't own this, so we check via a configurable callback
            from django.conf import settings
            checker = getattr(settings, 'DBI_ORG_DATASOURCE_CHECKER', None)
            if checker and callable(checker):
                if not checker(org_id, datasource.id):
                    return False, JsonResponse({
                        'status': 'error',
                        'error': f'Datasource {datasource.id} not accessible for this organization'
                    }, status=403)
        return True, None
    
    return False, JsonResponse({
        'status': 'error',
        'error': 'No valid authentication'
    }, status=401)

