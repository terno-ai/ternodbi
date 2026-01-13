
from django.http import JsonResponse
from django.utils.functional import SimpleLazyObject
from dbi_layer.services.auth import verify_token, update_token_usage
import logging

logger = logging.getLogger(__name__)

class ServiceTokenMiddleware:
    """
    Middleware to enforce Service Token authentication for /api/ routes.
    """
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        # 1. Skip non-API routes (e.g. admin panel)
        if not request.path.startswith("/api/"):
            return self.get_response(request)
            
        # 1.1 Skip Health/Info check endpoints
        if "/health/" in request.path or "/info/" in request.path:
             return self.get_response(request)

            
        # 2. Extract Token
        auth_header = request.headers.get("Authorization", "")
        if not auth_header.startswith("Bearer "):
            return JsonResponse(
                {"error": "Missing or invalid Authorization header. Expected 'Bearer <token>'."}, 
                status=401
            )
            
        token_str = auth_header.split(" ")[1]
        
        # 3. Verify Token
        token = verify_token(token_str)
        if not token:
            logger.warning(f"Invalid or active token used: {token_str[:15]}...")
            return JsonResponse({"error": "Invalid or expired Service Token"}, status=401)
            
        # 4. Attach to Request
        request.service_token = token
        
        # 5. Async Audit (Update Last Used)
        # We assume trivial overhead for now; for high-scale, offload to background task
        update_token_usage(token)
        
        return self.get_response(request)
