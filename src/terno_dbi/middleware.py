
from django.http import JsonResponse
from django.utils.functional import SimpleLazyObject
from terno_dbi.services.auth import verify_token, update_token_usage
import logging

logger = logging.getLogger(__name__)


class ServiceTokenMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        if not request.path.startswith("/api/"):
            return self.get_response(request)

        if "/health/" in request.path or "/info/" in request.path:
            return self.get_response(request)

        auth_header = request.headers.get("Authorization", "")
        if not auth_header.startswith("Bearer "):
            return JsonResponse(
                {"error": "Missing or invalid Authorization header. Expected 'Bearer <token>'."}, 
                status=401
            )

        token_str = auth_header.split(" ")[1]

        token = verify_token(token_str)
        if not token:
            logger.warning(f"Invalid or inactive token used: {token_str[:15]}...")
            return JsonResponse({"error": "Invalid or expired Service Token"}, status=401)

        request.service_token = token

        update_token_usage(token)

        return self.get_response(request)
