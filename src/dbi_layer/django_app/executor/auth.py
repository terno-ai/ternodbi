from django.http import JsonResponse, HttpResponseForbidden
from functools import wraps
import json
from django.apps import apps
from django.conf import settings

def get_kernel_model():
    """Get the Kernel model dynamically."""
    # Try chat_interface.Kernel, or fallback to configured model
    try:
        return apps.get_model('chat_interface', 'Kernel')
    except LookupError:
        # Fallback or configuration if needed
        model_path = getattr(settings, 'TERNO_KERNEL_MODEL', 'chat_interface.Kernel')
        return apps.get_model(model_path)


def require_session(view_func):
    @wraps(view_func)
    def _wrapped(request, *args, **kwargs):
        print("Request body", request.body)
        try:
            payload = json.loads(request.body.decode())
        except (ValueError, UnicodeDecodeError):
            return JsonResponse({"error": "invalid JSON"}, status=400)

        session_id = payload.get("session_id")
        if not session_id:
            return JsonResponse({"error": "session required"}, status=401)
        print("Session verified")
        
        Kernel = get_kernel_model()
        try:
            sess = Kernel.objects.get(
                id=session_id,
                expires_at__isnull=True
            )
        except Kernel.DoesNotExist:
            return JsonResponse({"error": "invalid or expired session"}, status=403)

        expected = (sess.metadata or {}).get("sandbox_token")
        given = request.headers.get("Authorization", "").removeprefix("Bearer ").strip()

        if expected and expected != given:
            return HttpResponseForbidden("bad sandbox token")

        request.session_obj = sess
        request.user = sess.user
        request.json = payload
        print("Auth completed")
        return view_func(request, *args, **kwargs)

    return _wrapped
