"""Transport layer for `TernoDBIClient`.

Supports two transports with the same API:

- `HttpTransport` — the default for stdio and out-of-process clients. Uses
  `requests` to call the TernoDBI API over HTTP.
- `InProcessTransport` — used when the MCP server runs in the same Django
  process as the API. Avoids HTTP requests to `127.0.0.1`, which would consume
  another worker and can deadlock under concurrent tool calls.

The in-process transport uses Django's URL resolver instead of calling service
functions directly. This keeps authentication, permissions, argument handling,
and response formatting on the same code path as HTTP requests.
"""

import json as _json
import logging
from typing import Any, Dict, Optional
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


class HttpTransport:
    """The default: real HTTP via `requests`."""

    def __init__(self, timeout: Optional[float] = None):
        self.timeout = timeout

    def _call(self, method: str, url: str, **kwargs):
        import requests

        if self.timeout is not None:
            kwargs.setdefault("timeout", self.timeout)
        return getattr(requests, method)(url, **kwargs)

    def get(self, url, **kwargs):
        return self._call("get", url, **kwargs)

    def post(self, url, **kwargs):
        return self._call("post", url, **kwargs)

    def patch(self, url, **kwargs):
        return self._call("patch", url, **kwargs)

    def delete(self, url, **kwargs):
        return self._call("delete", url, **kwargs)


class _InProcessResponse:
    """Presents a Django `HttpResponse` with the parts of the `requests` API
    that `TernoDBIClient` uses — `status_code`, `.json()`, `.raise_for_status()`,
    and enough of `.request`/`.url` for its error messages."""

    def __init__(self, django_response, method: str, url: str):
        self._response = django_response
        self.status_code = django_response.status_code
        self.url = url
        self.request = type("_Req", (), {"method": method.upper(), "url": url})()

    @property
    def content(self) -> bytes:
        return self._response.content

    @property
    def text(self) -> str:
        return self._response.content.decode("utf-8", errors="replace")

    def json(self) -> Any:
        return _json.loads(self._response.content or b"null")

    def raise_for_status(self) -> None:
        if 400 <= self.status_code < 600:
            import requests

            raise requests.exceptions.HTTPError(
                f"{self.status_code} for {self.url}", response=self
            )


class InProcessTransport:
    """Dispatches to Django views directly, with no socket.

    `api_key` is resolved to a `ServiceToken` once per call and attached to the
    synthesized request as `request.service_token` — the same attribute
    `ServiceTokenMiddleware` sets. Middleware does not run on this path (there is
    no middleware chain), so anything the views need from it must be set here.
    """

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key

    # `RequestFactory` lives under `django.test`, but it is a pure request
    # builder with no test-only behaviour — no database teardown, no client
    # state. Constructing a `WSGIRequest` by hand is the alternative and is
    # strictly more code to get wrong.
    def _factory(self):
        from django.test import RequestFactory

        return RequestFactory()

    def _token(self, headers: Optional[Dict[str, str]]):
        from terno_dbi.services.auth import verify_token

        key = self.api_key
        if not key and headers:
            raw = headers.get("Authorization", "")
            if raw.startswith("Bearer "):
                key = raw[7:].strip()
        if not key:
            return None
        return verify_token(key)

    def _call(self, method: str, url: str, *, headers=None, params=None, json=None):
        from django.urls import resolve

        path = urlparse(url).path
        factory = self._factory()

        if json is not None:
            request = getattr(factory, method)(
                path, data=_json.dumps(json), content_type="application/json"
            )
        else:
            request = getattr(factory, method)(path, data=params or {})

        token = self._token(headers)
        if token is None:
            # Mirror what ServiceTokenMiddleware would return rather than
            # letting the view fail on a missing attribute — the caller sees the
            # same 401 body it would get over HTTP.
            from django.http import JsonResponse

            return _InProcessResponse(
                JsonResponse({"error": "Invalid or expired Service Token"}, status=401),
                method, url,
            )
        request.service_token = token

        try:
            match = resolve(path)
        except Exception:
            from django.http import JsonResponse

            logger.warning("In-process dispatch found no route for %s", path)
            return _InProcessResponse(
                JsonResponse({"error": f"No route for {path}"}, status=404), method, url
            )

        response = match.func(request, *match.args, **match.kwargs)
        return _InProcessResponse(response, method, url)

    def get(self, url, **kwargs):
        return self._call("get", url, **kwargs)

    def post(self, url, **kwargs):
        return self._call("post", url, **kwargs)

    def patch(self, url, **kwargs):
        return self._call("patch", url, **kwargs)

    def delete(self, url, **kwargs):
        return self._call("delete", url, **kwargs)


_default_transport_factory = None


def set_default_transport_factory(factory) -> None:
    """Make new clients use `factory()` as their transport.

    Called once by the HTTP entrypoint when MCP runs inside Django. A factory
    rather than an instance because the transport carries the per-request API
    key, and a shared instance would be exactly the cross-tenant bleed
    `mcp/context.py` exists to prevent.
    """
    global _default_transport_factory
    _default_transport_factory = factory
    logger.info("Default TernoDBI transport set to %s", getattr(factory, "__name__", factory))


def build_transport(api_key: Optional[str] = None):
    if _default_transport_factory is not None:
        return _default_transport_factory(api_key)
    return HttpTransport()
