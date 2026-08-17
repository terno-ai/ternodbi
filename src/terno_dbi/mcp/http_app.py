"""Streamable HTTP transport and request authentication.

TernoDBI is a standalone ASGI application and can be mounted inside another
application's ASGI stack. When mounted under a ProtocolTypeRouter["http"],
the /mcp endpoint is handled directly by TernoDBI and does not pass through
the host application's Django middleware.

Authentication is therefore handled at the transport layer using the same
token verification logic used by the middleware.

The session manager is started lazily because a mounted ASGI application may
not have its own Starlette lifespan. The manager is started once on the first
request and kept running for subsequent requests.

Credentials are required for each request. If they cannot be resolved, the
request fails rather than falling back to server-level credentials.
"""

import asyncio
import json
import logging
from typing import Any, Awaitable, Callable, Dict, Optional
from mcp.server.streamable_http_manager import StreamableHTTPSessionManager
from terno_dbi.mcp.context import request_credentials, require_request_credentials

logger = logging.getLogger(__name__)

MCP_PATH = "/mcp"

# RFC 9728: the 401 response points clients to the resource metadata, where
# they can discover the authorization server and start the authentication flow.
PROTECTED_RESOURCE_METADATA_PATH = "/.well-known/oauth-protected-resource"


class TokenRejected(Exception):
    """The bearer token was absent, malformed, or not valid."""


async def _send_json(send, status: int, payload: Dict[str, Any], headers=None) -> None:
    body = json.dumps(payload).encode()
    raw = [(b"content-type", b"application/json"), (b"content-length", str(len(body)).encode())]
    for key, value in (headers or {}).items():
        raw.append((key.encode(), value.encode()))
    await send({"type": "http.response.start", "status": status, "headers": raw})
    await send({"type": "http.response.body", "body": body})


def _bearer(scope) -> Optional[str]:
    for key, value in scope.get("headers", []):
        if key.lower() == b"authorization":
            raw = value.decode("latin-1")
            if raw.lower().startswith("bearer "):
                return raw[7:].strip()
            return None
    return None


def default_token_resolver(resource_url: str = "") -> Callable[[str], Awaitable[Dict[str, Any]]]:
    """Resolve a bearer token against `ServiceToken`, off the event loop.

    `verify_token` uses the Django ORM synchronously; calling it directly from
    async code raises `SynchronousOnlyOperation`. `sync_to_async` with
    `thread_sensitive=True` keeps it on the same thread Django expects.
    """
    from asgiref.sync import sync_to_async

    def _verify(token_str: str) -> Dict[str, Any]:
        from terno_dbi.oauth.minting import token_grant_summary
        from terno_dbi.services.auth import update_token_usage, verify_token

        token = verify_token(token_str)
        if token is None:
            raise TokenRejected("Invalid or expired token")
        update_token_usage(token)
        # Write access requires both checks: the scope confirms what the client was
        # granted, while the group confirms what the user is allowed to do in this org.
        # Either check alone is insufficient.
        summary = token_grant_summary(token)
        return {
            "api_key": token_str,
            "can_write": summary["can_write"],
            "scopes": summary["scopes"],
            "org_subdomain": getattr(token.organisation, "subdomain", None),
        }

    return sync_to_async(_verify, thread_sensitive=True)


def build_asgi_app(
    server,
    *,
    token_resolver: Optional[Callable[[str], Awaitable[Dict[str, Any]]]] = None,
    base_url: Optional[str] = None,
    json_response: bool = False,
    stateless: bool = False,
    strict_credentials: bool = True,
    in_process: bool = False,
):
    """Return a raw ASGI app that serves server over Streamable HTTP.

        token_resolver can be injected for testing and defaults to the real
        ServiceToken lookup.

        Set in_process=True when the app is mounted in the same process as the
        TernoDBI API. This keeps tool calls in-process instead of making HTTP requests
        back to 127.0.0.1, which can consume an extra worker per call and deadlock
        under concurrent load.
    """
    if strict_credentials:
        require_request_credentials(True)

    if in_process:
        from terno_dbi.transport import InProcessTransport, set_default_transport_factory

        set_default_transport_factory(InProcessTransport)

    manager = StreamableHTTPSessionManager(
        app=server, json_response=json_response, stateless=stateless
    )
    resolve = token_resolver or default_token_resolver()

    _started = asyncio.Event()
    _lock = asyncio.Lock()
    _runner: Dict[str, Any] = {}

    async def _ensure_started() -> None:
        if _started.is_set():
            return
        async with _lock:
            if _started.is_set():
                return

            async def _run() -> None:
                try:
                    async with manager.run():
                        _started.set()
                        await asyncio.Event().wait()  # hold the context open
                except Exception:
                    logger.exception("MCP session manager stopped")
                    _started.set()  # unblock waiters; requests will then fail loudly

            _runner["task"] = asyncio.create_task(_run())
        await _started.wait()

    async def app(scope, receive, send) -> None:
        if scope["type"] != "http":
            raise RuntimeError(f"MCP app received unsupported scope {scope['type']!r}")

        token = _bearer(scope)
        if not token:
            await _send_json(
                send,
                401,
                {"error": "Missing bearer token"},
                # Points at the metadata document so the client can begin OAuth
                # itself instead of showing the user a bare 401.
                {
                    "WWW-Authenticate": (
                        'Bearer realm="terno", '
                        f'resource_metadata="{base_url or ""}{PROTECTED_RESOURCE_METADATA_PATH}"'
                    )
                },
            )
            return

        try:
            resolved = await resolve(token)
        except TokenRejected as exc:
            await _send_json(send, 401, {"error": str(exc)})
            return
        except Exception:
            logger.exception("Token resolution failed")
            await _send_json(send, 500, {"error": "Authentication backend error"})
            return

        await _ensure_started()

        with request_credentials(
            api_key=resolved["api_key"],
            base_url=resolved.get("base_url", base_url),
            can_write=resolved.get("can_write", False),
            # Use frozenset() for no permissions. None means "unscoped" and allows
            # everything, so returning None here would accidentally grant full access.
            scopes=resolved.get("scopes", frozenset()),
        ):
            await manager.handle_request(scope, receive, send)

    return app
