"""Streamable HTTP transport, and the authentication that must travel with it.

## Why auth lives here and not in Django middleware

The app is mounted in terno-ai's `mysite/asgi.py` under
`ProtocolTypeRouter["http"]`, which means `/mcp` never enters Django's URL
resolver — and therefore never passes through `MIDDLEWARE`.
`ServiceTokenMiddleware` does not run. `SubdomainOrganisationMiddleware` does not
run. Nothing sets `request.service_token`, because no Django `request` is ever
built.

That is not an oversight in the mount: `StreamableHTTPSessionManager` is a raw
ASGI app that needs `send`/`receive` to stream, which a Django view cannot give
it. So the transport has to authenticate itself, using the same `verify_token`
call the middleware uses.

## Lazy session-manager startup

`StreamableHTTPSessionManager.handle_request` requires `run()` to have been
entered — it raises otherwise, in both stateful and stateless mode. `run()` is
designed for a Starlette lifespan, and there is no lifespan for a sub-app
mounted inside `ProtocolTypeRouter`. So it is entered once, lazily, in a
long-lived background task on the first request, guarded by a lock.

## Failing closed

`require_request_credentials()` is called when the app is built, so an
unauthenticated path cannot silently fall back to the server's own environment
credentials — which would run a stranger's request as Terno. After that, an
unscoped resolution raises rather than transacting.
"""

import asyncio
import json
import logging
from typing import Any, Awaitable, Callable, Dict, Optional

from mcp.server.streamable_http_manager import StreamableHTTPSessionManager

from terno_dbi.mcp.context import request_credentials, require_request_credentials

logger = logging.getLogger(__name__)

MCP_PATH = "/mcp"

# RFC 9728: the 401 points clients at the resource metadata, from which they
# discover the authorization server. Claude Code runs sign-in automatically on
# seeing this, rather than surfacing a bare 401 to the user.
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

        # Write access needs both halves, and they answer different questions:
        # the scope is what the *client* was granted at consent, the group is
        # what the *user* may do in this org. Either alone leaves a hole.
        summary = token_grant_summary(token)
        return {
            "api_key": token_str,
            "can_write": summary["can_write"],
            "scopes": summary["scopes"],
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
    """Return a raw ASGI app serving `server` over Streamable HTTP.

    `token_resolver` is injectable so the transport can be tested without a
    database; it defaults to the real `ServiceToken` lookup.

    `in_process=True` when mounted inside the Django process that serves the
    TernoDBI API — it stops every tool call making an HTTP request to
    `127.0.0.1` from the process handling it, which wastes a worker per call and
    deadlocks once concurrent calls exceed the worker count.
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
            # frozenset() — a grant that permits nothing — rather than None,
            # which would mean "unscoped, allow everything". Defaulting the
            # wrong way here would open every tool to a token whose resolver
            # returned no scopes.
            scopes=resolved.get("scopes", frozenset()),
        ):
            await manager.handle_request(scope, receive, send)

    return app
