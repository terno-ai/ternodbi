"""Per-request credential resolution for MCP servers.

Replaces import-time credentials with ContextVar-based per-request identity,
preventing cross-tenant credential sharing under HTTP.

ContextVar safely isolates concurrent asyncio requests. The client is created
per request because it is lightweight and has no connection pool; do not add
credential-based caching without benchmarking, as it could reintroduce
cross-tenant credential leakage.

HTTP must call require_request_credentials() at startup so missing request
credentials raise an error instead of falling back to process-level credentials.
"""

import logging
import os
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Iterator, Optional

from terno_dbi.client import TernoDBIClient

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RequestCredentials:
    """The calling identity for one MCP request."""

    api_key: str
    base_url: Optional[str] = None
    can_write: bool = True
    scopes: Optional[frozenset] = None


_credentials: ContextVar[Optional[RequestCredentials]] = ContextVar(
    "terno_dbi_request_credentials", default=None
)

_strict = False


class MissingRequestCredentials(RuntimeError):
    """No per-request identity was set, and falling back is not permitted."""


def require_request_credentials(strict: bool = True) -> None:
    """Prevent unauthenticated requests from using environment credentials.

    Call from the HTTP entrypoint before serving. Missing request identity must
    raise instead of falling back to process-level credentials, preventing
    cross-tenant credential leakage.
    """
    global _strict
    _strict = strict
    logger.info("MCP credential resolution strict mode: %s", strict)


def is_strict() -> bool:
    return _strict


@contextmanager
def request_credentials(
    api_key: str,
    base_url: Optional[str] = None,
    can_write: bool = True,
    scopes: Optional[frozenset] = None,
) -> Iterator[RequestCredentials]:
    """Scope the calling identity to this block.

    The HTTP layer wraps each request in this once it has resolved the bearer
    token to a `ServiceToken`.
    """
    creds = RequestCredentials(
        api_key=api_key, base_url=base_url, can_write=can_write, scopes=scopes
    )
    token = _credentials.set(creds)
    try:
        yield creds
    finally:
        _credentials.reset(token)


def current_credentials() -> Optional[RequestCredentials]:
    return _credentials.get()


def can_write() -> bool:
    """Whether the current grant may reach write tools.
    Unscoped means stdio, where the token in the environment is the whole story
    and has always had full access.
    """
    creds = _credentials.get()
    return True if creds is None else creds.can_write


def current_scopes() -> Optional[frozenset]:
    """Granted scopes, or None when unscoped (stdio).

    None and `frozenset()` mean different things and must not be conflated:
    None is "no grant applies, allow everything", an empty set is "a grant that
    permits nothing".
    """
    creds = _credentials.get()
    return None if creds is None else creds.scopes


def current_client() -> TernoDBIClient:
    """Return a client scoped to the current request.

    Under stdio, with no request identity set and strict mode off, this returns a
    client reading the environment — preserving today's behaviour exactly.
    """
    creds = _credentials.get()
    if creds is not None:
        return TernoDBIClient(base_url=creds.base_url, api_key=creds.api_key)

    if _strict:
        raise MissingRequestCredentials(
            "No request credentials are set and environment fallback is "
            "disabled. Every HTTP request must be wrapped in "
            "request_credentials(); falling back here would run the call as the "
            "server's own identity."
        )

    return TernoDBIClient()


class _ClientProxy:
    """Resolves to a per-request client on every attribute access.

    Exists so the 29 handler call sites can keep saying `client.list_tables(...)`
    unchanged. Attribute access — not construction — is the resolution point,
    because the module-level `client` is bound once at import while the identity
    changes per request.
    """

    def __getattr__(self, name: str):
        return getattr(current_client(), name)

    def __repr__(self) -> str:
        creds = _credentials.get()
        scope = "request-scoped" if creds else ("strict/unscoped" if _strict else "environment")
        return f"<TernoDBIClient proxy: {scope}>"


client = _ClientProxy()


def describe_backend() -> str:
    """A log-safe description of where calls will go. Never includes the key."""
    creds = _credentials.get()
    if creds is not None:
        return creds.base_url or "<request base_url unset>"
    if _strict:
        return "<unscoped, strict mode>"
    return os.environ.get("TERNODBI_API_URL") or "http://127.0.0.1:8376"
