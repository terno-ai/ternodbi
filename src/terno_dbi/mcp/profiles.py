"""Release channels: which tool surface each MCP endpoint exposes.

A public MCP endpoint is often listed somewhere — a client's connector
directory, a customer's configuration — where its tool list is expected not to
change without notice. New tools still need real clients to be tried against.
So the hosted server runs two endpoints from one codebase:

- `stable` (`/mcp`): the released surface. Its tool list, schemas, discovery
  documents and consent screen change only on purpose;
  `tests/unit/mcp/test_stable_manifest.py` pins them to a snapshot.
- `preview` (`/mcp/preview`): everything implemented, including tools not yet
  released on `stable`. Clients add it by URL.

The channel is fixed per endpoint in code, not by a setting or environment
variable, so no deployment can change what the stable endpoint shows by
mistake.

To release the preview tools on `/mcp`, set `STABLE`'s `connectors=True`: that
one value drives `/mcp`'s tools, schemas, discovery documents and consent
screen. Then regenerate the stable snapshot. Do not mount `/mcp` with
`PREVIEW` instead: its discovery documents name `/mcp/preview`, and clients
reject a `resource` that differs from the URL they connected to.

The active channel lives in a ContextVar set by the HTTP app for each request,
like the request credentials. The MCP SDK runs a session's handlers in the
context of the request that opened it, and each endpoint has its own session
manager, so a session never sees another endpoint's channel.
"""

from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import FrozenSet, Iterable, Iterator, Optional
from urllib.parse import urlparse


@dataclass(frozen=True)
class Profile:
    name: str
    path: str
    # API-source tools, list_connectors, connector write tools, and the schemas
    # and scope that came with them.
    connectors: bool

    @property
    def hidden_scopes(self) -> FrozenSet[str]:
        """Scopes neither advertised nor granted on this endpoint."""
        from terno_dbi.oauth.scopes import CONNECTOR_WRITE
        return frozenset() if self.connectors else frozenset({CONNECTOR_WRITE})

    @property
    def advertised_scopes(self) -> FrozenSet[str]:
        from terno_dbi.oauth.scopes import ALL_SCOPES
        return ALL_SCOPES - self.hidden_scopes


STABLE = Profile(name="stable", path="/mcp", connectors=False)
PREVIEW = Profile(name="preview", path="/mcp/preview", connectors=True)
PROFILES = {p.name: p for p in (STABLE, PREVIEW)}

# Default for stdio and anything outside a request: the stable surface.
_current: ContextVar[Profile] = ContextVar("terno_dbi_mcp_profile", default=STABLE)


def current_profile() -> Profile:
    return _current.get()


@contextmanager
def use_profile(profile: Profile) -> Iterator[Profile]:
    token = _current.set(profile)
    try:
        yield profile
    finally:
        _current.reset(token)


def set_process_profile(profile: Profile) -> None:
    """For stdio, where one process serves one client: set it once at startup."""
    _current.set(profile)


def profile_for_resource(resources: Optional[Iterable[str]]) -> Profile:
    """The channel an OAuth request is for, from its RFC 8707 `resource`.

    MCP clients send the server URL they are connecting to. Anything that does
    not name the preview endpoint — including no `resource` at all — gets the
    stable channel, so the stable consent screen is the default.
    """
    for resource in resources or ():
        path = urlparse(resource).path.rstrip("/")
        if path == PREVIEW.path:
            return PREVIEW
    return STABLE
