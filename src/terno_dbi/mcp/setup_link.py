"""Provide a safe way to add database credentials through the web UI.

Database credentials must not be passed through hosted MCP tool arguments,
where they may end up in the model context, conversation history, or client
logs.

Instead, the tool returns a link to the organisation's Terno admin page. The
user enters the credentials there through the normal authenticated web flow.

The link contains no token or credential, so sharing it in a conversation or
log does not grant access.
"""

import logging
from typing import Optional
from urllib.parse import quote

logger = logging.getLogger(__name__)


def _datasource_admin_path() -> str:
    from terno_dbi.core.models import DataSource
    meta = DataSource._meta
    return f"/admin/{meta.app_label}/{meta.model_name}/add/"


SETUP_INSTRUCTION = (
    "Show this link to the user as a clickable link and ask them to add the "
    "connection there, then continue once they confirm. Do not ask them for a "
    "connection string or password — this link exists so the credential never "
    "passes through this conversation. Saving the form there already syncs the "
    "schema (tables and columns), so do not suggest sync_metadata as a next "
    "step — call list_datasources or list_tables directly to confirm it "
    "connected. sync_metadata is only for re-syncing after the database's own "
    "schema changes later, not for a datasource that was just added."
)


def _root_domain() -> Optional[str]:
    """The domain org subdomains hang off, e.g. 'app.terno.ai'."""
    try:
        from django.conf import settings
    except Exception:
        return None

    for name in ("MAIN_DOMAIN", "TERNO_ROOT_DOMAIN"):
        value = (getattr(settings, name, None) or "").strip()
        if value:
            # MAIN_DOMAIN is a bare host in some deployments and a full origin in
            # others; the same inconsistency that produced 'https://http//...'
            # in the login redirect.
            return value.split("://")[-1].strip("/").lstrip(".")
    return None


def _workspace_origin(org_subdomain: Optional[str]) -> Optional[str]:
    """The absolute origin of this organisation's Terno workspace.

    Two deployment shapes:
      * Multi-tenant (ENABLE_SUBDOMAIN true): each org lives at
        `https://<subdomain>.<root>`, so the subdomain is required.
      * Single-host (ENABLE_SUBDOMAIN false): every org is
        served from one host, so we use MAIN_DOMAIN verbatim (keeping its own
        scheme, e.g. `http://127.0.0.1:8000`) with no subdomain. The `/connect`
        endpoint resolves the org from the session there, not the host.

    Returns None when the origin cannot be determined, so callers emit prose
    rather than a broken link.
    """
    try:
        from django.conf import settings
    except Exception:
        return None

    if getattr(settings, "ENABLE_SUBDOMAIN", True):
        if not org_subdomain:
            return None
        root = _root_domain()
        if not root:
            logger.warning(
                "Cannot build a workspace link: neither MAIN_DOMAIN nor "
                "TERNO_ROOT_DOMAIN is set."
            )
            return None
        return f"https://{org_subdomain}.{root}"

    # Single-host: MAIN_DOMAIN as an absolute origin (bare host defaults to https).
    for name in ("MAIN_DOMAIN", "TERNO_ROOT_DOMAIN"):
        value = (getattr(settings, name, None) or "").strip().rstrip("/")
        if value:
            return value if "://" in value else f"https://{value}"
    logger.warning(
        "Cannot build a workspace link: MAIN_DOMAIN is not set."
    )
    return None


def datasource_setup_url(org_subdomain: Optional[str]) -> Optional[str]:
    """A link to the Datasources page of this organisation's Terno workspace.

    Returns None when the origin is unknown, so callers can fall back to naming
    the app rather than emitting a broken URL — a dead link on this path is worse
    than prose, because the user clicks it and gives up.
    """
    origin = _workspace_origin(org_subdomain)
    if not origin:
        return None
    return f"{origin}{_datasource_admin_path()}"


def _manual_connect_base() -> str:
    """Frontend path that opens a manual connector's credentials modal.

    Deployment-configurable (TERNO_MANUAL_CONNECT_PATH) because it is a route in
    the host app's UI, not something ternodbi owns; the connector key is appended
    as the final segment (e.g. `/data-connectors/datasource/mysql`).
    """
    try:
        from django.conf import settings
        raw = getattr(settings, "TERNO_MANUAL_CONNECT_PATH", None)
    except Exception:
        raw = None
    path = (raw or "/data-connectors/datasource").strip().rstrip("/")
    return path if path.startswith("/") else f"/{path}"


def connect_url(org_subdomain: Optional[str], catalog) -> Optional[str]:
    """Build a link that connects (or reconnects) a catalog entry.

    OAuth connectors get the backend `/connect?connector=<key>` endpoint, which
    redirects straight to the provider's consent screen — one click. Manual
    (database) connectors instead get the host app's credentials modal
    (`/data-connectors/datasource/<key>`), so the user lands on the secure form
    rather than the bare Django admin. Neither URL carries a credential.
    """
    if catalog is None:
        return None
    origin = _workspace_origin(org_subdomain)
    if not origin:
        return None
    key = quote(catalog.key, safe="")
    if getattr(catalog, "auth_type", None) == "manual":
        return f"{origin}{_manual_connect_base()}/{key}"
    return f"{origin}/connect?connector={key}"


def setup_handoff(org_subdomain: Optional[str], reason: str) -> dict:
    """The payload a tool returns instead of accepting a credential.

    `reason` states why the conversation cannot do this itself, so the model can
    explain it rather than treating the refusal as an unexplained failure.
    """
    url = datasource_setup_url(org_subdomain)
    payload = {
        "credential_required": True,
        "reason": reason,
        "instruction": SETUP_INSTRUCTION,
    }
    if url:
        payload["setup_url"] = url
    else:
        payload["setup_location"] = (
            "the Datasources section of Terno, after signing in"
        )
    return payload


__all__ = [
    "SETUP_INSTRUCTION",
    "connect_url",
    "datasource_setup_url",
    "setup_handoff",
]
