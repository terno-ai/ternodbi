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

logger = logging.getLogger(__name__)

# Terno's admin route for adding a datasource, on the org's own subdomain.
DATASOURCE_ADMIN_PATH = "/admin/terno/datasource/add/"

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


def datasource_setup_url(org_subdomain: Optional[str]) -> Optional[str]:
    """A link to the Datasources page of this organisation's Terno workspace.

    Returns None when the organisation or root domain is unknown, so callers can
    fall back to naming the app rather than emitting a broken URL — a dead link on
    this path is worse than prose, because the user clicks it and gives up.
    """
    if not org_subdomain:
        return None
    root = _root_domain()
    if not root:
        logger.warning(
            "Cannot build a datasource setup link: neither MAIN_DOMAIN nor "
            "TERNO_ROOT_DOMAIN is set."
        )
        return None
    return f"https://{org_subdomain}.{root}{DATASOURCE_ADMIN_PATH}"


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
    "DATASOURCE_ADMIN_PATH",
    "SETUP_INSTRUCTION",
    "datasource_setup_url",
    "setup_handoff",
]
