"""Build the OAuth discovery documents required by MCP clients.

These are pure functions that return dictionaries, making them easy to test
against the relevant RFCs without a Django request.

The protected `/mcp` endpoint uses `WWW-Authenticate` to point clients to the
RFC 9728 resource metadata. That metadata points to the authorization server,
whose RFC 8414 metadata describes its endpoints and supported features.

The resource and authorization servers use separate hosts because `/mcp` and
the existing authentication flow are served by different vhosts.
"""

from typing import Any, Dict
from terno_dbi.oauth.scopes import ALL_SCOPES, scope_string


def protected_resource_metadata(resource_url: str, auth_server_url: str) -> Dict[str, Any]:
    """RFC 9728. Served at `/.well-known/oauth-protected-resource`."""
    return {
        "resource": f"{resource_url.rstrip('/')}/mcp",
        "authorization_servers": [auth_server_url.rstrip("/")],
        "scopes_supported": sorted(ALL_SCOPES),
        "bearer_methods_supported": ["header"],
        "resource_documentation": f"{resource_url.rstrip('/')}/docs",
    }


def authorization_server_metadata(
    auth_server_url: str, authorize_base: str = ""
) -> Dict[str, Any]:
    """Return the RFC 8414 authorization server metadata.

    `authorize_base` separates the browser-based authorization endpoint from the
    programmatic registration and token endpoints. This is required because the
    browser flow can use the mTLS-protected host, while background MCP clients
    cannot provide a client certificate.

    Defaults to `auth_server_url` so single-host deployments continue to work.
    """
    base = auth_server_url.rstrip("/")
    authz = (authorize_base or auth_server_url).rstrip("/")
    return {
        "issuer": base,
        "authorization_endpoint": f"{authz}/oauth/authorize/",
        "token_endpoint": f"{base}/oauth/token/",
        "revocation_endpoint": f"{base}/oauth/revoke_token/",
        "registration_endpoint": f"{base}/oauth/register",
        "scopes_supported": sorted(ALL_SCOPES),
        "response_types_supported": ["code"],
        "grant_types_supported": ["authorization_code", "refresh_token"],
        "code_challenge_methods_supported": ["S256"],
        "token_endpoint_auth_methods_supported": [
            "client_secret_post",
            "client_secret_basic",
            "none",
        ],
        "service_documentation": f"{base}/docs",
    }


def scope_consent_rows(scopes) -> list:
    """Scope descriptions for the consent screen, in a stable order."""
    from terno_dbi.oauth.scopes import SCOPE_DESCRIPTIONS

    return [
        {"scope": s, "description": SCOPE_DESCRIPTIONS[s]}
        for s in sorted(scopes)
        if s in SCOPE_DESCRIPTIONS
    ]


__all__ = [
    "protected_resource_metadata",
    "authorization_server_metadata",
    "scope_consent_rows",
    "scope_string",
]
