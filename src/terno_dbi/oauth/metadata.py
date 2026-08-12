"""Build the OAuth discovery documents required by MCP clients.

Pure functions returning dicts, so they can be tested directly against the
relevant RFCs without a Django request.

The protected `/mcp` endpoint points clients to RFC 9728 metadata via
`WWW-Authenticate`; these documents provide the discovery chain needed to
start authentication.

RFC 9728 metadata is served on the resource host (`mcp.terno.ai`) and points
to the authorization server. RFC 8414 metadata is served on the auth host
(`app.terno.ai`) and describes its endpoints and supported features.

The hosts are intentionally separate because the MCP endpoint and existing
allauth session are served by different vhosts.
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


def authorization_server_metadata(auth_server_url: str) -> Dict[str, Any]:
    """RFC 8414. Served at `/.well-known/oauth-authorization-server`."""
    base = auth_server_url.rstrip("/")
    return {
        "issuer": base,
        "authorization_endpoint": f"{base}/oauth/authorize/",
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
