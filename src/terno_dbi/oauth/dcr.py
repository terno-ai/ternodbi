"""Dynamic Client Registration (RFC 7591) — validation without Django.

Client's has no client ID until it registers. DCR is an unauthenticated POST
that creates a client record and returns credentials.

Redirect URIs: Claude Code uses a random localhost callback port unless
--callback-port is set. Allow any localhost port per RFC 8252 §7.3; require
HTTPS for all non-loopback URIs to prevent authorization-code exposure.

Rate limiting: DCR is intentionally unauthenticated, so it can be abused for
unbounded client creation. check_registration_rate is the hook for the caller
to enforce registration limits.
"""

import ipaddress
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

MAX_REDIRECT_URIS = 10
MAX_CLIENT_NAME = 200

_LOOPBACK_HOSTS = {"localhost", "127.0.0.1", "::1", "[::1]"}


class InvalidRegistration(ValueError):
    """The registration request is malformed or asks for something unsafe.

    `error` is the RFC 7591 code returned to the client.
    """

    def __init__(self, message: str, error: str = "invalid_client_metadata"):
        super().__init__(message)
        self.error = error


@dataclass
class ClientRegistration:
    """A validated registration request, ready to become an `Application`."""

    client_name: str
    redirect_uris: List[str]
    grant_types: List[str] = field(default_factory=lambda: ["authorization_code", "refresh_token"])
    response_types: List[str] = field(default_factory=lambda: ["code"])
    token_endpoint_auth_method: str = "none"
    scope: Optional[str] = None

    @property
    def is_public(self) -> bool:
        return self.token_endpoint_auth_method == "none"


def _is_loopback(parsed) -> bool:
    host = (parsed.hostname or "").lower()
    if host in _LOOPBACK_HOSTS:
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def validate_redirect_uri(uri: str) -> str:
    if not isinstance(uri, str) or not uri.strip():
        raise InvalidRegistration("redirect_uris entries must be non-empty strings",
                                  error="invalid_redirect_uri")

    parsed = urlparse(uri)

    if parsed.fragment:
        raise InvalidRegistration(f"redirect_uri must not contain a fragment: {uri}",
                                  error="invalid_redirect_uri")

    if parsed.scheme == "http":
        # http is acceptable only on loopback, where the code cannot leave the
        # machine. Anywhere else it would put an authorization code on the wire
        # in plaintext.
        if not _is_loopback(parsed):
            raise InvalidRegistration(
                f"http redirect_uri is only allowed on loopback, got: {uri}",
                error="invalid_redirect_uri",
            )
        return uri

    if parsed.scheme == "https":
        if not parsed.hostname:
            raise InvalidRegistration(f"redirect_uri has no host: {uri}",
                                      error="invalid_redirect_uri")
        return uri

    # Custom schemes are how native apps receive callbacks; allow a plausible
    # one rather than blocking a whole client class, but reject the empty and
    # dangerous cases.
    if parsed.scheme and "." in parsed.scheme:
        return uri

    raise InvalidRegistration(
        f"redirect_uri scheme must be https, loopback http, or a reverse-domain "
        f"custom scheme, got: {uri}",
        error="invalid_redirect_uri",
    )


def validate_registration(body: Dict[str, Any]) -> ClientRegistration:
    if not isinstance(body, dict):
        raise InvalidRegistration("Request body must be a JSON object")

    raw_uris = body.get("redirect_uris")
    if not isinstance(raw_uris, list) or not raw_uris:
        raise InvalidRegistration("redirect_uris is required and must be a non-empty array",
                                  error="invalid_redirect_uri")
    if len(raw_uris) > MAX_REDIRECT_URIS:
        raise InvalidRegistration(f"At most {MAX_REDIRECT_URIS} redirect_uris are allowed",
                                  error="invalid_redirect_uri")

    redirect_uris = [validate_redirect_uri(u) for u in raw_uris]

    client_name = (body.get("client_name") or "Unnamed MCP client").strip()
    if len(client_name) > MAX_CLIENT_NAME:
        raise InvalidRegistration(f"client_name exceeds {MAX_CLIENT_NAME} characters")

    grant_types = body.get("grant_types") or ["authorization_code", "refresh_token"]
    unsupported = set(grant_types) - {"authorization_code", "refresh_token"}
    if unsupported:
        raise InvalidRegistration(
            f"Unsupported grant_types: {' '.join(sorted(unsupported))}",
            error="invalid_client_metadata",
        )
    if "authorization_code" not in grant_types:
        raise InvalidRegistration("grant_types must include authorization_code")

    response_types = body.get("response_types") or ["code"]
    if set(response_types) - {"code"}:
        raise InvalidRegistration("Only the 'code' response_type is supported",
                                  error="invalid_client_metadata")

    auth_method = body.get("token_endpoint_auth_method", "none")
    if auth_method not in {"none", "client_secret_post", "client_secret_basic"}:
        raise InvalidRegistration(f"Unsupported token_endpoint_auth_method: {auth_method}")

    scope = body.get("scope")
    if scope is not None:
        from terno_dbi.oauth.scopes import parse_scope_string

        # Raises UnknownScope, which the view maps to invalid_client_metadata.
        parse_scope_string(scope)

    return ClientRegistration(
        client_name=client_name,
        redirect_uris=redirect_uris,
        grant_types=list(grant_types),
        response_types=list(response_types),
        token_endpoint_auth_method=auth_method,
        scope=scope,
    )


def registration_response(
    registration: ClientRegistration,
    client_id: str,
    client_secret: Optional[str],
    issued_at: int,
) -> Dict[str, Any]:
    """The RFC 7591 success body."""
    payload: Dict[str, Any] = {
        "client_id": client_id,
        "client_id_issued_at": issued_at,
        "client_name": registration.client_name,
        "redirect_uris": registration.redirect_uris,
        "grant_types": registration.grant_types,
        "response_types": registration.response_types,
        "token_endpoint_auth_method": registration.token_endpoint_auth_method,
    }
    if client_secret:
        payload["client_secret"] = client_secret
        # 0 means "does not expire" per RFC 7591.
        payload["client_secret_expires_at"] = 0
    if registration.scope:
        payload["scope"] = registration.scope
    return payload
