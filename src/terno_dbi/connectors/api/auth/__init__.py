"""Auth layer: obtaining, refreshing and scoping credentials.

The OAuth client flow, the provider registry, proactive token refresh, and the
account-level allowlist resolver. Depends on `model` only.
"""

from terno_dbi.connectors.api.auth.oauth import (
    complete_authorization,
    make_ensure_token,
    refresh_access_token,
    start_authorization,
)
from terno_dbi.connectors.api.auth.providers import OAuthProvider, get_provider
from terno_dbi.connectors.api.auth.rbac import filter_accounts, permitted_accounts
from terno_dbi.connectors.api.auth.tokens import (
    ensure_fresh_token,
    token_needs_refresh,
)

__all__ = [
    "OAuthProvider",
    "complete_authorization",
    "ensure_fresh_token",
    "filter_accounts",
    "get_provider",
    "make_ensure_token",
    "permitted_accounts",
    "refresh_access_token",
    "start_authorization",
    "token_needs_refresh",
]
