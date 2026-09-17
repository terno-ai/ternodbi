"""OAuth provider configuration for API connectors.

TernoDBI is an OAuth client for providers such as Google and Meta. This is
separate from `terno_dbi.oauth`, where TernoDBI acts as an OAuth provider for
MCP clients.

Provider endpoints and scopes are connector configuration; client IDs and
secrets come from the environment. Multiple connectors can share a provider,
such as Google Analytics, YouTube, and Google Ads.
"""

from __future__ import annotations
import os
from dataclasses import dataclass, field
from typing import Dict, Optional


@dataclass(frozen=True)
class OAuthProvider:
    name: str
    authorization_url: str
    token_url: str
    scope: str
    client_id_env: str
    client_secret_env: str
    use_pkce: bool = True
    extra_authorize_params: Dict[str, str] = field(default_factory=dict)

    def client_id(self) -> str:
        return os.getenv(self.client_id_env, "").strip()

    def client_secret(self) -> str:
        return os.getenv(self.client_secret_env, "").strip()

    def is_configured(self) -> bool:
        """True when this deployment has credentials for the provider.

        A connector whose provider is unconfigured cannot start a flow — the
        catalog can still list it, but connecting will report the gap rather
        than bounce the user to a broken consent screen.
        """
        return bool(self.client_id() and self.client_secret())


_GOOGLE = OAuthProvider(
    name="google",
    authorization_url="https://accounts.google.com/o/oauth2/v2/auth",
    token_url="https://oauth2.googleapis.com/token",
    scope="",
    client_id_env="TERNO_GOOGLE_OAUTH_CLIENT_ID",
    client_secret_env="TERNO_GOOGLE_OAUTH_CLIENT_SECRET",
    use_pkce=True,
    extra_authorize_params={
        "access_type": "offline",
        "prompt": "consent",
    },
)

_LINKEDIN = OAuthProvider(
    name="linkedin",
    authorization_url="https://www.linkedin.com/oauth/v2/authorization",
    token_url="https://www.linkedin.com/oauth/v2/accessToken",
    scope="r_ads r_ads_reporting",
    client_id_env="TERNO_LINKEDIN_CLIENT_ID",
    client_secret_env="TERNO_LINKEDIN_CLIENT_SECRET",
    # LinkedIn's authorization-code flow authenticates with the client secret;
    # it does not accept a PKCE challenge on this endpoint.
    use_pkce=False,
)

_META = OAuthProvider(
    name="meta",
    authorization_url="https://www.facebook.com/v25.0/dialog/oauth",
    token_url="https://graph.facebook.com/v25.0/oauth/access_token",
    scope="public_profile,ads_read",
    client_id_env="TERNO_META_APP_ID",
    client_secret_env="TERNO_META_APP_SECRET",
    use_pkce=False,   # Meta's flow is not PKCE
)


def _google_with_scope(scope: str) -> OAuthProvider:
    from dataclasses import replace
    return replace(_GOOGLE, scope=scope)


# Provider per connector key. Scope is the connector's own — read-only wherever
# possible.
_PROVIDERS: Dict[str, OAuthProvider] = {
    "googleanalytics4": _google_with_scope(
        "https://www.googleapis.com/auth/analytics.readonly"),
    "youtube": _google_with_scope(
        "https://www.googleapis.com/auth/yt-analytics.readonly "
        "https://www.googleapis.com/auth/yt-analytics-monetary.readonly "
        "https://www.googleapis.com/auth/youtube.readonly "
        "https://www.googleapis.com/auth/youtube.channel-memberships.creator"),
    "google_search_console": _google_with_scope(
        "https://www.googleapis.com/auth/webmasters.readonly"),
    "google_ads": _google_with_scope(
        "https://www.googleapis.com/auth/adwords"),
    # Read-only over the whole drive: file properties, the shared-drive list
    # (which the narrower metadata scope cannot serve), and file contents.
    "google_drive": _google_with_scope(
        "https://www.googleapis.com/auth/drive.readonly"),
    # Two scopes, because the split is real: the Sheets API can read a
    # spreadsheet but cannot *find* one, so discovery goes through Drive. Only
    # file metadata is needed for that, so the narrower Drive scope is used —
    # spreadsheet contents come from the Sheets scope.
    "google_sheets": _google_with_scope(
        "https://www.googleapis.com/auth/spreadsheets.readonly "
        "https://www.googleapis.com/auth/drive.metadata.readonly"),
    "meta_ads": _META,
    "linkedin_ads": _LINKEDIN,
}


def get_provider(connector_key: str) -> Optional[OAuthProvider]:
    return _PROVIDERS.get(connector_key)


__all__ = ["OAuthProvider", "get_provider"]
