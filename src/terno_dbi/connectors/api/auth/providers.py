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
from typing import Callable, Dict, Optional, Union


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
    # Extra fields added to the token-exchange (and refresh) POST body. Shopify
    # uses `expiring=1` here to request an expiring offline token — non-expiring
    # offline tokens are no longer accepted by the Admin API.
    extra_token_params: Dict[str, str] = field(default_factory=dict)
    # When True, `authorization_url` and `token_url` are templates containing
    # `{instance}` (e.g. Shopify's per-store domain) that the flow fills in from
    # a store name the user supplies before connecting.
    requires_instance: bool = False

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


# Salesforce authenticates against login.salesforce.com for production orgs and
# test.salesforce.com for sandboxes, and an org with a My Domain may use its own
# host. The endpoint is therefore per-deployment rather than a constant, and is
# read at call time so a sandbox can be pointed at without a code release.
_SALESFORCE_LOGIN_ENV = "TERNO_SALESFORCE_LOGIN_URL"
_SALESFORCE_DEFAULT_LOGIN = "https://login.salesforce.com"


def salesforce_login_url() -> str:
    return ((os.getenv(_SALESFORCE_LOGIN_ENV, "").strip()
             or _SALESFORCE_DEFAULT_LOGIN).rstrip("/"))


def _salesforce() -> OAuthProvider:
    base = salesforce_login_url()
    return OAuthProvider(
        name="salesforce",
        authorization_url=f"{base}/services/oauth2/authorize",
        token_url=f"{base}/services/oauth2/token",
        # `api` is the read/write REST scope — Salesforce has no read-only
        # variant, so least privilege is enforced by the connected app's profile
        # and permission set, not here. `refresh_token` is what keeps the source
        # alive past the org's session timeout; without it the connection dies
        # in hours.
        scope="api refresh_token",
        client_id_env="TERNO_SALESFORCE_CLIENT_ID",
        client_secret_env="TERNO_SALESFORCE_CLIENT_SECRET",
        use_pkce=True,
    )


_HUBSPOT = OAuthProvider(
    name="hubspot",
    authorization_url="https://app.hubspot.com/oauth/authorize",
    token_url="https://api.hubapi.com/oauth/v1/token",
    scope="crm.objects.contacts.read crm.objects.companies.read "
          "crm.objects.deals.read crm.objects.tickets.read "
          "crm.objects.leads.read crm.objects.owners.read",
    client_id_env="TERNO_HUBSPOT_CLIENT_ID",
    client_secret_env="TERNO_HUBSPOT_CLIENT_SECRET",
    use_pkce=False,
)


_AMAZON_ADS = OAuthProvider(
    name="amazon_ads",
    authorization_url="https://www.amazon.com/ap/oa",
    token_url="https://api.amazon.com/auth/o2/token",
    scope="advertising::campaign_management",
    client_id_env="TERNO_AMAZON_ADS_CLIENT_ID",
    client_secret_env="TERNO_AMAZON_ADS_CLIENT_SECRET",
    use_pkce=False,
)


_MICROSOFT = OAuthProvider(
    name="microsoft",
    authorization_url="https://login.microsoftonline.com/common/oauth2/v2.0/authorize",
    token_url="https://login.microsoftonline.com/common/oauth2/v2.0/token",
    scope="https://ads.microsoft.com/msads.manage offline_access",
    client_id_env="TERNO_MICROSOFT_ADS_CLIENT_ID",
    client_secret_env="TERNO_MICROSOFT_ADS_CLIENT_SECRET",
    use_pkce=True,
)


_SHOPIFY = OAuthProvider(
    name="shopify",
    # Per-store URLs: {instance} is filled with '<store>.myshopify.com'.
    authorization_url="https://{instance}/admin/oauth/authorize",
    token_url="https://{instance}/admin/oauth/access_token",
    scope="read_orders,read_all_orders,read_products,read_customers,"
          "read_inventory,read_locations,read_draft_orders,read_discounts",
    client_id_env="TERNO_SHOPIFY_CLIENT_ID",
    client_secret_env="TERNO_SHOPIFY_CLIENT_SECRET",
    use_pkce=False,
    requires_instance=True,
    # Request an expiring offline token (with a refresh_token); the Admin API no
    # longer accepts non-expiring offline tokens.
    extra_token_params={"expiring": "1"},
)


def _google_with_scope(scope: str) -> OAuthProvider:
    from dataclasses import replace
    return replace(_GOOGLE, scope=f"openid email {scope}")


# Provider per connector key. Scope is the connector's own — read-only wherever
# possible. A value may be a callable when the provider's endpoints depend on
# the environment and so must be built per call rather than at import.
_PROVIDERS: Dict[str, Union[OAuthProvider, Callable[[], OAuthProvider]]] = {
    "googleanalytics4": _google_with_scope(
        "https://www.googleapis.com/auth/analytics.readonly"),
    "youtube": _google_with_scope(
        "https://www.googleapis.com/auth/yt-analytics.readonly "
        "https://www.googleapis.com/auth/yt-analytics-monetary.readonly "
        "https://www.googleapis.com/auth/youtube.readonly"),
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
    "salesforce": _salesforce,
    "microsoft_ads": _MICROSOFT,
    "hubspot": _HUBSPOT,
    "amazon_ads": _AMAZON_ADS,
    "shopify": _SHOPIFY,
}


def get_provider(connector_key: str) -> Optional[OAuthProvider]:
    entry = _PROVIDERS.get(connector_key)
    return entry() if callable(entry) else entry


__all__ = ["OAuthProvider", "get_provider"]
