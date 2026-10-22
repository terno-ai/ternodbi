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
from typing import Callable, Dict, FrozenSet, Optional, Union


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
    # When True, `authorization_url`, `token_url` and `scope` are templates
    # containing `{instance}` (Shopify's per-store domain, a Salesforce My
    # Domain, a Dynamics 365 environment host) that the flow fills in from a
    # host the user supplies before connecting.
    requires_instance: bool = False
    # When set, the provider names the user's regional server only at the
    # callback, in this query parameter (Zoho's `accounts-server`). `token_url`
    # is then an `{instance}` template filled from that host, which must be one
    # of `callback_instances` — it arrives in the browser redirect, so it is
    # untrusted, and the token POST carries our client secret.
    callback_instance_param: str = ""
    callback_instances: FrozenSet[str] = frozenset()
    # When True, the token exchange and refresh authenticate the client with an
    # HTTP Basic header rather than `client_id`/`client_secret` in the body
    # (Pipedrive accepts the body form but advises against it).
    token_auth_basic: bool = False
    # The consent URL's name for the client id. TikTok's portal takes `app_id`.
    client_id_param: str = "client_id"
    # When True, the token exchange is TikTok's: a JSON body of `app_id`,
    # `secret` and `auth_code`, answered inside a `{code, message, data}`
    # envelope rather than as a bare OAuth token response.
    token_json_envelope: bool = False
    # When set, the provider speaks OAuth 1.0a (X Ads) and this is its
    # request-token URL; see `oauth1`. `authorization_url` is then where the
    # user approves the request token and `token_url` swaps it for the user's
    # access token. The client id and secret are the app's consumer key and
    # secret, and `scope` and `use_pkce` are unused.
    request_token_url: str = ""

    @property
    def uses_instance(self) -> bool:
        """True when `token_url` is an `{instance}` template."""
        return self.requires_instance or bool(self.callback_instance_param)

    @property
    def is_oauth1(self) -> bool:
        return bool(self.request_token_url)

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

_FACEBOOK_PAGES = OAuthProvider(
    name="meta",
    authorization_url="https://www.facebook.com/v25.0/dialog/oauth",
    token_url="https://graph.facebook.com/v25.0/oauth/access_token",
    scope="public_profile,pages_show_list,pages_read_engagement,"
          "pages_read_user_content,business_management",
    client_id_env="TERNO_META_APP_ID",
    client_secret_env="TERNO_META_APP_SECRET",
    use_pkce=False,
)

_INSTAGRAM = OAuthProvider(
    name="meta",
    authorization_url="https://www.facebook.com/v25.0/dialog/oauth",
    token_url="https://graph.facebook.com/v25.0/oauth/access_token",
    scope="public_profile,instagram_basic,instagram_manage_insights,"
          "pages_show_list,pages_read_engagement,business_management,"
          "instagram_content_publish,instagram_manage_comments,"
          "instagram_manage_engagement",
    client_id_env="TERNO_META_APP_ID",
    client_secret_env="TERNO_META_APP_SECRET",
    use_pkce=False,
)

_INSTAGRAM_LOGIN = OAuthProvider(
    name="instagram_login",
    authorization_url="https://www.instagram.com/oauth/authorize",
    token_url="https://api.instagram.com/oauth/access_token",
    scope="instagram_business_basic,instagram_business_manage_insights,"
          "instagram_business_content_publish,instagram_business_manage_comments",
    client_id_env="TERNO_INSTAGRAM_APP_ID",
    client_secret_env="TERNO_INSTAGRAM_APP_SECRET",
    use_pkce=False,
)


_SALESFORCE = OAuthProvider(
    name="salesforce",
    authorization_url="https://{instance}/services/oauth2/authorize",
    token_url="https://{instance}/services/oauth2/token",
    scope="api refresh_token openid email",
    client_id_env="TERNO_SALESFORCE_CLIENT_ID",
    client_secret_env="TERNO_SALESFORCE_CLIENT_SECRET",
    use_pkce=True,
    requires_instance=True,
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


_DYNAMICS365 = OAuthProvider(
    name="dynamics365",
    # `organizations`, not `common`: Dataverse has no personal-account users.
    authorization_url="https://login.microsoftonline.com/organizations/oauth2/v2.0/authorize",
    token_url="https://login.microsoftonline.com/organizations/oauth2/v2.0/token",
    # Every Dataverse environment is its own resource, so the scope names the one
    # the user chose ({instance} is '<org>.crm<N>.dynamics.com').
    # `user_impersonation` is Dataverse's only delegated permission and is both
    # the read and the write scope: it acts as the signed-in user, so reads and
    # the connector's create/update writes reach whatever their Dynamics 365
    # security roles allow. There is no narrower read-only or write-only scope.
    # `openid email` names the connected user; `offline_access` issues the
    # refresh token.
    scope="https://{instance}/user_impersonation offline_access openid email",
    client_id_env="TERNO_DYNAMICS365_CLIENT_ID",
    client_secret_env="TERNO_DYNAMICS365_CLIENT_SECRET",
    use_pkce=True,
    requires_instance=True,
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


_ZOHO = OAuthProvider(
    name="zoho",
    # Consent starts at the US server, which forwards the user to their own data
    # centre; the callback then names that centre's accounts server, and the
    # token exchange and every refresh must go there.
    authorization_url="https://accounts.zoho.com/oauth/v2/auth",
    token_url="https://{instance}/oauth/v2/token",
    # Record write access (create/update, never delete) is requested up front so
    # write features need no reconnect; nothing in the connector writes yet.
    scope="ZohoCRM.modules.READ,ZohoCRM.modules.CREATE,ZohoCRM.modules.UPDATE,"
          "ZohoCRM.coql.READ,ZohoCRM.settings.fields.READ,"
          "ZohoCRM.org.READ,ZohoCRM.users.READ",
    client_id_env="TERNO_ZOHO_CLIENT_ID",
    client_secret_env="TERNO_ZOHO_CLIENT_SECRET",
    use_pkce=False,
    extra_authorize_params={
        "access_type": "offline",   # issue a refresh token
        "prompt": "consent",        # ...on every connect, not just the first
    },
    callback_instance_param="accounts-server",
    callback_instances=frozenset({
        "accounts.zoho.com",
        "accounts.zoho.eu",
        "accounts.zoho.in",
        "accounts.zoho.com.au",
        "accounts.zoho.jp",
        "accounts.zoho.com.cn",
        "accounts.zohocloud.ca",
        "accounts.zoho.sa",
        "accounts.zoho.uk",
    }),
)


_PIPEDRIVE = OAuthProvider(
    name="pipedrive",
    authorization_url="https://oauth.pipedrive.com/oauth/authorize",
    token_url="https://oauth.pipedrive.com/oauth/token",
    # Pipedrive takes no scope at consent: the app's scopes are chosen in its
    # Developer Hub settings — deals:full, contacts:full, activities:full,
    # leads:full and users:read. `:full` is Pipedrive's only write scope and
    # includes delete; the connector itself never deletes or archives. The token
    # response reports what was granted.
    scope="",
    client_id_env="TERNO_PIPEDRIVE_CLIENT_ID",
    client_secret_env="TERNO_PIPEDRIVE_CLIENT_SECRET",
    use_pkce=False,
    token_auth_basic=True,
)


_TIKTOK = OAuthProvider(
    name="tiktok",
    authorization_url="https://business-api.tiktok.com/portal/auth",
    token_url="https://business-api.tiktok.com/open_api/v1.3/oauth2/access_token/",
    # TikTok takes no scope at consent: the app's permissions are chosen on the
    # app in the TikTok API for Business portal — Ad Account Management (the
    # advertiser list), Ads Management (the Campaigns/AdGroups/Ads reports) and
    # Reporting. The token response lists the granted permission ids. The
    # access token is long-term and comes with no refresh token.
    scope="",
    client_id_env="TERNO_TIKTOK_APP_ID",
    client_secret_env="TERNO_TIKTOK_APP_SECRET",
    use_pkce=False,
    client_id_param="app_id",
    token_json_envelope=True,
)


_X = OAuthProvider(
    name="x",
    authorization_url="https://x.com/i/oauth2/authorize",
    token_url="https://api.x.com/2/oauth2/token",
    # Reads: the user's profile and posts (with the owner-only metrics on
    # them). Writes are requested up front so write features need no
    # reconnect: post, reply, repost and delete (`tweet.write`), hide replies
    # (`tweet.moderate.write`), like (`like.write`) and upload a post's images
    # or video (`media.write`). DMs, follows, lists, blocks and mutes are left
    # out. `offline.access` issues the refresh token; the access token lasts
    # two hours.
    scope="tweet.read users.read offline.access tweet.write "
          "tweet.moderate.write like.write media.write",
    client_id_env="TERNO_X_CLIENT_ID",
    client_secret_env="TERNO_X_CLIENT_SECRET",
    use_pkce=True,
    # A confidential client authenticates the exchange and every refresh with
    # HTTP Basic.
    token_auth_basic=True,
)


_X_ADS = OAuthProvider(
    name="x_ads",
    # The X Ads API takes only OAuth 1.0a, with the app's consumer keys (the
    # "API Key and Secret" in the X developer portal) rather than its OAuth 2.0
    # client. OAuth 1.0a has no scopes: the token gets read and write access
    # (see `oauth1.fetch_request_token`), capped by the app's own permission
    # setting, so the app must be set to "Read and write". The user's access
    # token and secret do not expire.
    authorization_url="https://api.x.com/oauth/authorize",
    token_url="https://api.x.com/oauth/access_token",
    request_token_url="https://api.x.com/oauth/request_token",
    scope="",
    client_id_env="TERNO_X_CONSUMER_KEY",
    client_secret_env="TERNO_X_CONSUMER_SECRET",
    use_pkce=False,
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
    # spreadsheet but cannot *find* one, so discovery goes through Drive.
    # Spreadsheet contents come from the Sheets scope.
    "google_sheets": _google_with_scope(
        "https://www.googleapis.com/auth/spreadsheets.readonly "
        "https://www.googleapis.com/auth/drive.readonly"),
    "meta_ads": _META,
    "facebook_pages": _FACEBOOK_PAGES,
    "instagram_insights": _INSTAGRAM,
    "instagram_public": _INSTAGRAM,
    "linkedin_ads": _LINKEDIN,
    "tiktok_ads": _TIKTOK,
    "x_analytics": _X,
    "x_ads": _X_ADS,
    "salesforce": _SALESFORCE,
    "microsoft_ads": _MICROSOFT,
    "dynamics365": _DYNAMICS365,
    "hubspot": _HUBSPOT,
    "amazon_ads": _AMAZON_ADS,
    "shopify": _SHOPIFY,
    "zoho_crm": _ZOHO,
    "pipedrive": _PIPEDRIVE,
}


_LOGIN_METHOD_PROVIDERS: Dict[tuple, Union[OAuthProvider, Callable[[], OAuthProvider]]] = {
    ("instagram_insights", "instagram"): _INSTAGRAM_LOGIN,
    ("instagram_insights", "facebook"): _INSTAGRAM,
}

LOGIN_METHODS: Dict[str, list] = {
    "instagram_insights": ["instagram", "facebook"],
}


def login_methods(connector_key: str) -> list:
    """Ordered login methods a connector offers (first = recommended); [] if one."""
    return list(LOGIN_METHODS.get(connector_key, []))


def get_provider(connector_key: str,
                 login_method: str = "") -> Optional[OAuthProvider]:
    if login_method:
        entry = _LOGIN_METHOD_PROVIDERS.get((connector_key, login_method))
        if entry is not None:
            return entry() if callable(entry) else entry
    entry = _PROVIDERS.get(connector_key)
    return entry() if callable(entry) else entry


__all__ = ["OAuthProvider", "get_provider", "login_methods", "LOGIN_METHODS"]
