"""Which OAuth permissions a connection was actually given.

Some consent screens let the user untick single permissions (Google's granular
consent, Meta's "Edit access"). A token can then be valid but unable to read
anything. The provider says what it granted; the connection stores it as
`GRANTED_SCOPES`, and this module compares it with what the connector needs.

An unknown grant (nothing recorded) is never treated as missing, so providers
that do not report grants keep working. Only scopes the consent actually asked
for (`REQUESTED_SCOPES`) can be missing: a scope the connector starts needing
later is left to the provider's own error. A connection made before requests
were recorded is judged leniently: it is missing a scope only when it was
granted none of the required ones.
"""

from __future__ import annotations
from typing import Any, Dict, List, Mapping, Optional
from terno_dbi.connectors.api.auth.providers import OAuthProvider, get_provider

# Scopes that only identify the user; never required.
_IDENTITY_SCOPES = frozenset({
    "openid", "email", "profile", "public_profile", "offline_access",
    "offline.access", "refresh_token",
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/userinfo.profile",
})

# How each permission reads on the provider's consent screen, so a message can
# name the box to tick. A scope without a label is shown as-is.
SCOPE_LABELS: Dict[str, str] = {
    "https://www.googleapis.com/auth/analytics.readonly":
        "See and download your Google Analytics data",
    "https://www.googleapis.com/auth/yt-analytics.readonly":
        "View YouTube Analytics reports for your YouTube content",
    "https://www.googleapis.com/auth/yt-analytics-monetary.readonly":
        "View monetary and non-monetary YouTube Analytics reports for your "
        "YouTube content",
    "https://www.googleapis.com/auth/youtube.readonly":
        "View your YouTube account",
    "https://www.googleapis.com/auth/webmasters.readonly":
        "View Search Console data for your verified sites",
    "https://www.googleapis.com/auth/adwords":
        "See, edit, create and delete your Google Ads accounts and data",
    "https://www.googleapis.com/auth/drive.readonly":
        "See and download all your Google Drive files",
    "https://www.googleapis.com/auth/spreadsheets.readonly":
        "See all your Google Sheets spreadsheets",
    "ads_read": "Read your ads performance (ads_read)",
    "pages_show_list": "Show a list of the Pages you manage (pages_show_list)",
    "pages_read_engagement":
        "Read content posted on your Pages (pages_read_engagement)",
    "instagram_basic": "Access your Instagram profile and media (instagram_basic)",
    "instagram_manage_insights":
        "Read your Instagram insights (instagram_manage_insights)",
    "instagram_business_basic": "Access your Instagram profile and media",
    "instagram_business_manage_insights": "Read your Instagram insights",
}

_PROVIDER_NAMES = {"google": "Google", "meta": "Facebook",
                   "instagram_login": "Instagram"}


def parse_scopes(raw: Any) -> set:
    """A scope value as a set; providers separate with spaces or commas."""
    if not raw:
        return set()
    if isinstance(raw, (list, tuple, set, frozenset)):
        raw = " ".join(str(s) for s in raw)
    return {s for s in str(raw).replace(",", " ").split() if s}


def required_scopes(provider: OAuthProvider) -> set:
    """Scopes the connector cannot work without; empty when not enforced."""
    if not provider.enforce_grants:
        return set()
    return parse_scopes(provider.scope) - _IDENTITY_SCOPES - set(provider.optional_scopes)


def missing_required_scopes(provider: Optional[OAuthProvider], granted: Any) -> List[str]:
    """Required scopes absent from `granted`, sorted; [] when the grant is unknown."""
    if provider is None:
        return []
    granted_set = parse_scopes(granted)
    if not granted_set:
        return []
    return sorted(required_scopes(provider) - granted_set)


def scope_label(scope: str) -> str:
    return SCOPE_LABELS.get(scope, scope)


def missing_permission_message(provider: OAuthProvider, missing: List[str]) -> str:
    where = _PROVIDER_NAMES.get(provider.name, provider.name.title())
    boxes = ", ".join(f"“{scope_label(s)}”" for s in missing)
    return (f"Some permissions were not allowed on {where}'s screen. "
            f"Reconnect and allow: {boxes}.")


def _provider_for(connector_key: str, bundle: Mapping[str, Any]) -> Optional[OAuthProvider]:
    return get_provider(connector_key, (bundle or {}).get("LOGIN_METHOD", "") or "")


def missing_scopes(connector_key: str, bundle: Mapping[str, Any]) -> List[str]:
    """Required scopes a stored token bundle lacks."""
    bundle = bundle or {}
    provider = _provider_for(connector_key, bundle)
    missing = missing_required_scopes(provider, bundle.get("GRANTED_SCOPES"))
    if not missing:
        return []
    requested = parse_scopes(bundle.get("REQUESTED_SCOPES"))
    if requested:
        return [s for s in missing if s in requested]
    # Recorded before requests were: block only a grant with none of them.
    return missing if set(missing) == required_scopes(provider) else []


def grant_problem(connector_key: str, bundle: Mapping[str, Any]) -> str:
    """A user-facing message when a required permission was not granted, else ""."""
    missing = missing_scopes(connector_key, bundle)
    if not missing:
        return ""
    return missing_permission_message(_provider_for(connector_key, bundle), missing)


__all__ = [
    "SCOPE_LABELS", "grant_problem", "missing_permission_message",
    "missing_required_scopes", "missing_scopes", "parse_scopes",
    "required_scopes", "scope_label",
]
