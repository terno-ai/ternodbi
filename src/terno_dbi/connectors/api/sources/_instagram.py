"""Shared Instagram Graph API plumbing.

Both Instagram connectors — Insights (your own account's performance) and Public
Data (public metrics of other accounts / hashtags) — talk to the same Instagram
Graph API on the same Meta OAuth token, and both reach the *querying* account the
same way: an Instagram Business/Creator account is linked to a Facebook Page, so
discovery walks `GET /me/accounts` and picks each Page's
`instagram_business_account`.

This module holds the transport and that shared discovery so the two connector
modules stay small and focused on their own reports.
"""

from __future__ import annotations
from typing import Any, Callable, Dict, List, Optional

from terno_dbi.connectors.api.model.types import Account

# Instagram is part of the Meta Graph API; keep the version pinned with meta_ads.
API_VERSION = "v25.0"
BASE = f"https://graph.facebook.com/{API_VERSION}"
IG_LOGIN_BASE = "https://graph.instagram.com"


class AuthError(Exception):
    """Internal marker for a 401 from Meta, mapped to AUTH_EXPIRED by callers."""


def default_http(method: str, url: str, token: str,
                 params: Optional[Dict] = None) -> Dict[str, Any]:
    """Graph API transport.
    """
    import requests

    resp = requests.request(
        method, url, headers={"Authorization": f"Bearer {token}"},
        params=params or {}, timeout=30)
    if resp.status_code == 401:
        raise AuthError()
    resp.raise_for_status()
    # A DELETE may return an empty body; treat that as success.
    return resp.json() if resp.content else {"success": True}


def discover_ig_accounts(call: Callable[..., Dict[str, Any]]) -> List[Account]:
    """The Instagram Business/Creator accounts this credential can act as.

    `call(method, url, params)` performs an authorised Graph request. Each linked
    Facebook Page may expose one `instagram_business_account`; those are the ids
    Instagram Graph endpoints accept. An `@username` name is preferred for display.
    """
    data = call(
        "GET", f"{BASE}/me/accounts",
        {"fields": "name,instagram_business_account{id,username,name}",
         "limit": 200})
    accounts: List[Account] = []
    seen = set()
    for page in data.get("data", []):
        ig = page.get("instagram_business_account") or {}
        ig_id = ig.get("id")
        if not ig_id or ig_id in seen:
            continue
        seen.add(ig_id)
        username = ig.get("username")
        display = f"@{username}" if username else (ig.get("name")
                                                   or page.get("name") or ig_id)
        accounts.append(Account(id=str(ig_id), name=str(display)))
    return accounts


def discover_ig_login_account(call: Callable[..., Dict[str, Any]]) -> List[Account]:
    """The single Instagram account behind an Instagram-Login token.

    With Instagram Login the token *is* one professional account (no Facebook
    Page walk), so `GET /me` returns that account directly.
    """
    data = call("GET", f"{IG_LOGIN_BASE}/me",
                {"fields": "user_id,username"})
    uid = str(data.get("user_id") or data.get("id") or "")
    if not uid:
        return []
    username = data.get("username")
    name = f"@{username}" if username else uid
    return [Account(id=uid, name=name)]


__all__ = ["API_VERSION", "BASE", "IG_LOGIN_BASE", "AuthError", "default_http",
           "discover_ig_accounts", "discover_ig_login_account"]
