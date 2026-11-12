"""Shopify App Store integration: install, claim, and app webhooks.

A public Shopify app is installed from Shopify, not from Terno, and Shopify
requires OAuth to run before the merchant sees any of the app's own screens
(App Store requirement 2.3.2). So the store is authorized first and attached to
a Terno organisation afterwards, once the merchant has signed in.

## Install

1. `GET /connectors/shopify/install/` is the app URL. Shopify opens it with a
   signed query string. The signature, a recent timestamp and a
   `*.myshopify.com` shop are checked, then the merchant goes straight to
   Shopify's consent screen. The OAuth state is also set in a cookie limited to
   the callback path, so the callback only completes in the browser that
   started it.
2. Shopify redirects to the usual callback (`/connectors/oauth/callback/`),
   which hands an install to `complete_install`. That checks the state cookie,
   Shopify's signature on the callback and the shop, exchanges the code, and
   holds the token in a `ShopifyPendingInstall` row. The browser gets a random
   claim secret in a cookie (only its hash is stored). The merchant is then sent
   to sign in, with `next=/connect?connector=shopify&shop=…&install=1`.
3. After sign-in (or sign-up, email verification and onboarding: the login
   service keeps `next` throughout), `/connect` runs in the merchant's
   organisation. It checks membership and admin rights as for any connector,
   then `claim_install` moves the token onto the organisation's Shopify
   datasource and deletes the pending row.

The App URL must be on the same host as the OAuth callback (the canonical host,
`MAIN_DOMAIN`), or the state cookie is not sent back. The claim cookie is scoped
to `MAIN_DOMAIN` and its subdomains so it reaches the organisation's host.

## Webhooks (`POST /connectors/shopify/webhooks/`)

One endpoint for every topic. The body signature (`X-Shopify-Hmac-Sha256`) is
verified first and anything unsigned gets a 401. Then:

- `app/uninstalled`: the store's tokens are removed and its connections are
  marked not authenticated. The store domain is kept, so a later `shop/redact`
  can still find the rows. Unclaimed installs for the store are deleted.
- `shop/redact` (48 hours after uninstall): the store's connections are deleted,
  along with what cascades from them (schema metadata, memories, stored query
  results, account selections), and any unclaimed install.
- `customers/redact`: stored query results for the store are deleted, since they
  may contain that customer.
- `customers/data_request`: logged for manual follow-up. Terno keeps no
  per-customer Shopify profile to export.

The topic and shop headers are not covered by the signature, so the shop named
in the signed body must match the `X-Shopify-Shop-Domain` header.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import re
import secrets as _secrets
import time
from datetime import timedelta
from typing import Callable, Dict, List, Mapping, Optional
from urllib.parse import quote, urlencode

from django.conf import settings
from django.db import transaction
from django.http import HttpResponse
from django.shortcuts import redirect
from django.utils import timezone
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_GET, require_POST

logger = logging.getLogger(__name__)

CONNECTOR_KEY = "shopify"

# Same rule the OAuth flow applies to the store domain (see `_INSTANCE_RULES` in
# auth/oauth.py), but matched against the raw value: no scheme, path or bare
# store name is accepted, because Shopify always sends the full host.
SHOP_DOMAIN_RE = re.compile(r"^[a-z0-9][a-z0-9-]{0,59}\.myshopify\.com$")

# How old an install link may be. Shopify signs the timestamp when it opens the
# app, so a short window limits replaying a captured link. The allowance for a
# timestamp in the future absorbs clock skew.
INSTALL_LINK_MAX_AGE_SECONDS = 3600
INSTALL_LINK_MAX_FUTURE_SKEW_SECONDS = 300

# How long an authorized store waits to be claimed by a signed-in organisation.
# Long enough for sign-up and email verification, short enough that an
# abandoned token does not linger.
CLAIM_TTL_SECONDS = 30 * 60

# The browser binding for each step. Both are HttpOnly and never leave Terno.
STATE_COOKIE = "terno_shopify_state"
CLAIM_COOKIE = "terno_shopify_claim"
CLAIM_COOKIE_PATH = "/connect"

EXPIRED_MESSAGE = (
    "This Shopify install has expired or was opened in another browser. Open the "
    "Terno app again from your Shopify admin."
)

UNINSTALLED_MESSAGE = (
    "The Terno app was uninstalled from this Shopify store. Install it again "
    "from the Shopify App Store to reconnect."
)


# --------------------------------------------------------------------------
# Signatures
# --------------------------------------------------------------------------

def _client_secret() -> str:
    from terno_dbi.connectors.api.auth.providers import get_provider

    provider = get_provider(CONNECTOR_KEY)
    return provider.client_secret() if provider else ""


def _escape_key(key: str) -> str:
    return key.replace("%", "%25").replace("=", "%3D")


def _escape_value(value: str) -> str:
    return value.replace("%", "%25")


def query_signature_message(params) -> str:
    """The string Shopify signs for an app URL or OAuth redirect.

    Every parameter except `hmac`, as `key=value`, sorted and joined with `&`.
    `%` and `=` in keys, `%` in values and `&` anywhere are escaped so one pair
    cannot pass for two. An array parameter (`ids[]=1&ids[]=2`) is signed as
    `ids=["1", "2"]`. This matches Shopify's own libraries.

    `params` is a Django `QueryDict` (or anything with `keys()` and `getlist()`).
    Raises `ValueError` for a repeated non-array parameter: Shopify never sends
    one, and accepting it would make the signed value ambiguous.
    """
    pairs: List[str] = []
    for key in params.keys():
        if key == "hmac":
            continue
        values = params.getlist(key)
        if key.endswith("[]"):
            name = key[:-2]
            value = json.dumps([str(v) for v in values])
        else:
            if len(values) != 1:
                raise ValueError(f"repeated query parameter {key!r}")
            name, value = key, str(values[0])
        pairs.append(f"{_escape_key(name)}={_escape_value(value)}".replace("&", "%26"))
    return "&".join(sorted(pairs))


def verify_query_hmac(params, secret: str) -> bool:
    """True when `params` carries a valid Shopify `hmac` for `secret`."""
    if not secret:
        return False
    received = params.getlist("hmac")
    if len(received) != 1 or not received[0]:
        return False
    try:
        message = query_signature_message(params)
    except ValueError:
        return False
    expected = hmac.new(secret.encode(), message.encode(), hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected.encode(), received[0].strip().lower().encode())


def verify_webhook_hmac(body: bytes, header: str, secret: str) -> bool:
    """True when `header` is the base64 HMAC-SHA256 of the raw `body`."""
    if not secret or not header:
        return False
    digest = base64.b64encode(
        hmac.new(secret.encode(), body, hashlib.sha256).digest())
    return hmac.compare_digest(digest, header.strip().encode())


def _timestamp_is_fresh(raw: Optional[str], now: Optional[float] = None) -> bool:
    try:
        timestamp = int(raw)
    except (TypeError, ValueError):
        return False
    now = time.time() if now is None else now
    age = now - timestamp
    return -INSTALL_LINK_MAX_FUTURE_SKEW_SECONDS <= age <= INSTALL_LINK_MAX_AGE_SECONDS


def is_valid_shop_domain(shop) -> bool:
    return isinstance(shop, str) and SHOP_DOMAIN_RE.fullmatch(shop) is not None


# --------------------------------------------------------------------------
# Install: Shopify consent first, then sign-in, then claim
# --------------------------------------------------------------------------

def _no_store(response: HttpResponse) -> HttpResponse:
    response["Cache-Control"] = "no-store"
    response["Referrer-Policy"] = "no-referrer"
    return response


def _plain(message: str, status: int) -> HttpResponse:
    return _no_store(HttpResponse(message, status=status,
                                  content_type="text/plain; charset=utf-8"))


def _secure_cookies() -> bool:
    return not settings.DEBUG


def _claim_cookie_domain(request) -> Optional[str]:
    """The cookie domain that reaches both the callback and the org's host.

    In a multi-tenant deployment the callback runs on `MAIN_DOMAIN` and the claim
    on `<org>.MAIN_DOMAIN`, so the cookie is scoped to `MAIN_DOMAIN`. Anywhere
    else (standalone, local development) it stays on the current host.
    """
    main = (getattr(settings, "MAIN_DOMAIN", "") or "").strip().lower().lstrip(".")
    if settings.DEBUG or not main:
        return None
    host = request.get_host().split(":")[0].lower()
    return main if host == main or host.endswith("." + main) else None


def _claim_hash(secret: str) -> str:
    return hashlib.sha256(secret.encode()).hexdigest()


def _delete_expired_pending() -> None:
    from terno_dbi.core.models import ShopifyPendingInstall

    ShopifyPendingInstall.objects.filter(expires_at__lte=timezone.now()).delete()


def _connect_path(shop: str) -> str:
    """Where sign-in resumes: `/connect` in claim mode for this store."""
    params = {"connector": CONNECTOR_KEY, "shop": shop, "install": "1"}
    return_to = getattr(settings, "TERNO_SHOPIFY_INSTALL_RETURN_TO", "") or ""
    if return_to:
        params["return_to"] = return_to
    return f"/connect?{urlencode(params)}"


def _with_next(login_url: str, next_path: str) -> str:
    sep = "&" if "?" in login_url else "?"
    return f"{login_url}{sep}{urlencode({'next': next_path})}"


def _sign_in_target(request, next_path: str) -> str:
    login_url = getattr(settings, "TERNO_CONNECTOR_LOGIN_URL", "") or ""
    if login_url:
        # Hosted: the login service keeps `next` across sign-in and sign-up and
        # forwards it to the user's organisation host.
        return _with_next(login_url, next_path)
    if getattr(request, "user", None) is not None and request.user.is_authenticated:
        return next_path
    return _with_next(getattr(settings, "LOGIN_URL", "/accounts/login/"), next_path)


@require_GET
def shopify_install(request):
    """Shopify's app URL: verify the launch and send the merchant to consent."""
    from terno_dbi.catalog.availability import offered_connectors
    from terno_dbi.connectors.api.auth.oauth import (
        STATE_TTL_MINUTES, start_authorization)
    from terno_dbi.connectors.api.model.errors import ApiError
    from terno_dbi.connectors.api.web import CALLBACK_PATH, _callback_uri

    secret = _client_secret()
    if not secret:
        logger.error("Shopify install rejected: TERNO_SHOPIFY_CLIENT_SECRET is not set")
        return _plain("Shopify is not configured on this server.", 503)

    if not verify_query_hmac(request.GET, secret):
        logger.warning("Shopify install rejected: invalid hmac")
        return _plain("Invalid request signature.", 401)

    if not _timestamp_is_fresh(request.GET.get("timestamp")):
        logger.warning("Shopify install rejected: stale or missing timestamp")
        return _plain(
            "This link has expired. Open the app again from your Shopify admin.", 401)

    shop = request.GET.get("shop", "")
    if not is_valid_shop_domain(shop):
        logger.warning("Shopify install rejected: invalid shop domain")
        return _plain("Invalid shop domain.", 400)

    if not offered_connectors().filter(key=CONNECTOR_KEY).exists():
        logger.error("Shopify install for %s: the shopify connector is not offered", shop)
        return _plain("Shopify is not available on this server.", 503)

    _delete_expired_pending()
    try:
        # No organisation yet: the token is held until the merchant signs in.
        started = start_authorization(
            connector_key=CONNECTOR_KEY,
            redirect_uri=_callback_uri(request),
            organisation=None,
            instance=shop,
        )
    except ApiError as exc:
        logger.error("Shopify install for %s could not start OAuth: %s", shop, exc.message)
        return _plain("Shopify is not available on this server.", 503)

    logger.info("Shopify install started for %s", shop)
    response = _no_store(redirect(started["authorization_url"]))
    response.set_cookie(
        STATE_COOKIE, started["state"], max_age=STATE_TTL_MINUTES * 60,
        path=CALLBACK_PATH, secure=_secure_cookies(), httponly=True, samesite="Lax")
    return response


def is_install_state(st) -> bool:
    """Whether an OAuth state was started by `shopify_install` (no organisation)."""
    return (st is not None and st.connector_key == CONNECTOR_KEY
            and st.organisation_id is None and st.data_source_id is None)


def _warn_missing_scopes(shop: str, token_response: Mapping) -> None:
    """Log requested scopes Shopify did not grant (Shopify asks apps to check).

    Logged, not refused: Shopify may report a scope implied by another one
    differently, and a refused install cannot be recovered by the merchant.
    Queries that need a missing scope fail with Shopify's own error.
    """
    from terno_dbi.connectors.api.auth.providers import get_provider

    provider = get_provider(CONNECTOR_KEY)
    requested = {s.strip() for s in (provider.scope if provider else "").split(",")
                 if s.strip()}
    granted = {s.strip() for s in str(token_response.get("scope") or "").split(",")
               if s.strip()}
    missing = sorted(requested - granted)
    if missing:
        logger.warning("Shopify install for %s: scopes not granted: %s",
                       shop, ", ".join(missing))


def complete_install(request, st, code: str) -> HttpResponse:
    """The OAuth callback for an App Store install: hold the token, then sign in.

    Called by the shared callback view for a state `is_install_state` accepts.
    The state is consumed whatever the outcome.
    """
    from terno_dbi.connectors.api.auth.oauth import exchange_code_for_state
    from terno_dbi.connectors.api.model.errors import ApiError
    from terno_dbi.connectors.api.web import CALLBACK_PATH
    from terno_dbi.core.models import ShopifyPendingInstall

    shop = st.instance
    try:
        if st.is_expired:
            logger.warning("Shopify install callback for %s: state expired", shop)
            return _plain(EXPIRED_MESSAGE, 400)

        # Login CSRF: only the browser that started this install may finish it.
        cookie = request.COOKIES.get(STATE_COOKIE, "")
        if not cookie or not hmac.compare_digest(cookie.encode(), st.state.encode()):
            logger.warning("Shopify install callback for %s: state cookie missing "
                           "or different", shop)
            return _plain(EXPIRED_MESSAGE, 400)

        if not verify_query_hmac(request.GET, _client_secret()):
            logger.warning("Shopify install callback for %s: invalid hmac", shop)
            return _plain("Invalid request signature.", 401)

        if request.GET.get("shop", "").strip().lower() != shop:
            logger.warning("Shopify install callback: shop does not match %s", shop)
            return _plain("The store does not match this install.", 400)

        try:
            token_response, _ = exchange_code_for_state(
                st, code, callback_params=request.GET)
        except ApiError as exc:
            return _plain(exc.message, 400)
        if not isinstance(token_response, dict) or not token_response.get("access_token"):
            logger.warning("Shopify install callback for %s: no access token", shop)
            return _plain("Shopify did not return an access token. Try again.", 400)
        _warn_missing_scopes(shop, token_response)
    finally:
        st.delete()

    claim_secret = _secrets.token_urlsafe(32)
    ShopifyPendingInstall.objects.create(
        claim_hash=_claim_hash(claim_secret),
        shop=shop,
        token_response=token_response,
        expires_at=timezone.now() + timedelta(seconds=CLAIM_TTL_SECONDS),
    )
    logger.info("Shopify install authorized for %s; waiting for sign-in", shop)

    response = _no_store(redirect(_sign_in_target(request, _connect_path(shop))))
    response.delete_cookie(STATE_COOKIE, path=CALLBACK_PATH, samesite="Lax")
    response.set_cookie(
        CLAIM_COOKIE, claim_secret, max_age=CLAIM_TTL_SECONDS,
        path=CLAIM_COOKIE_PATH, domain=_claim_cookie_domain(request),
        secure=_secure_cookies(), httponly=True, samesite="Lax")
    return response


def claim_install(request, organisation, shop: str, return_to: str = "") -> HttpResponse:
    """Attach a store authorized by `complete_install` to `organisation`.

    Called by `/connect` once it has checked that the signed-in user is an admin
    of `organisation`. Needs the claim cookie set in this browser; the pending
    row is used once and deleted.
    """
    from terno_dbi.connectors.api.auth.oauth import _store_tokens, connector_datasource
    from terno_dbi.core.models import ShopifyPendingInstall
    from terno_dbi.services.secrets import decrypt_dict, encrypt_dict

    shop = (shop or "").strip().lower()
    if not is_valid_shop_domain(shop):
        return _plain("Invalid shop domain.", 400)

    claim_secret = request.COOKIES.get(CLAIM_COOKIE, "")
    if not claim_secret:
        logger.warning("Shopify claim for %s: no claim cookie", shop)
        return _plain(EXPIRED_MESSAGE, 400)

    with transaction.atomic():
        pending = (ShopifyPendingInstall.objects.select_for_update()
                   .filter(claim_hash=_claim_hash(claim_secret), shop=shop,
                           expires_at__gt=timezone.now())
                   .first())
        if pending is None:
            logger.warning("Shopify claim for %s: no matching pending install", shop)
            return _plain(EXPIRED_MESSAGE, 400)

        token_response = decrypt_dict(pending.token_response) or {}
        data_source = connector_datasource(organisation, CONNECTOR_KEY)
        existing = decrypt_dict(data_source.connection_json) or {}
        if str(existing.get("INSTANCE") or "").strip().lower() != shop:
            # Another store's tokens must not survive into this one's bundle.
            data_source.connection_json = encrypt_dict({})
        _store_tokens(data_source, token_response, instance=shop)
        pending.delete()

    _delete_expired_pending()
    logger.info("Shopify %s connected to organisation %s", shop, organisation.pk)

    target = return_to or "/"
    sep = "&" if "?" in target else "?"
    target += (f"{sep}connected={quote(data_source.display_name)}"
               f"&select_accounts={quote(CONNECTOR_KEY)}")
    response = _no_store(redirect(target))
    response.delete_cookie(CLAIM_COOKIE, path=CLAIM_COOKIE_PATH,
                           domain=_claim_cookie_domain(request), samesite="Lax")
    return response


# --------------------------------------------------------------------------
# Webhooks
# --------------------------------------------------------------------------

def datasources_for_shop(shop: str) -> list:
    """Every Shopify datasource, in any organisation, connected to `shop`.

    The store domain lives inside the encrypted token bundle, so each Shopify
    row is decrypted and compared. A row that cannot be decrypted is logged and
    skipped.
    """
    from django.db.models import Q
    from terno_dbi.core.models import DataSource
    from terno_dbi.services.secrets import decrypt_dict

    matches = []
    rows = DataSource.objects.filter(
        Q(catalog__key=CONNECTOR_KEY) | Q(type=CONNECTOR_KEY)).distinct()
    for ds in rows.iterator():
        try:
            bundle = decrypt_dict(ds.connection_json) or {}
        except Exception:   # noqa: BLE001 - one bad row must not stop the rest
            logger.exception("Cannot read the token bundle of datasource %s", ds.pk)
            continue
        if not isinstance(bundle, dict):
            continue
        stored = str(bundle.get("INSTANCE") or bundle.get("SHOP_DOMAIN") or "")
        if stored.strip().lower() == shop:
            matches.append(ds)
    return matches


def _on_app_uninstalled(shop: str, payload: Mapping) -> None:
    from terno_dbi.core.models import DataSource, ShopifyPendingInstall
    from terno_dbi.services.secrets import encrypt_dict

    rows = datasources_for_shop(shop)
    with transaction.atomic():
        # The token of an install nobody has claimed yet is revoked too.
        ShopifyPendingInstall.objects.filter(shop=shop).delete()
        for ds in rows:
            # Shopify has already revoked the token; drop it and everything else
            # in the bundle except the store domain, which shop/redact needs.
            ds.connection_json = encrypt_dict({"INSTANCE": shop})
            ds.auth_status = DataSource.AuthStatus.NOT_AUTHENTICATED
            ds.auth_error = UNINSTALLED_MESSAGE
            ds.save(update_fields=["connection_json", "auth_status", "auth_error"])
    logger.info("Shopify app/uninstalled for %s: cleared %d connection(s)",
                shop, len(rows))


def _on_shop_redact(shop: str, payload: Mapping) -> None:
    from terno_dbi.core.models import DataSource, ShopifyPendingInstall

    ids = [ds.pk for ds in datasources_for_shop(shop)]
    with transaction.atomic():
        ShopifyPendingInstall.objects.filter(shop=shop).delete()
        deleted, _ = DataSource.objects.filter(pk__in=ids).delete()
    logger.info("Shopify shop/redact for %s: deleted %d connection(s) (%d rows in all)",
                shop, len(ids), deleted)


def _on_customers_redact(shop: str, payload: Mapping) -> None:
    from terno_dbi.core.models import ApiQueryJob

    ids = [ds.pk for ds in datasources_for_shop(shop)]
    # Stored query results may contain the customer. They are not indexed by
    # customer, so every finished result for the store is deleted. Jobs still
    # running are left alone: deleting one would let its final save recreate it.
    with transaction.atomic():
        deleted, _ = ApiQueryJob.objects.filter(
            data_source_id__in=ids,
            status__in=[ApiQueryJob.Status.COMPLETED, ApiQueryJob.Status.FAILED],
        ).delete()
    customer = payload.get("customer") if isinstance(payload.get("customer"), dict) else {}
    logger.info("Shopify customers/redact for %s (customer id %s): deleted %d "
                "stored query result(s)", shop, customer.get("id"), deleted)


def _on_customers_data_request(shop: str, payload: Mapping) -> None:
    customer = payload.get("customer") if isinstance(payload.get("customer"), dict) else {}
    data_request = (payload.get("data_request")
                    if isinstance(payload.get("data_request"), dict) else {})
    # No per-customer profile is kept, so there is nothing to export
    # automatically. Logged (ids only, no contact details) for manual follow-up.
    logger.warning("Shopify customers/data_request for %s: customer id %s, request "
                   "id %s. Follow up with the merchant manually.",
                   shop, customer.get("id"), data_request.get("id"))


_HANDLERS: Dict[str, Callable[[str, Mapping], None]] = {
    "app/uninstalled": _on_app_uninstalled,
    "shop/redact": _on_shop_redact,
    "customers/redact": _on_customers_redact,
    "customers/data_request": _on_customers_data_request,
}


def _payload_shop(topic: str, payload: Mapping) -> str:
    """The store named inside the signed body."""
    key = "myshopify_domain" if topic == "app/uninstalled" else "shop_domain"
    value = payload.get(key)
    return value.strip().lower() if isinstance(value, str) else ""


@csrf_exempt
@require_POST
def shopify_webhook(request):
    """Receive every Shopify webhook topic this app subscribes to."""
    secret = _client_secret()
    if not secret:
        logger.error("Shopify webhook rejected: TERNO_SHOPIFY_CLIENT_SECRET is not set")
        return HttpResponse(status=401)

    body = request.body
    if not verify_webhook_hmac(body, request.headers.get("X-Shopify-Hmac-Sha256", ""),
                               secret):
        logger.warning("Shopify webhook rejected: invalid hmac")
        return HttpResponse(status=401)

    topic = request.headers.get("X-Shopify-Topic", "").strip().lower()
    handler = _HANDLERS.get(topic)
    if handler is None:
        # Signed but not one we act on: acknowledge so Shopify does not retry.
        logger.info("Shopify webhook ignored: topic %r", topic[:100])
        return HttpResponse(status=200)

    shop = request.headers.get("X-Shopify-Shop-Domain", "").strip().lower()
    if not is_valid_shop_domain(shop):
        logger.warning("Shopify %s webhook rejected: invalid shop header", topic)
        return HttpResponse(status=400)

    try:
        payload = json.loads(body)
    except (ValueError, UnicodeDecodeError):
        logger.warning("Shopify %s webhook rejected: body is not JSON", topic)
        return HttpResponse(status=400)
    if not isinstance(payload, dict):
        logger.warning("Shopify %s webhook rejected: body is not an object", topic)
        return HttpResponse(status=400)

    if _payload_shop(topic, payload) != shop:
        logger.warning("Shopify %s webhook rejected: body shop does not match header %s",
                       topic, shop)
        return HttpResponse(status=400)

    try:
        handler(shop, payload)
    except Exception:   # noqa: BLE001 - a 5xx makes Shopify retry the delivery
        logger.exception("Shopify %s webhook for %s failed", topic, shop)
        return HttpResponse(status=500)
    return HttpResponse(status=200)


__all__ = [
    "claim_install",
    "complete_install",
    "datasources_for_shop",
    "is_install_state",
    "is_valid_shop_domain",
    "query_signature_message",
    "shopify_install",
    "shopify_webhook",
    "verify_query_hmac",
    "verify_webhook_hmac",
]
