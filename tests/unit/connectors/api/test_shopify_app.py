"""Shopify App Store integration: install, claim and webhooks.

Covers query and webhook HMAC verification (valid, invalid, tampered); the
OAuth-first install (consent before sign-in, browser binding, callback checks,
holding the token, claiming it into an organisation); and every webhook topic,
end to end through the URL routes.
"""

import base64
import hashlib
import hmac
import json
import time
from urllib.parse import parse_qs, urlencode, urlsplit

import pytest
from django.contrib.auth.models import AnonymousUser
from django.http import QueryDict
from django.test import RequestFactory

from terno_dbi.connectors.api import shopify_app

SECRET = "shpss_test_secret"
SHOP = "acme-store.myshopify.com"
INSTALL_PATH = "/connectors/shopify/install/"
WEBHOOK_PATH = "/connectors/shopify/webhooks/"


@pytest.fixture(autouse=True)
def shopify_secret(monkeypatch):
    monkeypatch.setenv("TERNO_SHOPIFY_CLIENT_ID", "client-id")
    monkeypatch.setenv("TERNO_SHOPIFY_CLIENT_SECRET", SECRET)


def _sign_query(params, secret=SECRET):
    """Reference signer, written independently of the code under test."""
    message = "&".join(f"{k}={v}" for k, v in sorted(params.items()))
    return hmac.new(secret.encode(), message.encode(), hashlib.sha256).hexdigest()


def _install_params(shop=SHOP, timestamp=None, secret=SECRET, **extra):
    params = {"shop": shop, "timestamp": str(int(timestamp or time.time())),
              "host": "YWRtaW4uc2hvcGlmeS5jb20vc3RvcmUvYWNtZQ", **extra}
    params["hmac"] = _sign_query(params, secret)
    return params


def _sign_body(body: bytes, secret=SECRET) -> str:
    return base64.b64encode(
        hmac.new(secret.encode(), body, hashlib.sha256).digest()).decode()


# --------------------------------------------------------------------------
# Query HMAC
# --------------------------------------------------------------------------

class TestQueryHmac:
    def test_shopify_documented_example(self):
        # The worked example from Shopify's OAuth documentation.
        params = QueryDict(
            "code=0907a61c0c8d55e99db179b68161bc00"
            "&hmac=700e2dadb827fcc8609e9d5ce208b2e9cdaab9df07390d2cbca10d7c328fc4bf"
            "&shop=some-shop.myshopify.com&state=0.6784241404160823"
            "&timestamp=1337178173")
        assert shopify_app.verify_query_hmac(params, "hush")

    def test_valid(self):
        assert shopify_app.verify_query_hmac(
            QueryDict(urlencode(_install_params())), SECRET)

    def test_uppercase_hex_accepted(self):
        params = _install_params()
        params["hmac"] = params["hmac"].upper()
        assert shopify_app.verify_query_hmac(QueryDict(urlencode(params)), SECRET)

    def test_wrong_secret(self):
        params = _install_params(secret="someone-else")
        assert not shopify_app.verify_query_hmac(QueryDict(urlencode(params)), SECRET)

    def test_tampered_shop(self):
        params = _install_params()
        params["shop"] = "evil.myshopify.com"
        assert not shopify_app.verify_query_hmac(QueryDict(urlencode(params)), SECRET)

    def test_added_parameter(self):
        params = _install_params()
        params["extra"] = "1"
        assert not shopify_app.verify_query_hmac(QueryDict(urlencode(params)), SECRET)

    def test_missing_hmac(self):
        params = _install_params()
        del params["hmac"]
        assert not shopify_app.verify_query_hmac(QueryDict(urlencode(params)), SECRET)

    def test_empty_hmac(self):
        params = _install_params()
        params["hmac"] = ""
        assert not shopify_app.verify_query_hmac(QueryDict(urlencode(params)), SECRET)

    def test_repeated_hmac_rejected(self):
        params = _install_params()
        query = urlencode(params) + "&hmac=" + params["hmac"]
        assert not shopify_app.verify_query_hmac(QueryDict(query), SECRET)

    def test_repeated_plain_parameter_rejected(self):
        params = _install_params()
        query = urlencode(params) + "&shop=evil.myshopify.com"
        assert not shopify_app.verify_query_hmac(QueryDict(query), SECRET)

    def test_no_secret_never_verifies(self):
        params = _install_params(secret="")
        assert not shopify_app.verify_query_hmac(QueryDict(urlencode(params)), "")

    def test_non_ascii_hmac_does_not_raise(self):
        params = _install_params()
        params["hmac"] = "é" * 64
        assert not shopify_app.verify_query_hmac(QueryDict(urlencode(params)), SECRET)

    def test_delimiters_are_escaped(self):
        # 'a=1&b=2' as one value must not sign the same as two parameters.
        one = QueryDict(mutable=True)
        one["a"] = "1&b=2"
        two = QueryDict("a=1&b=2")
        assert (shopify_app.query_signature_message(one)
                != shopify_app.query_signature_message(two))
        assert shopify_app.query_signature_message(one) == "a=1%26b=2"

    def test_percent_and_equals_escaped(self):
        params = QueryDict(mutable=True)
        params["k=x"] = "50%"
        assert shopify_app.query_signature_message(params) == "k%3Dx=50%25"

    def test_array_parameter(self):
        params = QueryDict("ids[]=1&ids[]=2&shop=a.myshopify.com")
        assert (shopify_app.query_signature_message(params)
                == 'ids=["1", "2"]&shop=a.myshopify.com')


# --------------------------------------------------------------------------
# Webhook HMAC
# --------------------------------------------------------------------------

class TestWebhookHmac:
    BODY = b'{"shop_id": 1, "shop_domain": "acme-store.myshopify.com"}'

    def test_valid(self):
        assert shopify_app.verify_webhook_hmac(self.BODY, _sign_body(self.BODY), SECRET)

    def test_wrong_secret(self):
        assert not shopify_app.verify_webhook_hmac(
            self.BODY, _sign_body(self.BODY, "other"), SECRET)

    def test_tampered_body(self):
        header = _sign_body(self.BODY)
        assert not shopify_app.verify_webhook_hmac(self.BODY + b" ", header, SECRET)

    def test_missing_header(self):
        assert not shopify_app.verify_webhook_hmac(self.BODY, "", SECRET)

    def test_no_secret(self):
        assert not shopify_app.verify_webhook_hmac(self.BODY, _sign_body(self.BODY, ""), "")

    def test_hex_digest_is_not_accepted(self):
        hex_digest = hmac.new(SECRET.encode(), self.BODY, hashlib.sha256).hexdigest()
        assert not shopify_app.verify_webhook_hmac(self.BODY, hex_digest, SECRET)

    def test_non_ascii_header_does_not_raise(self):
        assert not shopify_app.verify_webhook_hmac(self.BODY, "ü" * 44, SECRET)


# --------------------------------------------------------------------------
# Shop domain validation
# --------------------------------------------------------------------------

@pytest.mark.parametrize("shop", [
    "acme.myshopify.com", "a.myshopify.com", "acme-store-2.myshopify.com",
])
def test_valid_shop_domains(shop):
    assert shopify_app.is_valid_shop_domain(shop)


@pytest.mark.parametrize("shop", [
    "", "acme", "acme.example.com", "https://acme.myshopify.com",
    "acme.myshopify.com/admin", "ACME.myshopify.com", "-acme.myshopify.com",
    "acme.myshopify.com.evil.com", "evil.com/.myshopify.com", "acme..myshopify.com",
    "acme.myshopify.com\n", "a" * 61 + ".myshopify.com", None, 123,
])
def test_invalid_shop_domains(shop):
    assert not shopify_app.is_valid_shop_domain(shop)


# --------------------------------------------------------------------------
# Install: consent first, then sign-in, then claim
# --------------------------------------------------------------------------

CALLBACK_PATH = "/connectors/oauth/callback/"


@pytest.fixture
def shopify_catalog(db):
    from terno_dbi.core.models import ConnectorCatalog
    cat, _ = ConnectorCatalog.objects.get_or_create(
        key="shopify",
        defaults=dict(display_name="Shopify", family="api", auth_type="oauth"),
    )
    if not cat.enabled:
        cat.enabled = True
        cat.save(update_fields=["enabled"])
    return cat


@pytest.fixture
def token_exchange(monkeypatch):
    """Stub Shopify's token endpoint; records each call."""
    calls = []

    def fake_post(url, data, auth=None, as_json=False):
        calls.append({"url": url, "data": dict(data)})
        return {"access_token": "shpat_new", "refresh_token": "shprt_new",
                "scope": "read_orders", "expires_in": 3600}

    monkeypatch.setattr("terno_dbi.connectors.api.auth.oauth._default_post", fake_post)
    return calls


def _install(params, user=None):
    request = RequestFactory().get(INSTALL_PATH, params)
    request.user = user or AnonymousUser()
    return shopify_app.shopify_install(request)


def _query(url):
    return {k: v[0] for k, v in parse_qs(urlsplit(url).query).items()}


def _next_of(url):
    return _query(url)["next"]


def _connect_params(path):
    parts = urlsplit(path)
    assert parts.path == "/connect"
    return {k: v[0] for k, v in parse_qs(parts.query).items()}


def _callback_params(state, shop=SHOP, code="auth-code", secret=SECRET):
    params = {"code": code, "shop": shop, "state": state,
              "timestamp": str(int(time.time())),
              "host": "YWRtaW4uc2hvcGlmeS5jb20vc3RvcmUvYWNtZQ"}
    params["hmac"] = _sign_query(params, secret)
    return params


def _start(client):
    """Run the install step; return the OAuth state it issued."""
    resp = client.get(INSTALL_PATH, _install_params())
    assert resp.status_code == 302
    return _query(resp.url)["state"]


def _authorize(client, token_exchange, **overrides):
    """Install, then the Shopify callback, through the URL routes."""
    state = _start(client)
    return client.get(CALLBACK_PATH, _callback_params(state, **overrides)), state


@pytest.mark.django_db
class TestInstall:
    def test_redirects_straight_to_shopify_consent(self, shopify_catalog):
        resp = _install(_install_params())
        assert resp.status_code == 302
        parts = urlsplit(resp.url)
        assert (parts.scheme, parts.netloc, parts.path) == (
            "https", SHOP, "/admin/oauth/authorize")
        query = _query(resp.url)
        assert query["client_id"] == "client-id"
        assert query["redirect_uri"].endswith(CALLBACK_PATH)
        assert "read_orders" in query["scope"]
        assert resp["Cache-Control"] == "no-store"
        assert resp["Referrer-Policy"] == "no-referrer"

    def test_signed_in_user_also_goes_to_consent_first(
            self, shopify_catalog, django_user_model):
        user = django_user_model.objects.create_user("merchant", "m@example.com", "pw")
        resp = _install(_install_params(), user=user)
        assert urlsplit(resp.url).netloc == SHOP

    def test_state_has_no_organisation_and_is_bound_to_browser(self, shopify_catalog):
        from terno_dbi.core.models import ConnectorOAuthState
        resp = _install(_install_params())
        state = _query(resp.url)["state"]
        st = ConnectorOAuthState.objects.get(state=state)
        assert (st.organisation_id, st.data_source_id, st.instance) == (None, None, SHOP)
        cookie = resp.cookies[shopify_app.STATE_COOKIE]
        assert cookie.value == state
        assert cookie["path"] == CALLBACK_PATH
        assert cookie["httponly"] and cookie["samesite"] == "Lax"

    def test_invalid_hmac_is_401(self, shopify_catalog):
        params = _install_params()
        params["hmac"] = "0" * 64
        assert _install(params).status_code == 401

    def test_tampered_shop_is_401(self, shopify_catalog):
        params = _install_params()
        params["shop"] = "victim.myshopify.com"
        assert _install(params).status_code == 401

    def test_missing_hmac_is_401(self, shopify_catalog):
        params = _install_params()
        del params["hmac"]
        assert _install(params).status_code == 401

    def test_rejected_install_creates_no_state(self, shopify_catalog):
        from terno_dbi.core.models import ConnectorOAuthState
        params = _install_params()
        params["hmac"] = "0" * 64
        _install(params)
        assert not ConnectorOAuthState.objects.exists()

    def test_stale_timestamp_is_401(self, shopify_catalog):
        old = time.time() - shopify_app.INSTALL_LINK_MAX_AGE_SECONDS - 60
        assert _install(_install_params(timestamp=old)).status_code == 401

    def test_future_timestamp_is_401(self, shopify_catalog):
        ahead = time.time() + shopify_app.INSTALL_LINK_MAX_FUTURE_SKEW_SECONDS + 60
        assert _install(_install_params(timestamp=ahead)).status_code == 401

    def test_small_clock_skew_accepted(self, shopify_catalog):
        assert _install(_install_params(timestamp=time.time() + 30)).status_code == 302

    def test_non_numeric_timestamp_is_401(self, shopify_catalog):
        params = {"shop": SHOP, "timestamp": "soon"}
        params["hmac"] = _sign_query(params)
        assert _install(params).status_code == 401

    def test_signed_but_invalid_shop_is_400(self, shopify_catalog):
        assert _install(_install_params(shop="acme.example.com")).status_code == 400

    def test_signed_shop_with_path_is_400(self, shopify_catalog):
        assert _install(_install_params(shop="acme.myshopify.com/x")).status_code == 400

    def test_no_secret_is_503(self, shopify_catalog, monkeypatch):
        params = _install_params()
        monkeypatch.delenv("TERNO_SHOPIFY_CLIENT_SECRET")
        assert _install(params).status_code == 503

    def test_no_client_id_is_503(self, shopify_catalog, monkeypatch):
        monkeypatch.delenv("TERNO_SHOPIFY_CLIENT_ID")
        assert _install(_install_params()).status_code == 503

    def test_connector_not_offered_is_503(self, shopify_catalog):
        shopify_catalog.enabled = False
        shopify_catalog.save(update_fields=["enabled"])
        assert _install(_install_params()).status_code == 503

    def test_post_not_allowed(self, client, shopify_catalog):
        assert client.post(INSTALL_PATH, _install_params()).status_code == 405

    def test_expired_pending_installs_are_swept(self, shopify_catalog):
        from datetime import timedelta
        from django.utils import timezone
        from terno_dbi.core.models import ShopifyPendingInstall
        ShopifyPendingInstall.objects.create(
            claim_hash="x" * 64, shop=SHOP, token_response={"access_token": "t"},
            expires_at=timezone.now() - timedelta(seconds=1))
        _install(_install_params())
        assert not ShopifyPendingInstall.objects.exists()


@pytest.mark.django_db
class TestInstallCallback:
    def test_holds_token_and_sends_to_sign_in(self, client, shopify_catalog,
                                              token_exchange, settings):
        from terno_dbi.core.models import (
            ConnectorOAuthState, DataSource, ShopifyPendingInstall)
        from terno_dbi.services.secrets import decrypt_dict, is_encrypted
        settings.TERNO_CONNECTOR_LOGIN_URL = "https://app.terno.ai/accounts/login/"
        settings.TERNO_SHOPIFY_INSTALL_RETURN_TO = ""

        resp, _ = _authorize(client, token_exchange)

        assert resp.status_code == 302
        assert resp.url.startswith("https://app.terno.ai/accounts/login/?next=")
        assert _connect_params(_next_of(resp.url)) == {
            "connector": "shopify", "shop": SHOP, "install": "1"}
        assert resp["Cache-Control"] == "no-store"
        # The token exchange went to the store, with the code and secret.
        assert token_exchange[0]["url"] == f"https://{SHOP}/admin/oauth/access_token"
        assert token_exchange[0]["data"]["code"] == "auth-code"
        # The token is held, encrypted, and nothing is attached to any org yet.
        pending = ShopifyPendingInstall.objects.get()
        assert pending.shop == SHOP
        assert is_encrypted(pending.token_response)
        assert decrypt_dict(pending.token_response)["access_token"] == "shpat_new"
        assert not DataSource.objects.exists()
        assert not ConnectorOAuthState.objects.exists()
        # The browser holds the claim secret; only its hash is stored.
        claim = resp.cookies[shopify_app.CLAIM_COOKIE]
        assert claim.value and claim.value not in pending.claim_hash
        assert pending.claim_hash == hashlib.sha256(claim.value.encode()).hexdigest()
        assert claim["path"] == "/connect" and claim["httponly"]
        assert claim["samesite"] == "Lax"
        assert resp.cookies[shopify_app.STATE_COOKIE]["max-age"] == 0

    def test_return_to_setting_is_carried(self, client, shopify_catalog,
                                          token_exchange, settings):
        settings.TERNO_CONNECTOR_LOGIN_URL = "https://app.terno.ai/accounts/login/"
        settings.TERNO_SHOPIFY_INSTALL_RETURN_TO = "/chat"
        resp, _ = _authorize(client, token_exchange)
        assert _connect_params(_next_of(resp.url))["return_to"] == "/chat"

    def test_standalone_signed_in_goes_to_connect(self, client, shopify_catalog,
                                                  token_exchange, settings,
                                                  django_user_model):
        settings.TERNO_CONNECTOR_LOGIN_URL = ""
        settings.TERNO_SHOPIFY_INSTALL_RETURN_TO = ""
        user = django_user_model.objects.create_user("merchant", "m@example.com", "pw")
        client.force_login(user)
        resp, _ = _authorize(client, token_exchange)
        assert _connect_params(resp.url)["install"] == "1"

    def test_standalone_anonymous_goes_to_login(self, client, shopify_catalog,
                                                token_exchange, settings):
        settings.TERNO_CONNECTOR_LOGIN_URL = ""
        settings.LOGIN_URL = "/accounts/login/"
        resp, _ = _authorize(client, token_exchange)
        assert resp.url.startswith("/accounts/login/?next=")
        assert _connect_params(_next_of(resp.url))["shop"] == SHOP

    def test_other_browser_cannot_finish(self, client, shopify_catalog, token_exchange):
        from django.test import Client
        from terno_dbi.core.models import ConnectorOAuthState, ShopifyPendingInstall
        state = _start(client)
        resp = Client().get(CALLBACK_PATH, _callback_params(state))
        assert resp.status_code == 400
        assert not token_exchange
        assert not ShopifyPendingInstall.objects.exists()
        assert not ConnectorOAuthState.objects.exists()   # consumed

    def test_wrong_state_cookie_cannot_finish(self, client, shopify_catalog,
                                              token_exchange):
        state = _start(client)
        client.cookies[shopify_app.STATE_COOKIE] = "someone-elses-state"
        client.cookies[shopify_app.STATE_COOKIE]["path"] = CALLBACK_PATH
        assert client.get(CALLBACK_PATH, _callback_params(state)).status_code == 400
        assert not token_exchange

    def test_invalid_callback_hmac_is_401(self, client, shopify_catalog, token_exchange):
        from terno_dbi.core.models import ShopifyPendingInstall
        resp, _ = _authorize(client, token_exchange, secret="someone-else")
        assert resp.status_code == 401
        assert not token_exchange
        assert not ShopifyPendingInstall.objects.exists()

    def test_callback_for_another_shop_is_400(self, client, shopify_catalog,
                                              token_exchange):
        resp, _ = _authorize(client, token_exchange, shop="other-store.myshopify.com")
        assert resp.status_code == 400
        assert not token_exchange

    def test_expired_state_is_400(self, client, shopify_catalog, token_exchange):
        from datetime import timedelta
        from django.utils import timezone
        from terno_dbi.core.models import ConnectorOAuthState
        state = _start(client)
        ConnectorOAuthState.objects.filter(state=state).update(
            expires_at=timezone.now() - timedelta(seconds=1))
        assert client.get(CALLBACK_PATH, _callback_params(state)).status_code == 400
        assert not token_exchange

    def test_state_is_single_use(self, client, shopify_catalog, token_exchange):
        resp, state = _authorize(client, token_exchange)
        assert resp.status_code == 302
        client.cookies[shopify_app.STATE_COOKIE] = state
        client.cookies[shopify_app.STATE_COOKIE]["path"] = CALLBACK_PATH
        assert client.get(CALLBACK_PATH, _callback_params(state)).status_code == 400
        assert len(token_exchange) == 1

    def test_failed_token_exchange_is_400(self, client, shopify_catalog, monkeypatch):
        from terno_dbi.core.models import ConnectorOAuthState, ShopifyPendingInstall

        def failing_post(*a, **kw):
            raise RuntimeError("shopify down")
        monkeypatch.setattr("terno_dbi.connectors.api.auth.oauth._default_post",
                            failing_post)
        state = _start(client)
        assert client.get(CALLBACK_PATH, _callback_params(state)).status_code == 400
        assert not ShopifyPendingInstall.objects.exists()
        assert not ConnectorOAuthState.objects.exists()

    def test_response_without_token_is_400(self, client, shopify_catalog, monkeypatch):
        from terno_dbi.core.models import ShopifyPendingInstall
        monkeypatch.setattr("terno_dbi.connectors.api.auth.oauth._default_post",
                            lambda *a, **kw: {"scope": "read_orders"})
        state = _start(client)
        assert client.get(CALLBACK_PATH, _callback_params(state)).status_code == 400
        assert not ShopifyPendingInstall.objects.exists()

    def test_declined_consent_stores_nothing(self, client, shopify_catalog):
        from terno_dbi.core.models import ShopifyPendingInstall
        _start(client)
        resp = client.get(CALLBACK_PATH, {"error": "access_denied"})
        assert resp.status_code == 400
        assert not ShopifyPendingInstall.objects.exists()

    def test_claim_cookie_domain_on_multitenant_host(self, settings):
        settings.DEBUG = False
        settings.MAIN_DOMAIN = "app.terno.ai"
        settings.ALLOWED_HOSTS = ["*"]
        rf = RequestFactory()
        assert shopify_app._claim_cookie_domain(
            rf.get("/", HTTP_HOST="app.terno.ai")) == "app.terno.ai"
        assert shopify_app._claim_cookie_domain(
            rf.get("/", HTTP_HOST="acme.app.terno.ai")) == "app.terno.ai"
        assert shopify_app._claim_cookie_domain(
            rf.get("/", HTTP_HOST="evilapp.terno.ai")) is None
        settings.DEBUG = True
        assert shopify_app._claim_cookie_domain(
            rf.get("/", HTTP_HOST="app.terno.ai")) is None

    def test_org_less_state_never_completes_through_generic_path(self, shopify_catalog):
        # Defence in depth: the generic completion refuses an org-less state, so
        # no datasource without an organisation can ever be created.
        from datetime import timedelta
        from django.utils import timezone
        from terno_dbi.connectors.api.auth.oauth import complete_authorization
        from terno_dbi.connectors.api.model.errors import ApiError
        from terno_dbi.core.models import ConnectorOAuthState, DataSource
        ConnectorOAuthState.objects.create(
            state="orgless", connector_key="google_ads", redirect_uri="https://x/cb",
            expires_at=timezone.now() + timedelta(minutes=5))
        with pytest.raises(ApiError):
            complete_authorization(state="orgless", code="c",
                                   http_post=lambda *a, **kw: {"access_token": "t"})
        assert not DataSource.objects.exists()


def _claim(client, org_id, shop=SHOP, return_to=None):
    from terno_dbi.connectors.api import web
    params = {"connector": "shopify", "shop": shop, "install": "1"}
    if return_to is not None:
        params["return_to"] = return_to
    request = RequestFactory().get("/connect", params)
    request.COOKIES = {k: m.value for k, m in client.cookies.items()}
    request.user = client._user
    request.org_id = org_id
    return web.connect(request)


@pytest.fixture
def authorized(client, shopify_catalog, token_exchange, settings):
    """A browser that has installed and approved, now holding the claim cookie."""
    settings.TERNO_CONNECTOR_LOGIN_URL = "https://app.terno.ai/accounts/login/"
    resp, _ = _authorize(client, token_exchange)
    assert resp.status_code == 302
    return client


@pytest.mark.django_db
class TestClaim:
    def test_attaches_store_to_admins_org(self, authorized, org):
        from terno_dbi.core.models import DataSource, ShopifyPendingInstall
        authorized._user = org.owner
        resp = _claim(authorized, org.id)
        assert resp.status_code == 302
        assert urlsplit(resp.url).path == "/"
        assert _query(resp.url)["select_accounts"] == "shopify"
        assert resp["Cache-Control"] == "no-store"
        ds = DataSource.objects.get(organisation=org)
        assert ds.auth_status == DataSource.AuthStatus.CONNECTED
        bundle = _bundle(ds)
        assert bundle["ACCESS_TOKEN"] == "shpat_new"
        assert bundle["REFRESH_TOKEN"] == "shprt_new"
        assert bundle["INSTANCE"] == SHOP
        assert not ShopifyPendingInstall.objects.exists()
        assert resp.cookies[shopify_app.CLAIM_COOKIE]["max-age"] == 0

    def test_return_to_is_honoured(self, authorized, org):
        authorized._user = org.owner
        resp = _claim(authorized, org.id, return_to="/chat")
        assert urlsplit(resp.url).path == "/chat"

    def test_offsite_return_to_is_ignored(self, authorized, org):
        authorized._user = org.owner
        resp = _claim(authorized, org.id, return_to="https://evil.example/")
        assert urlsplit(resp.url).netloc == ""

    def test_reconnect_updates_existing_datasource(self, authorized, org, connected):
        from terno_dbi.core.models import DataSource
        authorized._user = org.owner
        _claim(authorized, org.id)
        assert DataSource.objects.filter(organisation=org).count() == 1
        assert _bundle(connected)["ACCESS_TOKEN"] == "shpat_new"

    def test_switching_store_drops_old_store_tokens(self, authorized, org, shopify_catalog,
                                                    monkeypatch):
        from terno_dbi.core.models import DataSource
        old = _connection(org, shopify_catalog, "other-store.myshopify.com", "Shopify")
        monkeypatch.setattr("terno_dbi.connectors.api.auth.oauth._default_post",
                            lambda *a, **kw: {"access_token": "shpat_only"})
        authorized._user = org.owner
        # Re-run with a token response that has no refresh token.
        authorized.cookies.clear()
        state = _start(authorized)
        authorized.get(CALLBACK_PATH, _callback_params(state))
        _claim(authorized, org.id)
        bundle = _bundle(old)
        assert bundle["INSTANCE"] == SHOP
        assert bundle["ACCESS_TOKEN"] == "shpat_only"
        assert "REFRESH_TOKEN" not in bundle
        assert DataSource.objects.filter(organisation=org).count() == 1

    def test_claim_is_single_use(self, authorized, org):
        authorized._user = org.owner
        assert _claim(authorized, org.id).status_code == 302
        assert _claim(authorized, org.id).status_code == 400

    def test_non_admin_cannot_claim(self, authorized, org):
        from django.contrib.auth.models import User
        from terno_dbi.core.models import OrganisationUser, ShopifyPendingInstall
        member = User.objects.create_user("member", "mem@example.com", "pw")
        OrganisationUser.objects.create(user=member, organisation=org)
        authorized._user = member
        assert _claim(authorized, org.id).status_code == 403
        assert ShopifyPendingInstall.objects.exists()

    def test_non_member_cannot_claim(self, authorized, org):
        from django.contrib.auth.models import User
        outsider = User.objects.create_user("outsider", "out@example.com", "pw")
        authorized._user = outsider
        assert _claim(authorized, org.id).status_code == 403

    def test_anonymous_is_sent_to_login(self, authorized, org):
        authorized._user = AnonymousUser()
        resp = _claim(authorized, org.id)
        assert resp.status_code == 302 and "login" in resp.url

    def test_without_cookie_is_400(self, authorized, org):
        from terno_dbi.core.models import DataSource
        authorized._user = org.owner
        authorized.cookies.clear()
        assert _claim(authorized, org.id).status_code == 400
        assert not DataSource.objects.exists()

    def test_forged_cookie_is_400(self, authorized, org):
        authorized._user = org.owner
        authorized.cookies[shopify_app.CLAIM_COOKIE] = "guess"
        assert _claim(authorized, org.id).status_code == 400

    def test_cookie_for_another_shop_is_400(self, authorized, org):
        authorized._user = org.owner
        assert _claim(authorized, org.id,
                      shop="other-store.myshopify.com").status_code == 400

    def test_invalid_shop_is_400(self, authorized, org):
        authorized._user = org.owner
        assert _claim(authorized, org.id, shop="evil.example.com").status_code == 400

    def test_expired_claim_is_400(self, authorized, org):
        from datetime import timedelta
        from django.utils import timezone
        from terno_dbi.core.models import DataSource, ShopifyPendingInstall
        ShopifyPendingInstall.objects.update(
            expires_at=timezone.now() - timedelta(seconds=1))
        authorized._user = org.owner
        assert _claim(authorized, org.id).status_code == 400
        assert not DataSource.objects.exists()

    def test_uninstall_before_claim_revokes_it(self, authorized, org):
        from terno_dbi.core.models import ShopifyPendingInstall
        from django.test import Client
        assert _post(Client(), "app/uninstalled", _uninstalled_payload()).status_code == 200
        assert not ShopifyPendingInstall.objects.exists()
        authorized._user = org.owner
        assert _claim(authorized, org.id).status_code == 400

    def test_shop_redact_removes_unclaimed(self, authorized):
        from django.test import Client
        from terno_dbi.core.models import ShopifyPendingInstall
        assert _post(Client(), "shop/redact", _shop_redact_payload()).status_code == 200
        assert not ShopifyPendingInstall.objects.exists()

    def test_plain_connect_is_unchanged(self, org, shopify_catalog, monkeypatch):
        # Without install=1, /connect for Shopify still starts its own OAuth.
        from terno_dbi.connectors.api import web
        monkeypatch.setattr(
            "terno_dbi.connectors.api.auth.oauth.start_authorization",
            lambda **kw: {"authorization_url": "https://provider/auth", "state": "s"})
        request = RequestFactory().get("/connect", {"connector": "shopify", "shop": SHOP})
        request.user = org.owner
        request.org_id = org.id
        assert web.connect(request).url == "https://provider/auth"


# --------------------------------------------------------------------------
# Webhooks
# --------------------------------------------------------------------------

@pytest.fixture
def org(db):
    from django.contrib.auth.models import User
    from terno_dbi.core.models import CoreOrganisation, OrganisationUser

    owner = User.objects.create_user("shop-owner", "o@example.com", "pw")
    org = CoreOrganisation.objects.create(name="Acme", subdomain="shop-acme", owner=owner)
    OrganisationUser.objects.create(user=owner, organisation=org)
    return org


def _connection(org, catalog, shop, name):
    from terno_dbi.core.models import DataSource
    from terno_dbi.services.secrets import encrypt_dict

    return DataSource.objects.create(
        display_name=name, type="shopify", connection_str="", organisation=org,
        catalog=catalog, auth_status=DataSource.AuthStatus.CONNECTED,
        connection_json=encrypt_dict({
            "ACCESS_TOKEN": "shpat_live", "REFRESH_TOKEN": "shprt_live",
            "INSTANCE": shop, "TOKEN_EXPIRES_AT": "9999999999",
        }),
    )


@pytest.fixture
def connected(org, shopify_catalog):
    return _connection(org, shopify_catalog, SHOP, "Shopify")


@pytest.fixture
def other_store(org, shopify_catalog):
    return _connection(org, shopify_catalog, "other-store.myshopify.com", "Shopify 2")


def _bundle(ds):
    from terno_dbi.services.secrets import decrypt_dict
    ds.refresh_from_db()
    return decrypt_dict(ds.connection_json)


def _post(client, topic, payload, shop=SHOP, secret=SECRET, body=None, hmac_header=None):
    body = body if body is not None else json.dumps(payload).encode()
    headers = {
        "HTTP_X_SHOPIFY_TOPIC": topic,
        "HTTP_X_SHOPIFY_SHOP_DOMAIN": shop,
        "HTTP_X_SHOPIFY_HMAC_SHA256": (hmac_header if hmac_header is not None
                                       else _sign_body(body, secret)),
    }
    return client.post(WEBHOOK_PATH, data=body, content_type="application/json", **headers)


def _uninstalled_payload(shop=SHOP):
    return {"id": 1, "name": "Acme", "domain": "acme.example.com",
            "myshopify_domain": shop}


def _shop_redact_payload(shop=SHOP):
    return {"shop_id": 1, "shop_domain": shop}


def _customers_payload(shop=SHOP):
    return {"shop_id": 1, "shop_domain": shop, "orders_requested": [11],
            "customer": {"id": 42, "email": "jane@example.com", "phone": "555-0100"},
            "data_request": {"id": 7}}


def _job(ds, status, job_id):
    from terno_dbi.core.models import ApiQueryJob
    return ApiQueryJob.objects.create(
        id=job_id, organisation=ds.organisation, data_source=ds, status=status,
        result={"rows": [{"customer": "Jane"}]})


@pytest.mark.django_db
class TestWebhookAuth:
    def test_invalid_hmac_is_401(self, client, connected):
        resp = _post(client, "shop/redact", _shop_redact_payload(), hmac_header="bad")
        assert resp.status_code == 401
        assert _bundle(connected)["ACCESS_TOKEN"] == "shpat_live"

    def test_wrong_secret_is_401(self, client, connected):
        resp = _post(client, "app/uninstalled", _uninstalled_payload(), secret="other")
        assert resp.status_code == 401
        assert _bundle(connected)["ACCESS_TOKEN"] == "shpat_live"

    def test_tampered_body_is_401(self, client, connected):
        signed = json.dumps(_shop_redact_payload("other-store.myshopify.com")).encode()
        tampered = json.dumps(_shop_redact_payload()).encode()
        resp = _post(client, "shop/redact", None, body=tampered,
                     hmac_header=_sign_body(signed))
        assert resp.status_code == 401

    def test_missing_hmac_is_401(self, client, connected):
        body = json.dumps(_shop_redact_payload()).encode()
        resp = client.post(WEBHOOK_PATH, data=body, content_type="application/json",
                           HTTP_X_SHOPIFY_TOPIC="shop/redact",
                           HTTP_X_SHOPIFY_SHOP_DOMAIN=SHOP)
        assert resp.status_code == 401

    def test_no_secret_is_401(self, client, connected, monkeypatch):
        monkeypatch.delenv("TERNO_SHOPIFY_CLIENT_SECRET")
        resp = _post(client, "shop/redact", _shop_redact_payload())
        assert resp.status_code == 401

    def test_get_not_allowed(self, client):
        assert client.get(WEBHOOK_PATH).status_code == 405

    def test_csrf_exempt(self, connected):
        from django.test import Client
        resp = _post(Client(enforce_csrf_checks=True), "customers/data_request",
                     _customers_payload())
        assert resp.status_code == 200

    def test_unknown_topic_acknowledged(self, client, connected):
        resp = _post(client, "orders/create", {"id": 1})
        assert resp.status_code == 200
        assert _bundle(connected)["ACCESS_TOKEN"] == "shpat_live"

    def test_invalid_shop_header_is_400(self, client, connected):
        resp = _post(client, "shop/redact", _shop_redact_payload("evil.example.com"),
                     shop="evil.example.com")
        assert resp.status_code == 400

    def test_header_body_shop_mismatch_is_400(self, client, connected, other_store):
        # A signed body for one store cannot be pointed at another by header.
        resp = _post(client, "shop/redact",
                     _shop_redact_payload("other-store.myshopify.com"), shop=SHOP)
        assert resp.status_code == 400
        assert _bundle(connected)["ACCESS_TOKEN"] == "shpat_live"
        assert _bundle(other_store)["ACCESS_TOKEN"] == "shpat_live"

    def test_topic_body_shape_mismatch_is_400(self, client, connected):
        # A compliance body replayed under app/uninstalled lacks myshopify_domain.
        resp = _post(client, "app/uninstalled", _shop_redact_payload())
        assert resp.status_code == 400
        assert _bundle(connected)["ACCESS_TOKEN"] == "shpat_live"

    def test_non_json_body_is_400(self, client):
        assert _post(client, "shop/redact", None, body=b"not json").status_code == 400

    def test_non_object_body_is_400(self, client):
        assert _post(client, "shop/redact", None, body=b"[1, 2]").status_code == 400

    def test_handler_failure_is_500(self, client, connected, monkeypatch):
        def boom(shop, payload):
            raise RuntimeError("db down")
        monkeypatch.setitem(shopify_app._HANDLERS, "shop/redact", boom)
        assert _post(client, "shop/redact", _shop_redact_payload()).status_code == 500


@pytest.mark.django_db
class TestAppUninstalled:
    def test_clears_tokens_keeps_shop(self, client, connected, other_store):
        from terno_dbi.core.models import DataSource

        resp = _post(client, "app/uninstalled", _uninstalled_payload())
        assert resp.status_code == 200

        assert _bundle(connected) == {"INSTANCE": SHOP}
        connected.refresh_from_db()
        assert connected.auth_status == DataSource.AuthStatus.NOT_AUTHENTICATED
        assert "uninstalled" in connected.auth_error
        # Another store in the same org is untouched.
        assert _bundle(other_store)["ACCESS_TOKEN"] == "shpat_live"

    def test_stored_bundle_stays_encrypted(self, client, connected):
        from terno_dbi.core.models import DataSource
        from terno_dbi.services.secrets import is_encrypted

        _post(client, "app/uninstalled", _uninstalled_payload())
        raw = DataSource.objects.filter(pk=connected.pk).values_list(
            "connection_json", flat=True).get()
        assert is_encrypted(raw)

    def test_every_org_connected_to_the_store(self, client, connected, shopify_catalog):
        from django.contrib.auth.models import User
        from terno_dbi.core.models import CoreOrganisation

        owner = User.objects.create_user("other-owner", "x@example.com", "pw")
        other_org = CoreOrganisation.objects.create(
            name="Other", subdomain="shop-other", owner=owner)
        second = _connection(other_org, shopify_catalog, SHOP, "Shopify other org")

        assert _post(client, "app/uninstalled", _uninstalled_payload()).status_code == 200
        assert "ACCESS_TOKEN" not in _bundle(connected)
        assert "ACCESS_TOKEN" not in _bundle(second)

    def test_domain_match_is_case_insensitive(self, client, org, shopify_catalog):
        ds = _connection(org, shopify_catalog, "Acme-Store.myshopify.com", "Shopify")
        _post(client, "app/uninstalled", _uninstalled_payload())
        assert "ACCESS_TOKEN" not in _bundle(ds)

    def test_unknown_store_is_200(self, client, connected):
        resp = _post(client, "app/uninstalled",
                     _uninstalled_payload("nobody.myshopify.com"),
                     shop="nobody.myshopify.com")
        assert resp.status_code == 200
        assert _bundle(connected)["ACCESS_TOKEN"] == "shpat_live"

    def test_redelivery_is_idempotent(self, client, connected):
        assert _post(client, "app/uninstalled", _uninstalled_payload()).status_code == 200
        assert _post(client, "app/uninstalled", _uninstalled_payload()).status_code == 200
        assert _bundle(connected) == {"INSTANCE": SHOP}

    def test_unreadable_row_is_skipped(self, client, connected, org, shopify_catalog):
        from terno_dbi.core.models import DataSource

        broken = _connection(org, shopify_catalog, SHOP, "Shopify broken")
        DataSource.objects.filter(pk=broken.pk).update(
            connection_json={"__enc__": 1, "ct": "not-a-fernet-token"})
        assert _post(client, "app/uninstalled", _uninstalled_payload()).status_code == 200
        assert "ACCESS_TOKEN" not in _bundle(connected)


@pytest.mark.django_db
class TestShopRedact:
    def test_deletes_store_connections_and_cascades(self, client, connected, other_store):
        from terno_dbi.core.models import ApiQueryJob, DataSource

        _job(connected, ApiQueryJob.Status.COMPLETED, "job-1")
        resp = _post(client, "shop/redact", _shop_redact_payload())
        assert resp.status_code == 200
        assert not DataSource.objects.filter(pk=connected.pk).exists()
        assert not ApiQueryJob.objects.filter(id="job-1").exists()
        assert DataSource.objects.filter(pk=other_store.pk).exists()

    def test_after_uninstall(self, client, connected):
        from terno_dbi.core.models import DataSource

        _post(client, "app/uninstalled", _uninstalled_payload())
        assert _post(client, "shop/redact", _shop_redact_payload()).status_code == 200
        assert not DataSource.objects.filter(pk=connected.pk).exists()

    def test_redelivery_is_idempotent(self, client, connected):
        assert _post(client, "shop/redact", _shop_redact_payload()).status_code == 200
        assert _post(client, "shop/redact", _shop_redact_payload()).status_code == 200


@pytest.mark.django_db
class TestCustomersRedact:
    def test_deletes_finished_results_for_store_only(self, client, connected, other_store):
        from terno_dbi.core.models import ApiQueryJob, DataSource

        _job(connected, ApiQueryJob.Status.COMPLETED, "done")
        _job(connected, ApiQueryJob.Status.FAILED, "failed")
        _job(connected, ApiQueryJob.Status.RUNNING, "running")
        _job(other_store, ApiQueryJob.Status.COMPLETED, "other")

        resp = _post(client, "customers/redact", _customers_payload())
        assert resp.status_code == 200
        remaining = set(ApiQueryJob.objects.values_list("id", flat=True))
        assert remaining == {"running", "other"}
        # The connection itself stays.
        assert _bundle(connected)["ACCESS_TOKEN"] == "shpat_live"
        assert DataSource.objects.filter(pk=connected.pk).exists()

    def test_logs_no_contact_details(self, client, connected, caplog):
        caplog.set_level("INFO", logger="terno_dbi.connectors.api.shopify_app")
        _post(client, "customers/redact", _customers_payload())
        assert "jane@example.com" not in caplog.text
        assert "555-0100" not in caplog.text
        assert "42" in caplog.text


@pytest.mark.django_db
class TestCustomersDataRequest:
    def test_acknowledged_and_logged(self, client, connected, caplog):
        caplog.set_level("INFO", logger="terno_dbi.connectors.api.shopify_app")
        resp = _post(client, "customers/data_request", _customers_payload())
        assert resp.status_code == 200
        assert "customers/data_request" in caplog.text
        assert "jane@example.com" not in caplog.text
        assert _bundle(connected)["ACCESS_TOKEN"] == "shpat_live"

    def test_missing_customer_block_still_200(self, client):
        resp = _post(client, "customers/data_request", {"shop_domain": SHOP})
        assert resp.status_code == 200


# --------------------------------------------------------------------------
# Other flows are unaffected; end-to-end and adversarial scenarios
# --------------------------------------------------------------------------

ORG_HOST = "shop-acme.testserver"


@pytest.fixture
def google_ads_catalog(db, monkeypatch):
    from terno_dbi.core.models import ConnectorCatalog
    monkeypatch.setenv("TERNO_GOOGLE_OAUTH_CLIENT_ID", "g-client")
    monkeypatch.setenv("TERNO_GOOGLE_OAUTH_CLIENT_SECRET", "g-secret")
    cat, _ = ConnectorCatalog.objects.get_or_create(
        key="google_ads",
        defaults=dict(display_name="Google Ads", family="api", auth_type="oauth"),
    )
    if not cat.enabled:
        cat.enabled = True
        cat.save(update_fields=["enabled"])
    return cat


def _org_client(user):
    from django.test import Client
    c = Client(HTTP_HOST=ORG_HOST)
    c.force_login(user)
    return c


def _shopify_cookies_touched(resp):
    return {shopify_app.STATE_COOKIE, shopify_app.CLAIM_COOKIE} & set(resp.cookies)


@pytest.mark.django_db
class TestOtherFlowsUnchanged:
    def test_google_ads_connect_and_callback(self, org, google_ads_catalog,
                                             token_exchange):
        from terno_dbi.core.models import DataSource, ShopifyPendingInstall
        c = _org_client(org.owner)
        start = c.get("/connect", {"connector": "google_ads", "return_to": "/chat"})
        assert start.status_code == 302
        assert urlsplit(start.url).netloc == "accounts.google.com"
        assert not _shopify_cookies_touched(start)
        state = _query(start.url)["state"]

        done = c.get(CALLBACK_PATH, {"state": state, "code": "g-code"})
        assert done.status_code == 302
        assert urlsplit(done.url).path == "/chat"
        assert _query(done.url)["select_accounts"] == "google_ads"
        assert not _shopify_cookies_touched(done)
        ds = DataSource.objects.get(organisation=org, catalog=google_ads_catalog)
        assert _bundle(ds)["ACCESS_TOKEN"] == "shpat_new"
        assert not ShopifyPendingInstall.objects.exists()

    def test_shopify_connect_from_terno_still_uses_normal_flow(
            self, org, shopify_catalog, token_exchange):
        # The in-app flow (merchant types the store) is untouched: the state
        # carries the org, the callback stores straight onto the datasource,
        # and nothing is held or claimed.
        from terno_dbi.core.models import (
            ConnectorOAuthState, DataSource, ShopifyPendingInstall)
        c = _org_client(org.owner)
        start = c.get("/connect", {"connector": "shopify", "shop": "acme-store"})
        assert start.status_code == 302
        assert urlsplit(start.url).netloc == SHOP
        assert not _shopify_cookies_touched(start)
        state = _query(start.url)["state"]
        assert ConnectorOAuthState.objects.get(state=state).organisation_id == org.id

        # Shopify signs the callback, but the normal flow never needed it.
        done = c.get(CALLBACK_PATH, _callback_params(state))
        assert done.status_code == 200
        assert json.loads(done.content)["status"] == "connected"
        assert not _shopify_cookies_touched(done)
        ds = DataSource.objects.get(organisation=org, catalog=shopify_catalog)
        assert _bundle(ds)["INSTANCE"] == SHOP
        assert not ShopifyPendingInstall.objects.exists()

    def test_shopify_reconnect_of_existing_datasource(self, org, connected,
                                                      token_exchange):
        from datetime import timedelta
        from django.utils import timezone
        from terno_dbi.connectors.api import web
        from terno_dbi.core.models import ConnectorOAuthState, ShopifyPendingInstall
        ConnectorOAuthState.objects.create(
            state="reconnect", connector_key="shopify", redirect_uri="https://x/cb",
            organisation=org, data_source=connected, instance=SHOP,
            expires_at=timezone.now() + timedelta(minutes=5))
        request = RequestFactory().get(CALLBACK_PATH, {"state": "reconnect", "code": "c"})
        request.user = AnonymousUser()
        resp = web.oauth_callback(request)
        assert resp.status_code == 200
        assert _bundle(connected)["ACCESS_TOKEN"] == "shpat_new"
        assert not ShopifyPendingInstall.objects.exists()

    def test_stale_install_cookie_does_not_divert_normal_callback(
            self, org, shopify_catalog, token_exchange):
        from terno_dbi.core.models import DataSource
        c = _org_client(org.owner)
        c.cookies[shopify_app.STATE_COOKIE] = "left-over"
        state = _query(c.get("/connect", {"connector": "shopify",
                                          "shop": SHOP}).url)["state"]
        assert c.get(CALLBACK_PATH, _callback_params(state)).status_code == 200
        assert DataSource.objects.filter(organisation=org).exists()

    def test_install_param_on_other_connector_is_ignored(self, org, google_ads_catalog):
        c = _org_client(org.owner)
        resp = c.get("/connect", {"connector": "google_ads", "install": "1"})
        assert resp.status_code == 302
        assert urlsplit(resp.url).netloc == "accounts.google.com"

    def test_callback_without_state_or_code_unchanged(self, client):
        assert client.get(CALLBACK_PATH, {"state": "x"}).status_code == 400
        assert client.get(CALLBACK_PATH, {"code": "x"}).status_code == 400

    def test_unknown_state_unchanged(self, client, db):
        resp = client.get(CALLBACK_PATH, {"state": "nope", "code": "c"})
        assert resp.status_code == 400


class TestMcpLoginRedirectUnchanged:
    """The MCP (Claude/ChatGPT) sign-in carry-over accepts the same paths."""

    def test_mcp_authorize_next_still_accepted(self):
        from terno_dbi.oauth.login_redirect import AUTHORIZE_PATHS, is_connector_next
        assert AUTHORIZE_PATHS == ("/oauth/authorize", "/connect")
        assert is_connector_next(
            "/oauth/authorize/?response_type=code&client_id=abc&scope=query%3Aread")

    def test_install_resume_path_is_carried_through_sign_in(self):
        from terno_dbi.oauth.login_redirect import is_connector_next
        assert is_connector_next(shopify_app._connect_path(SHOP))

    def test_mcp_routes_still_resolve(self):
        from django.urls import resolve
        assert resolve("/oauth/authorize/").url_name == "authorize"
        assert resolve("/connectors/oauth/callback/").url_name == "oauth_callback"


@pytest.mark.django_db
class TestEndToEnd:
    def test_install_consent_signin_claim(self, client, org, shopify_catalog,
                                          token_exchange, settings):
        """The whole merchant journey, through the URL routes."""
        from terno_dbi.core.models import DataSource, ShopifyPendingInstall
        settings.TERNO_CONNECTOR_LOGIN_URL = "https://app.terno.ai/accounts/login/"
        settings.TERNO_SHOPIFY_INSTALL_RETURN_TO = ""

        # 1. Install: straight to Shopify consent, nothing about Terno sign-in.
        install = client.get(INSTALL_PATH, _install_params())
        assert urlsplit(install.url).netloc == SHOP
        # 2. Callback: token held, merchant sent to sign in.
        callback = client.get(CALLBACK_PATH, _callback_params(_query(install.url)["state"]))
        next_path = _next_of(callback.url)
        assert ShopifyPendingInstall.objects.count() == 1
        assert not DataSource.objects.exists()
        # 3. Signed in, the login service resumes `next` on the org's host.
        client.force_login(org.owner)
        claim = client.get(next_path, HTTP_HOST=ORG_HOST)
        assert claim.status_code == 302
        assert _query(claim.url)["select_accounts"] == "shopify"
        ds = DataSource.objects.get(organisation=org)
        assert ds.auth_status == DataSource.AuthStatus.CONNECTED
        assert _bundle(ds)["INSTANCE"] == SHOP
        assert not ShopifyPendingInstall.objects.exists()

    def test_second_install_in_same_browser_supersedes_first(
            self, client, shopify_catalog, token_exchange):
        first = _start(client)
        second = _start(client)
        assert client.get(CALLBACK_PATH, _callback_params(first)).status_code == 400
        assert client.get(CALLBACK_PATH, _callback_params(second)).status_code == 302

    def test_attacker_cannot_claim_victims_install(self, client, org, shopify_catalog,
                                                   token_exchange):
        """Someone who knows the store and the claim URL, but not the cookie."""
        from django.contrib.auth.models import User
        from terno_dbi.core.models import (
            CoreOrganisation, DataSource, ShopifyPendingInstall)
        _authorize(client, token_exchange)      # the victim's browser

        attacker = User.objects.create_user("attacker", "a@example.com", "pw")
        from terno_dbi.core.models import OrganisationUser
        evil_org = CoreOrganisation.objects.create(
            name="Evil", subdomain="evil", owner=attacker)
        OrganisationUser.objects.create(user=attacker, organisation=evil_org)
        from django.test import Client
        evil = Client(HTTP_HOST="evil.testserver")
        evil.force_login(attacker)
        resp = evil.get(shopify_app._connect_path(SHOP))
        assert resp.status_code == 400
        assert not DataSource.objects.exists()
        assert ShopifyPendingInstall.objects.count() == 1   # victim can still claim

    def test_claim_only_touches_the_claiming_org(self, client, org, shopify_catalog,
                                                 token_exchange):
        from django.contrib.auth.models import User
        from terno_dbi.core.models import CoreOrganisation
        other_owner = User.objects.create_user("o2", "o2@example.com", "pw")
        other_org = CoreOrganisation.objects.create(
            name="Other", subdomain="other", owner=other_owner)
        theirs = _connection(other_org, shopify_catalog, SHOP, "Shopify (other)")

        callback, _ = _authorize(client, token_exchange)
        client.force_login(org.owner)
        assert client.get(shopify_app._connect_path(SHOP),
                          HTTP_HOST=ORG_HOST).status_code == 302
        assert _bundle(theirs)["ACCESS_TOKEN"] == "shpat_live"   # untouched

    def test_held_token_never_appears_in_redirects(self, client, shopify_catalog,
                                                   token_exchange):
        resp, _ = _authorize(client, token_exchange)
        assert "shpat_new" not in resp.url
        assert "shpat_new" not in str(resp.cookies)

    def test_missing_scopes_are_logged(self, client, shopify_catalog, token_exchange,
                                       caplog):
        import logging
        with caplog.at_level(logging.WARNING, logger="terno_dbi.connectors.api.shopify_app"):
            _authorize(client, token_exchange)     # the stub grants read_orders only
        assert "scopes not granted" in caplog.text
        assert "read_products" in caplog.text
