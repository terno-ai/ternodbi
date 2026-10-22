"""OAuth 1.0a (X Ads): request signing and the three-legged connect flow.

The signature is checked against X's published example; the flow runs with the
HTTP injected, from the request token through to the stored access token.
"""

from urllib.parse import unquote

import pytest
from django.contrib.auth.models import User

from terno_dbi.catalog.refresh import refresh_catalog
from terno_dbi.connectors.api.auth import oauth, oauth1
from terno_dbi.connectors.api.auth.providers import get_provider
from terno_dbi.connectors.api.model.errors import ApiError
from terno_dbi.core.models import ConnectorOAuthState, CoreOrganisation, DataSource
from terno_dbi.services import secrets

CALLBACK = "https://acme.app.terno.ai/connectors/oauth/callback/"


def _header_params(header):
    assert header.startswith("OAuth ")
    pairs = (p.split("=", 1) for p in header[len("OAuth "):].split(", "))
    return {unquote(k): unquote(v.strip('"')) for k, v in pairs}


class TestSigning:
    # X's worked example: "Creating a signature" in the X API docs.
    _EXAMPLE = dict(
        consumer_secret="kAcSOqF21Fu85e7zjz7ZN2U4ZRhfV3WpwPAoE3Z7kBw",
        token_secret="LswwdoUaIvS8ltyTt5jkRh4J50vUPVVHtR2YPi5kE",
    )
    _OAUTH = {
        "oauth_consumer_key": "xvz1evFS4wEEPTGEFPHBog",
        "oauth_nonce": "kYjzVBB8Y0ZFabxSWbWovY3uYSQ2pTgmZeNu2VS4cg",
        "oauth_signature_method": "HMAC-SHA1",
        "oauth_timestamp": "1318622958",
        "oauth_token": "370773112-GmHxMAgYyLbNEtIKZeRNFsMKPR9EyMZeS9weJAEb",
        "oauth_version": "1.0",
    }

    def test_matches_xs_published_signature(self):
        params = {**self._OAUTH, "include_entities": "true",
                  "status": "Hello Ladies + Gentlemen, a signed OAuth request!"}
        assert oauth1.signature(
            "POST", "https://api.twitter.com/1.1/statuses/update.json", params,
            **self._EXAMPLE) == "hCtSmYh+iHYCEqBWrE7C7hYmtUk="

    def test_the_query_string_is_signed(self):
        params = {**self._OAUTH,
                  "status": "Hello Ladies + Gentlemen, a signed OAuth request!"}
        assert oauth1.signature(
            "POST",
            "https://api.twitter.com/1.1/statuses/update.json?include_entities=true",
            params, **self._EXAMPLE) == "hCtSmYh+iHYCEqBWrE7C7hYmtUk="

    def test_header_carries_every_protocol_parameter(self):
        header = oauth1.authorization_header(
            "GET", "https://ads-api.x.com/12/accounts?count=10",
            consumer_key="ck", consumer_secret="cs", token="t", token_secret="ts",
            nonce="n", timestamp=1700000000)
        params = _header_params(header)
        assert params.pop("oauth_signature") == oauth1.signature(
            "GET", "https://ads-api.x.com/12/accounts?count=10", params, "cs", "ts")
        assert params == {
            "oauth_consumer_key": "ck", "oauth_nonce": "n",
            "oauth_signature_method": "HMAC-SHA1",
            "oauth_timestamp": "1700000000", "oauth_token": "t",
            "oauth_version": "1.0"}

    def test_query_encoding_is_rfc3986(self):
        # A space is %20 (as signed), never '+'; '~' stays bare.
        assert oauth1.encode_query({"a b": "c d", "e": "~,/"}) == "a%20b=c%20d&e=~%2C%2F"


@pytest.fixture
def org(db):
    user = User.objects.create_user("oauth1user", "o@x.com", "pw")
    return CoreOrganisation.objects.create(name="Acme", subdomain="acme", owner=user)


@pytest.fixture
def catalog(db):
    refresh_catalog()


@pytest.fixture(autouse=True)
def x_ads_creds(monkeypatch):
    monkeypatch.setenv("TERNO_X_CONSUMER_KEY", "ck")
    monkeypatch.setenv("TERNO_X_CONSUMER_SECRET", "cs")
    secrets.reset_cache()
    yield
    secrets.reset_cache()


class _X:
    """Stands in for X's two OAuth 1.0a endpoints, recording each POST."""

    def __init__(self, request_token=None, access_token=None):
        self.request_token = request_token or {
            "oauth_token": "req-tok", "oauth_token_secret": "req-secret",
            "oauth_callback_confirmed": "true"}
        self.access_token = access_token if access_token is not None else {
            "oauth_token": "acc-tok", "oauth_token_secret": "acc-secret",
            "user_id": "42", "screen_name": "acme"}
        self.calls = []

    def __call__(self, url, headers):
        self.calls.append((url, _header_params(headers["Authorization"])))
        if url.startswith("https://api.x.com/oauth/request_token"):
            return self.request_token
        assert url == "https://api.x.com/oauth/access_token"
        return self.access_token


def _start(org, x):
    return oauth.start_authorization(
        connector_key="x_ads", redirect_uri=CALLBACK, organisation=org,
        http_post=x)


@pytest.mark.django_db
class TestXAdsFlow:
    def test_provider_is_oauth1(self):
        provider = get_provider("x_ads")
        assert provider.is_oauth1
        assert not get_provider("x_analytics").is_oauth1

    def test_start_fetches_a_request_token_for_our_callback(self, org, catalog):
        x = _X()
        out = _start(org, x)
        url, params = x.calls[0]
        # Read and write, never the app's Direct Message access.
        assert url == "https://api.x.com/oauth/request_token?x_auth_access_type=write"
        assert params["oauth_callback"] == CALLBACK
        assert params["oauth_consumer_key"] == "ck"
        assert "oauth_token" not in params
        assert out["authorization_url"] == (
            "https://api.x.com/oauth/authorize?oauth_token=req-tok")
        # The request token is the state; its secret is kept to sign step 3.
        st = ConnectorOAuthState.objects.get(state="req-tok")
        assert out["state"] == "req-tok"
        assert st.code_verifier == "req-secret"
        assert st.connector_key == "x_ads"

    def test_an_unconfirmed_callback_is_refused(self, org, catalog):
        x = _X(request_token={"oauth_token": "t", "oauth_token_secret": "s",
                              "oauth_callback_confirmed": "false"})
        with pytest.raises(ApiError) as exc:
            _start(org, x)
        assert CALLBACK in exc.value.message
        assert not ConnectorOAuthState.objects.exists()

    def test_a_failed_request_token_is_an_api_error(self, org, catalog):
        def broken(url, headers):
            raise RuntimeError("401 Unauthorized")

        with pytest.raises(ApiError):
            _start(org, broken)

    def test_complete_signs_with_the_request_token_and_stores_both_secrets(
            self, org, catalog):
        x = _X()
        _start(org, x)
        ds = oauth.complete_authorization(
            state="req-tok", code="verifier-1", http_post=x)
        url, params = x.calls[1]
        assert params["oauth_token"] == "req-tok"
        assert params["oauth_verifier"] == "verifier-1"
        # Signed under the request token's secret.
        signed = dict(params)
        sig = signed.pop("oauth_signature")
        assert sig == oauth1.signature("POST", url, signed, "cs", "req-secret")

        bundle = secrets.decrypt_dict(ds.connection_json)
        assert bundle["ACCESS_TOKEN"] == "acc-tok"
        assert bundle["ACCESS_TOKEN_SECRET"] == "acc-secret"
        assert bundle["CONNECTED_EMAIL"] == "@acme"
        # The token does not expire; nothing to refresh.
        assert "TOKEN_EXPIRES_AT" not in bundle
        assert "REFRESH_TOKEN" not in bundle
        assert ds.type == "x_ads"
        assert ds.auth_status == DataSource.AuthStatus.CONNECTED
        assert not ConnectorOAuthState.objects.exists()

    def test_a_refused_exchange_stores_nothing(self, org, catalog):
        x = _X(access_token={})
        _start(org, x)
        with pytest.raises(ApiError):
            oauth.complete_authorization(state="req-tok", code="v", http_post=x)
        assert not DataSource.objects.filter(type="x_ads").exists()

    def test_default_post_decodes_the_form_body(self, monkeypatch):
        import requests
        seen = {}

        class _Resp:
            text = "oauth_token=a&oauth_token_secret=b&oauth_callback_confirmed=true"
            def raise_for_status(self): pass

        def fake_post(url, headers=None, timeout=None):
            seen.update(url=url, headers=headers)
            return _Resp()

        monkeypatch.setattr(requests, "post", fake_post)
        out = oauth1._default_post("https://api.x.com/oauth/request_token",
                                   {"Authorization": "OAuth x"})
        assert out == {"oauth_token": "a", "oauth_token_secret": "b",
                       "oauth_callback_confirmed": "true"}
        assert seen["headers"] == {"Authorization": "OAuth x"}
