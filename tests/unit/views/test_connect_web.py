"""The browser-facing connect flow (Phase 1 web glue).

Exercises /connect and the OAuth callback with the token exchange mocked, so the
whole click-through — consent redirect, callback, encrypted-token storage — is
proven without a live Google.
"""

import pytest
from django.contrib.auth.models import AnonymousUser, User
from django.test import RequestFactory

from terno_dbi.catalog.refresh import refresh_catalog
from terno_dbi.connectors.api.auth import oauth as oauth_mod
from terno_dbi.connectors.api import web
from terno_dbi.core.models import (
    ConnectorOAuthState,
    CoreOrganisation,
    DataSource,
    OrganisationUser,
)
from terno_dbi.services import secrets


@pytest.fixture(autouse=True)
def google_creds(monkeypatch):
    monkeypatch.setenv("TERNO_GOOGLE_OAUTH_CLIENT_ID", "cid")
    monkeypatch.setenv("TERNO_GOOGLE_OAUTH_CLIENT_SECRET", "csecret")
    secrets.reset_cache()
    yield
    secrets.reset_cache()


@pytest.fixture
def member(db):
    user = User.objects.create_user("mem", "m@x.com", "pw")
    org = CoreOrganisation.objects.create(name="Acme", subdomain="acme", owner=user)
    OrganisationUser.objects.create(organisation=org, user=user)
    refresh_catalog()
    return {"user": user, "org": org}


def _get(path, user=None, host="acme.app.terno.ai", **params):
    from urllib.parse import urlencode
    q = ("?" + urlencode(params)) if params else ""
    request = RequestFactory().get(path + q, HTTP_HOST=host)
    request.user = user or AnonymousUser()
    return request


@pytest.mark.django_db
class TestConnect:
    def test_anonymous_is_sent_to_login(self, member, settings):
        settings.DEBUG = True
        resp = web.connect(_get("/connect", connector="googleanalytics4"))
        assert resp.status_code == 302
        assert "next=" in resp.url        # bounced to login, will come back

    def test_unknown_connector_is_a_400(self, member):
        resp = web.connect(_get("/connect", member["user"], connector="nope"))
        assert resp.status_code == 400

    def test_non_member_is_forbidden(self, member, settings):
        settings.DEBUG = True
        outsider = User.objects.create_user("out", "o@x.com", "pw")
        resp = web.connect(
            _get("/connect", outsider, connector="googleanalytics4"))
        assert resp.status_code == 403

    def test_member_is_redirected_to_google_consent(self, member, settings):
        settings.DEBUG = True
        resp = web.connect(
            _get("/connect", member["user"], connector="googleanalytics4"))
        assert resp.status_code == 302
        assert "accounts.google.com" in resp.url
        assert "code_challenge=" in resp.url        # PKCE
        # State recorded for the callback.
        assert ConnectorOAuthState.objects.filter(
            organisation=member["org"]).exists()

    def test_embedded_host_org_id_is_used_when_no_subdomain(self, member, settings):
        # In an embedding app (terno-ai) the host is not an org subdomain — e.g.
        # local 127.0.0.1 or the single-tenant default — and the org is resolved
        # onto request.org_id by the host's middleware. The connect flow must use
        # it instead of parsing the (absent) subdomain, or it 403s. Still
        # membership-validated, so the value is not trusted blindly.
        request = _get("/connect", member["user"], host="127.0.0.1:8000",
                       connector="googleanalytics4")
        request.org_id = member["org"].id
        settings.DEBUG = True
        resp = web.connect(request)
        assert resp.status_code == 302
        assert "accounts.google.com" in resp.url

    def test_embedded_host_org_id_still_rejects_non_member(self, member, settings):
        # A request.org_id the signed-in user is not a member of must fail closed.
        outsider = User.objects.create_user("out2", "o2@x.com", "pw")
        request = _get("/connect", outsider, host="127.0.0.1:8000",
                       connector="googleanalytics4")
        request.org_id = member["org"].id      # not this user's org
        settings.DEBUG = True
        resp = web.connect(request)
        assert resp.status_code == 403

    def test_manual_connector_goes_to_the_credentials_form(self, member, settings):
        settings.DEBUG = True
        settings.MAIN_DOMAIN = "app.terno.ai"
        resp = web.connect(
            _get("/connect", member["user"], connector="postgres"))
        assert resp.status_code == 302
        assert "/admin/" in resp.url          # not an OAuth redirect


@pytest.mark.django_db
class TestCallback:
    def _start(self, member, settings):
        settings.DEBUG = True
        web.connect(_get("/connect", member["user"], connector="googleanalytics4"))
        return ConnectorOAuthState.objects.get(organisation=member["org"]).state

    def test_callback_completes_and_stores_encrypted_tokens(
        self, member, settings, monkeypatch
    ):
        state = self._start(member, settings)

        # Mock the token exchange so no real Google call happens.
        monkeypatch.setattr(
            oauth_mod, "_default_post",
            lambda url, data: {"access_token": "at", "refresh_token": "rt",
                               "expires_in": 3600},
        )
        resp = web.oauth_callback(
            _get("/connectors/oauth/callback/", member["user"],
                 state=state, code="the-code"))
        assert resp.status_code == 200

        ds = DataSource.objects.get(type="googleanalytics4",
                                    organisation=member["org"])
        assert ds.auth_status == DataSource.AuthStatus.CONNECTED
        assert secrets.is_encrypted(ds.connection_json)
        assert secrets.decrypt_dict(ds.connection_json)["ACCESS_TOKEN"] == "at"

    def test_reconnect_reuses_the_same_datasource(self, member, settings, monkeypatch):
        # Clicking Connect again (or re-authing) must not create a duplicate
        # DataSource for the same org+connector — it re-authenticates the one row.
        monkeypatch.setattr(
            oauth_mod, "_default_post",
            lambda url, data: {"access_token": "at", "refresh_token": "rt",
                               "expires_in": 3600},
        )
        for _ in range(2):
            state = self._start(member, settings)
            web.oauth_callback(
                _get("/connectors/oauth/callback/", member["user"],
                     state=state, code="the-code"))
        assert DataSource.objects.filter(
            type="googleanalytics4", organisation=member["org"]).count() == 1

    def test_disconnect_deauths_all_rows_but_keeps_them(self, member, settings):
        # Disconnect clears tokens and marks the source not-authenticated, but
        # KEEPS the row so its memory/history survive. A source connected more
        # than once before the reuse fix may have duplicates; all are cleared.
        from terno_dbi.core.models import ConnectorCatalog

        cat = ConnectorCatalog.objects.get(key="googleanalytics4")
        for i in range(2):
            DataSource.objects.create(
                display_name=f"GA4 {i}", type="googleanalytics4",
                connection_str="", organisation=member["org"], catalog=cat,
                auth_status=DataSource.AuthStatus.CONNECTED,
                connection_json={"ACCESS_TOKEN": "at"})

        request = RequestFactory().post(
            "/connectors/api/googleanalytics4/disconnect/",
            HTTP_HOST="acme.app.terno.ai")
        request.user = member["user"]
        resp = web.disconnect_connector(request, "googleanalytics4")
        assert resp.status_code == 200

        remaining = DataSource.objects.filter(
            type="googleanalytics4", organisation=member["org"])
        assert remaining.count() == 2      # rows kept (memory/history preserved)
        assert not remaining.exclude(
            auth_status=DataSource.AuthStatus.NOT_AUTHENTICATED).exists()
        # Tokens gone: connection_json is stored encrypted, so the decrypted
        # payload — not the (non-empty) envelope — must be empty.
        assert all(not secrets.decrypt_dict(d.connection_json) for d in remaining)

    def test_declined_authorization_is_reported(self, member):
        resp = web.oauth_callback(
            _get("/connectors/oauth/callback/", member["user"],
                 error="access_denied"))
        assert resp.status_code == 400

    def test_missing_code_is_a_400(self, member):
        resp = web.oauth_callback(
            _get("/connectors/oauth/callback/", member["user"], state="x"))
        assert resp.status_code == 400
