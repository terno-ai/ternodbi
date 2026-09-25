"""Browser OAuth/connect routes in connectors/api/web.py.

Covers the connect entry point, the OAuth callback (including the
`select_accounts` return-suffix that drives the account picker), the connector
gallery listing, and disconnect — across their auth gates and edge cases.
"""

import json

import pytest
from django.test import RequestFactory

from terno_dbi.connectors.api import web
from terno_dbi.connectors.api.model.errors import ApiError, ErrorCode


@pytest.fixture
def org(db):
    from django.contrib.auth.models import User
    from terno_dbi.core.models import CoreOrganisation, OrganisationUser

    owner = User.objects.create_user("wf-owner", "o@example.com", "pw")
    org = CoreOrganisation.objects.create(name="Acme", subdomain="wf-acme", owner=owner)
    OrganisationUser.objects.create(user=owner, organisation=org)
    return org


@pytest.fixture
def member(db, org):
    from django.contrib.auth.models import User
    from terno_dbi.core.models import OrganisationUser

    u = User.objects.create_user("wf-member", "m@example.com", "pw")
    OrganisationUser.objects.create(user=u, organisation=org)
    return u


@pytest.fixture
def oauth_catalog(db):
    from terno_dbi.core.models import ConnectorCatalog
    cat, _ = ConnectorCatalog.objects.get_or_create(
        key="google_ads",
        defaults=dict(display_name="Google Ads", family="api", auth_type="oauth"),
    )
    return cat


def _get(path, user, org):
    request = RequestFactory().get(path)
    request.user = user
    request.org_id = org.id
    return request


# ----------------------------- connect() ---------------------------------

@pytest.mark.django_db
def test_connect_unknown_connector_is_400(org):
    resp = web.connect(_get("/connect?connector=nope", org.owner, org))
    assert resp.status_code == 400


@pytest.mark.django_db
def test_connect_unauthenticated_redirects_to_login(org, oauth_catalog):
    from django.contrib.auth.models import AnonymousUser
    request = RequestFactory().get("/connect?connector=google_ads")
    request.user = AnonymousUser()
    request.org_id = org.id
    resp = web.connect(request)
    assert resp.status_code == 302
    assert "login" in resp.url


@pytest.mark.django_db
def test_connect_non_admin_is_403(org, oauth_catalog, member):
    resp = web.connect(_get("/connect?connector=google_ads", member, org))
    assert resp.status_code == 403


@pytest.mark.django_db
def test_connect_oauth_redirects_to_provider(org, oauth_catalog, monkeypatch):
    monkeypatch.setattr(
        "terno_dbi.connectors.api.auth.oauth.start_authorization",
        lambda **kw: {"authorization_url": "https://provider/auth", "state": "s"},
    )
    resp = web.connect(_get("/connect?connector=google_ads", org.owner, org))
    assert resp.status_code == 302
    assert resp.url == "https://provider/auth"


@pytest.mark.django_db
def test_connect_offsite_return_to_is_ignored(org, oauth_catalog, monkeypatch):
    captured = {}
    def _fake_start(**kw):
        captured.update(kw)
        return {"authorization_url": "https://provider/auth", "state": "s"}
    monkeypatch.setattr(
        "terno_dbi.connectors.api.auth.oauth.start_authorization", _fake_start)
    web.connect(_get("/connect?connector=google_ads&return_to=https://evil.com/x",
                     org.owner, org))
    assert captured["return_to"] == ""     # off-site target dropped


@pytest.mark.django_db
def test_connect_provider_error_is_400(org, oauth_catalog, monkeypatch):
    def _boom(**kw):
        raise ApiError(ErrorCode.UPSTREAM_ERROR, "no creds")
    monkeypatch.setattr(
        "terno_dbi.connectors.api.auth.oauth.start_authorization", _boom)
    resp = web.connect(_get("/connect?connector=google_ads", org.owner, org))
    assert resp.status_code == 400


# --------------------------- oauth_callback() ----------------------------

class _DS:
    display_name = "Google Ads"


@pytest.mark.django_db
def test_callback_declined_is_400(org):
    resp = web.oauth_callback(RequestFactory().get("/cb?error=access_denied"))
    assert resp.status_code == 400


@pytest.mark.django_db
def test_callback_missing_state_or_code_is_400(org):
    resp = web.oauth_callback(RequestFactory().get("/cb?state=only"))
    assert resp.status_code == 400


@pytest.mark.django_db
def test_callback_success_appends_select_accounts(org, monkeypatch):
    from terno_dbi.core.models import ConnectorOAuthState
    from django.utils import timezone
    from datetime import timedelta
    ConnectorOAuthState.objects.create(
        state="st1", connector_key="google_ads", code_verifier="",
        redirect_uri="https://x/cb", organisation=org,
        return_to="https://wf-acme.app/data-connectors/connectors/google-ads",
        expires_at=timezone.now() + timedelta(minutes=5),
    )
    monkeypatch.setattr(
        "terno_dbi.connectors.api.auth.oauth.complete_authorization",
        lambda **kw: _DS())
    resp = web.oauth_callback(RequestFactory().get("/cb?state=st1&code=c"))
    assert resp.status_code == 302
    assert "connected=Google%20Ads" in resp.url
    assert "select_accounts=google_ads" in resp.url


@pytest.mark.django_db
def test_callback_without_return_to_returns_json(org, monkeypatch):
    monkeypatch.setattr(
        "terno_dbi.connectors.api.auth.oauth.complete_authorization",
        lambda **kw: _DS())
    resp = web.oauth_callback(RequestFactory().get("/cb?state=none&code=c"))
    assert resp.status_code == 200
    assert json.loads(resp.content)["status"] == "connected"


# ------------------------ list / disconnect ------------------------------

@pytest.mark.django_db
def test_list_api_connectors_reports_status(org, oauth_catalog):
    resp = web.list_api_connectors(_get("/connectors/api/", org.owner, org))
    payload = json.loads(resp.content)
    assert payload["can_manage"] is True
    keys = {c["key"] for c in payload["connectors"]}
    assert "google_ads" in keys
    ga = next(c for c in payload["connectors"] if c["key"] == "google_ads")
    assert ga["status"] == "not_connected"


@pytest.mark.django_db
def test_disconnect_clears_tokens(org, oauth_catalog):
    from terno_dbi.core.models import DataSource
    from terno_dbi.services.secrets import encrypt_dict
    ds = DataSource.objects.create(
        display_name="Google Ads", type="google_ads", connection_str="",
        organisation=org, catalog=oauth_catalog,
        connection_json=encrypt_dict({"ACCESS_TOKEN": "x"}),
        auth_status=DataSource.AuthStatus.CONNECTED,
    )
    request = RequestFactory().post("/connectors/api/google_ads/disconnect/")
    request.user = org.owner
    request.org_id = org.id
    resp = web.disconnect_connector(request, "google_ads")
    assert resp.status_code == 200
    ds.refresh_from_db()
    assert ds.auth_status == DataSource.AuthStatus.NOT_AUTHENTICATED


@pytest.mark.django_db
def test_disconnect_not_connected_is_404(org, oauth_catalog):
    request = RequestFactory().post("/connectors/api/google_ads/disconnect/")
    request.user = org.owner
    request.org_id = org.id
    resp = web.disconnect_connector(request, "google_ads")
    assert resp.status_code == 404


@pytest.mark.django_db
def test_disconnect_non_admin_is_403(org, member):
    request = RequestFactory().post("/connectors/api/google_ads/disconnect/")
    request.user = member
    request.org_id = org.id
    resp = web.disconnect_connector(request, "google_ads")
    assert resp.status_code == 403
