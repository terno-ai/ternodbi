"""The browser account-management endpoint: GET lists+syncs, POST saves.

Covers the session-authenticated `connector_accounts` view — org-admin gated,
returns every visible account with its enabled flag and the connected email, and
persists the enabled selection.
"""

import json

import pytest
from django.test import RequestFactory

from terno_dbi.connectors.api import web
from terno_dbi.connectors.api.model.types import Account
from terno_dbi.services.secrets import encrypt_dict


class _FakeConnector:
    def list_accounts(self):
        return [Account(id="1", name="Alpha"), Account(id="2", name="Beta")]


@pytest.fixture
def org(db):
    from django.contrib.auth.models import User
    from terno_dbi.core.models import CoreOrganisation, OrganisationUser

    owner = User.objects.create_user("ca-owner", "o@example.com", "pw")
    org = CoreOrganisation.objects.create(name="Acme", subdomain="ca-acme", owner=owner)
    OrganisationUser.objects.create(user=owner, organisation=org)
    return org


@pytest.fixture
def ds(org):
    from terno_dbi.core.models import ConnectorCatalog, DataSource

    cat, _ = ConnectorCatalog.objects.get_or_create(
        key="google_ads",
        defaults=dict(display_name="Google Ads", family="api", auth_type="oauth"),
    )
    return DataSource.objects.create(
        display_name="Google Ads", type="google_ads", connection_str="",
        organisation=org, catalog=cat,
        connection_json=encrypt_dict({"ACCESS_TOKEN": "x", "CONNECTED_EMAIL": "a@b.com"}),
        auth_status=DataSource.AuthStatus.CONNECTED,
    )


def _req(method, org, monkeypatch, body=None):
    monkeypatch.setattr(web.registry, "build_connector", lambda ds: _FakeConnector())
    rf = RequestFactory()
    if method == "GET":
        request = rf.get("/connectors/api/google_ads/accounts/")
    else:
        request = rf.post("/connectors/api/google_ads/accounts/",
                          data=json.dumps(body or {}), content_type="application/json")
    request.user = org.owner
    request.org_id = org.id
    return request


@pytest.mark.django_db
def test_get_lists_all_accounts_enabled_with_email(org, ds, monkeypatch):
    resp = web.connector_accounts(_req("GET", org, monkeypatch), "google_ads")
    payload = json.loads(resp.content)
    assert resp.status_code == 200
    assert payload["email"] == "a@b.com"
    assert payload["count"] == 2
    assert payload["enabled_count"] == 2               # auto-select-all
    assert {a["account_id"] for a in payload["accounts"]} == {"1", "2"}
    assert all(a["enabled"] for a in payload["accounts"])


@pytest.mark.django_db
def test_post_saves_the_enabled_subset(org, ds, monkeypatch):
    web.connector_accounts(_req("GET", org, monkeypatch), "google_ads")  # materialise
    resp = web.connector_accounts(
        _req("POST", org, monkeypatch, {"account_ids": ["1"]}), "google_ads")
    payload = json.loads(resp.content)
    assert resp.status_code == 200
    assert payload["enabled_count"] == 1

    from terno_dbi.connectors.api.auth import account_selection
    assert account_selection.enabled_account_ids(ds) == {"1"}


@pytest.mark.django_db
def test_post_rejects_non_list_body(org, ds, monkeypatch):
    resp = web.connector_accounts(
        _req("POST", org, monkeypatch, {"account_ids": "1"}), "google_ads")
    assert resp.status_code == 400


@pytest.mark.django_db
def test_not_connected_is_404(org, monkeypatch):
    # No DataSource for this connector in the org.
    resp = web.connector_accounts(_req("GET", org, monkeypatch), "google_ads")
    assert resp.status_code == 404


@pytest.mark.django_db
def test_post_rejects_invalid_json(org, ds, monkeypatch):
    from terno_dbi.connectors.api import web
    rf = RequestFactory()
    request = rf.post("/connectors/api/google_ads/accounts/",
                      data="{not json", content_type="application/json")
    request.user = org.owner
    request.org_id = org.id
    monkeypatch.setattr(web.registry, "build_connector", lambda ds: _FakeConnector())
    resp = web.connector_accounts(request, "google_ads")
    assert resp.status_code == 400


@pytest.mark.django_db
def test_get_surfaces_connector_error(org, ds, monkeypatch):
    from terno_dbi.connectors.api import web
    from terno_dbi.connectors.api.model.errors import ApiError, ErrorCode

    class _Broken:
        def list_accounts(self):
            raise ApiError(ErrorCode.AUTH_EXPIRED, "reconnect")

    request = _req("GET", org, monkeypatch)
    monkeypatch.setattr(web.registry, "build_connector", lambda ds: _Broken())
    resp = web.connector_accounts(request, "google_ads")
    assert resp.status_code == 400
    assert "reconnect" in json.loads(resp.content)["error"]


@pytest.mark.django_db
def test_get_tolerates_undecryptable_email(org, monkeypatch):
    # A connection whose bundle cannot be decrypted still lists accounts, just
    # with no email (the _connected_email guard returns "").
    from terno_dbi.connectors.api import web
    from terno_dbi.core.models import ConnectorCatalog, DataSource

    cat, _ = ConnectorCatalog.objects.get_or_create(
        key="google_ads",
        defaults=dict(display_name="Google Ads", family="api", auth_type="oauth"))
    DataSource.objects.create(
        display_name="Google Ads", type="google_ads", connection_str="",
        organisation=org, catalog=cat, connection_json="not-encrypted",
        auth_status=DataSource.AuthStatus.CONNECTED)
    resp = web.connector_accounts(_req("GET", org, monkeypatch), "google_ads")
    assert resp.status_code == 200
    assert json.loads(resp.content)["email"] == ""


@pytest.mark.django_db
def test_non_admin_is_forbidden(org, ds, monkeypatch):
    from django.contrib.auth.models import User
    from terno_dbi.core.models import OrganisationUser

    member = User.objects.create_user("ca-member", "m@example.com", "pw")
    OrganisationUser.objects.create(user=member, organisation=org)
    request = _req("GET", org, monkeypatch)
    request.user = member                              # member, not admin/owner
    resp = web.connector_accounts(request, "google_ads")
    assert resp.status_code == 403
