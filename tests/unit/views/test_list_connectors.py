"""`list_connectors`: the catalogue an agent uses to recommend and connect a
source, with a one-click connect link per connector.

The contract worth pinning: every enabled connector appears with its
connection `status`, its `auth_type` (oauth vs manual), and a `connect_url`;
`can_connect` reflects the caller's admin scope.
"""

import hashlib
import json

import pytest
from django.test import RequestFactory

from terno_dbi.catalog.refresh import refresh_catalog
from terno_dbi.core.models import (
    ConnectorCatalog,
    CoreOrganisation,
    DataSource,
    ServiceToken,
)
from terno_dbi.core.query_service.views import list_connectors


@pytest.fixture
def user(db):
    from django.contrib.auth.models import User

    return User.objects.create_user("connuser", "c@example.com", "pw")


@pytest.fixture
def org(db, user):
    return CoreOrganisation.objects.create(name="Acme", subdomain="acme", owner=user)


@pytest.fixture
def catalog(db):
    refresh_catalog()


def _token(user, org, scopes):
    key = "dbi_query_listconnectorstoken_" + "_".join(scopes)
    return ServiceToken.objects.create(
        name="Query Token",
        token_type=ServiceToken.TokenType.QUERY,
        key_prefix="dbi_query_",
        key_hash=hashlib.sha256(key.encode()).hexdigest(),
        is_active=True,
        created_by=user,
        organisation=org,
        scopes=scopes,
    )


def _call(token, settings):
    settings.MAIN_DOMAIN = "app.terno.ai"
    request = RequestFactory().get("/api/query/connectors/")
    request.service_token = token
    request.token_organisation = token.organisation
    return json.loads(list_connectors(request).content)


@pytest.mark.django_db
class TestListConnectors:
    def test_lists_every_enabled_connector_with_status_and_url(
        self, user, org, catalog, settings
    ):
        payload = _call(_token(user, org, ["query:read"]), settings)
        assert payload["status"] == "success"
        by_key = {c["key"]: c for c in payload["connectors"]}

        # A representative OAuth connector and a manual one.
        ga4 = by_key["googleanalytics4"]
        assert ga4["auth_type"] == "oauth"
        assert ga4["family"] == "api"
        assert ga4["status"] == "not_connected"
        assert ga4["connect_url"] == (
            "https://acme.app.terno.ai/connect?connector=googleanalytics4"
        )

        pg = by_key["postgres"]
        assert pg["auth_type"] == "manual"
        assert pg["family"] == "database"
        # Manual connectors link to the credentials modal, not /connect.
        assert pg["connect_url"].endswith("/data-connectors/datasource/postgres")

    def test_disabled_connectors_are_omitted(self, user, org, catalog, settings):
        # meta_ads ships disabled (approval pending) — it must not be offered.
        payload = _call(_token(user, org, ["query:read"]), settings)
        keys = {c["key"] for c in payload["connectors"]}
        assert "meta_ads" not in keys

    def test_connected_source_shows_status_and_id(
        self, user, org, catalog, settings
    ):
        ds = DataSource.objects.create(
            display_name="Our GA4", type="googleanalytics4", connection_str="",
            organisation=org,
            catalog=ConnectorCatalog.objects.get(key="googleanalytics4"),
            auth_status=DataSource.AuthStatus.CONNECTED,
        )
        payload = _call(_token(user, org, ["query:read"]), settings)
        ga4 = next(c for c in payload["connectors"] if c["key"] == "googleanalytics4")
        assert ga4["status"] == "connected"
        assert ga4["datasource_id"] == ds.id

    def test_can_connect_reflects_admin_scope(self, user, org, catalog, settings):
        assert _call(_token(user, org, ["query:read"]), settings)["can_connect"] is False
        assert _call(_token(user, org, ["admin:write"]), settings)["can_connect"] is True

    def test_notes_warn_non_admins(self, user, org, catalog, settings):
        payload = _call(_token(user, org, ["query:read"]), settings)
        assert any("admin" in n.lower() for n in payload["notes"])
