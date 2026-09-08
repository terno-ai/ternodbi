"""Phase 0 acceptance: `list_datasources` returns connected *and* available.

Listing sources nobody has connected is what lets an agent say "you don't have
Meta Ads yet, here's the link" rather than being unable to mention it. The other
half of the contract is that the existing `datasources` key keeps its shape, so
callers written before the union are unaffected.
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
from terno_dbi.core.query_service.views import list_datasources


@pytest.fixture
def user(db):
    from django.contrib.auth.models import User

    return User.objects.create_user("listuser", "l@example.com", "pw")


@pytest.fixture
def org(db, user):
    return CoreOrganisation.objects.create(
        name="Acme", subdomain="acme", owner=user,
    )


@pytest.fixture
def catalog(db):
    refresh_catalog()
    return ConnectorCatalog.objects


@pytest.fixture
def postgres_ds(db, catalog, org):
    return DataSource.objects.create(
        display_name="Warehouse",
        type="postgres",
        connection_str="postgresql://u:p@h:5432/d",
        organisation=org,
        catalog=ConnectorCatalog.objects.get(key="postgres"),
    )


def _token_for(user, datasources, org=None):
    key = "dbi_query_listdatasourcestoken"
    token = ServiceToken.objects.create(
        name="Query Token",
        token_type=ServiceToken.TokenType.QUERY,
        key_prefix="dbi_query_",
        key_hash=hashlib.sha256(key.encode()).hexdigest(),
        is_active=True,
        created_by=user,
        organisation=org,
    )
    for ds in datasources:
        token.datasources.add(ds)
    return token


def _call(token, settings=None):
    request = RequestFactory().get("/api/query/datasources/")
    request.service_token = token
    request.allowed_datasources = token.get_accessible_datasources()
    request.token_organisation = token.organisation
    response = list_datasources(request)
    return json.loads(response.content)


@pytest.mark.django_db
class TestConnectedBlock:
    def test_existing_shape_is_preserved(self, user, postgres_ds, settings):
        # Callers predating the union read `datasources`; renaming it would
        # break them for no gain.
        settings.MAIN_DOMAIN = "app.terno.ai"
        payload = _call(_token_for(user, [postgres_ds], postgres_ds.organisation))

        assert payload["status"] == "success"
        assert payload["count"] == 1
        entry = payload["datasources"][0]
        for legacy_key in (
            "id", "name", "type", "description", "is_erp",
            "dialect_name", "dialect_version",
        ):
            assert legacy_key in entry
        assert entry["name"] == "Warehouse"

    def test_carries_family_and_key(self, user, postgres_ds, settings):
        settings.MAIN_DOMAIN = "app.terno.ai"
        payload = _call(_token_for(user, [postgres_ds], postgres_ds.organisation))
        entry = payload["datasources"][0]
        assert entry["family"] == "database"
        assert entry["key"] == "postgres"

    def test_healthy_source_carries_no_reconnect_url(
        self, user, postgres_ds, settings
    ):
        settings.MAIN_DOMAIN = "app.terno.ai"
        payload = _call(_token_for(user, [postgres_ds], postgres_ds.organisation))
        assert "reconnect_url" not in payload["datasources"][0]

    def test_expired_source_carries_a_reconnect_url(
        self, user, postgres_ds, settings
    ):
        settings.MAIN_DOMAIN = "app.terno.ai"
        postgres_ds.auth_status = DataSource.AuthStatus.EXPIRED
        postgres_ds.auth_error = "refresh token revoked"
        postgres_ds.save()

        payload = _call(_token_for(user, [postgres_ds], postgres_ds.organisation))
        entry = payload["datasources"][0]

        assert entry["reconnect_url"].startswith("https://acme.app.terno.ai/connect")
        assert entry["auth_error"] == "refresh token revoked"
        assert any("reconnect" in n.lower() for n in payload["notes"])


@pytest.mark.django_db
class TestAvailableBlock:
    def test_lists_enabled_but_unconnected_sources(
        self, user, postgres_ds, settings
    ):
        settings.MAIN_DOMAIN = "app.terno.ai"
        payload = _call(_token_for(user, [postgres_ds], postgres_ds.organisation))

        keys = {e["key"] for e in payload["available"]}
        assert "googleanalytics4" in keys      # enabled: already verified
        assert payload["available_count"] == len(payload["available"])

    def test_disabled_sources_are_never_offered(
        self, user, postgres_ds, settings
    ):
        settings.MAIN_DOMAIN = "app.terno.ai"
        payload = _call(_token_for(user, [postgres_ds], postgres_ds.organisation))

        keys = {e["key"] for e in payload["available"]}
        # Approvals still pending — offering these would produce a connect link
        # that cannot complete.
        assert "meta_ads" not in keys
        assert "google_ads" not in keys
        assert "youtube" not in keys
        assert "generic" not in keys

    def test_connect_url_shape_follows_auth_type(
        self, user, postgres_ds, settings
    ):
        settings.MAIN_DOMAIN = "app.terno.ai"
        payload = _call(_token_for(user, [postgres_ds], postgres_ds.organisation))
        by_key = {e["key"]: e for e in payload["available"]}

        # Stateless and unsigned: the URL names what to connect, the session
        # names who. Nothing here is a credential.
        assert by_key["googleanalytics4"]["connect_url"] == (
            "https://acme.app.terno.ai/connect?connector=googleanalytics4"
        )
        assert by_key["mysql"]["connect_url"] == (
            "https://acme.app.terno.ai/connect?connector=mysql"
        )

    def test_connected_api_source_is_not_re_offered(
        self, user, postgres_ds, org, settings
    ):
        settings.MAIN_DOMAIN = "app.terno.ai"
        ga4 = DataSource.objects.create(
            display_name="Our GA4", type="googleanalytics4",
            connection_str="", organisation=org,
            catalog=ConnectorCatalog.objects.get(key="googleanalytics4"),
            auth_status=DataSource.AuthStatus.CONNECTED,
        )
        payload = _call(_token_for(user, [postgres_ds, ga4], org))

        keys = {e["key"] for e in payload["available"]}
        assert "googleanalytics4" not in keys

    def test_disconnected_api_source_leaves_connected_and_is_re_offered(
        self, user, postgres_ds, org, settings
    ):
        # After Disconnect the row is kept (memory/history preserved) but marked
        # not-authenticated. It must drop out of `connected` (so the agent won't
        # query it) and reappear under `available` as a reconnect.
        settings.MAIN_DOMAIN = "app.terno.ai"
        ga4 = DataSource.objects.create(
            display_name="Our GA4", type="googleanalytics4",
            connection_str="", organisation=org,
            catalog=ConnectorCatalog.objects.get(key="googleanalytics4"),
            auth_status=DataSource.AuthStatus.NOT_AUTHENTICATED,
        )
        payload = _call(_token_for(user, [postgres_ds, ga4], org))

        connected_types = {e["type"] for e in payload["datasources"]}
        assert "googleanalytics4" not in connected_types      # left the queryable set
        assert "googleanalytics4" in {e["key"] for e in payload["available"]}  # offered again

    def test_databases_stay_offered_after_one_is_connected(
        self, user, postgres_ds, settings
    ):
        # An organisation may legitimately hold several Postgres connections,
        # unlike an API source where a second connection means re-authorising.
        settings.MAIN_DOMAIN = "app.terno.ai"
        payload = _call(_token_for(user, [postgres_ds], postgres_ds.organisation))
        assert "postgres" in {e["key"] for e in payload["available"]}

    def test_no_connect_url_without_a_root_domain(
        self, user, postgres_ds, settings
    ):
        # A dead link is worse than prose: the user clicks it and gives up.
        settings.MAIN_DOMAIN = ""
        settings.TERNO_ROOT_DOMAIN = ""
        payload = _call(_token_for(user, [postgres_ds], postgres_ds.organisation))
        assert all("connect_url" not in e for e in payload["available"])


@pytest.mark.django_db
class TestNotes:
    def test_available_sources_produce_a_steering_note(
        self, user, postgres_ds, settings
    ):
        settings.MAIN_DOMAIN = "app.terno.ai"
        payload = _call(_token_for(user, [postgres_ds], postgres_ds.organisation))
        joined = " ".join(payload["notes"]).lower()
        assert "connect_url" in joined
        # The credential must never be requested in conversation.
        assert "password" in joined or "connection string" in joined

    def test_api_source_note_points_at_data_query(
        self, user, postgres_ds, org, settings
    ):
        settings.MAIN_DOMAIN = "app.terno.ai"
        ga4 = DataSource.objects.create(
            display_name="Our GA4", type="googleanalytics4",
            connection_str="", organisation=org,
            catalog=ConnectorCatalog.objects.get(key="googleanalytics4"),
        )
        payload = _call(_token_for(user, [postgres_ds, ga4], org))
        joined = " ".join(payload["notes"])
        assert "data_query" in joined
        assert "execute_query" in joined
