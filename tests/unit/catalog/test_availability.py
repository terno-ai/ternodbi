"""TERNO_DISABLED_CONNECTORS withholds connectors from a deployment.

Everything that offers a connector must agree: the connect link, the in-app
connectors list, and the catalogue the agent and MCP tools read. Sources an
organisation already connected keep working.
"""

import hashlib
import logging

import pytest
from django.test import RequestFactory

from terno_dbi.catalog.availability import disabled_connector_keys, offered_connectors
from terno_dbi.connectors.api import web

WITHHELD = "hubspot"
OFFERED = "google_ads"


@pytest.fixture
def org(db):
    from django.contrib.auth.models import User

    from terno_dbi.core.models import CoreOrganisation, OrganisationUser

    owner = User.objects.create_user("av-owner", "o@example.com", "pw")
    org = CoreOrganisation.objects.create(name="Avail", subdomain="avail", owner=owner)
    OrganisationUser.objects.create(user=owner, organisation=org)
    return org


@pytest.fixture
def catalog(db):
    """Both connectors exist and are enabled in the database."""
    from terno_dbi.core.models import ConnectorCatalog

    for key, name in ((WITHHELD, "HubSpot"), (OFFERED, "Google Ads")):
        row, _ = ConnectorCatalog.objects.get_or_create(
            key=key, defaults=dict(display_name=name, family="api", auth_type="oauth"))
        row.enabled = True
        row.save(update_fields=["enabled"])


@pytest.fixture
def withheld(settings):
    settings.TERNO_DISABLED_CONNECTORS = WITHHELD


def _auth(org, key="dbi_query_availtoken"):
    from terno_dbi.core.models import ServiceToken

    ServiceToken.objects.create(
        name="avail", token_type=ServiceToken.TokenType.QUERY, key_prefix=key[:10],
        key_hash=hashlib.sha256(key.encode()).hexdigest(), is_active=True,
        organisation=org, scopes=["query:read", "query:execute"],
    )
    return {"HTTP_AUTHORIZATION": f"Bearer {key}"}


# ------------------------------------------------------------ the setting

@pytest.mark.parametrize("value, expected", [
    ("hubspot,meta_ads", {"hubspot", "meta_ads"}),
    (" hubspot , meta_ads ,", {"hubspot", "meta_ads"}),
    (["hubspot", " meta_ads "], {"hubspot", "meta_ads"}),
    ("", set()),
])
def test_setting_is_parsed(settings, value, expected):
    settings.TERNO_DISABLED_CONNECTORS = value
    assert disabled_connector_keys() == expected


def test_environment_variable_is_the_fallback(settings, monkeypatch):
    if hasattr(settings, "TERNO_DISABLED_CONNECTORS"):
        del settings.TERNO_DISABLED_CONNECTORS
    monkeypatch.setenv("TERNO_DISABLED_CONNECTORS", "shopify")
    assert disabled_connector_keys() == {"shopify"}


def test_the_django_setting_wins_over_the_environment(settings, monkeypatch):
    monkeypatch.setenv("TERNO_DISABLED_CONNECTORS", "shopify")
    settings.TERNO_DISABLED_CONNECTORS = "hubspot"
    assert disabled_connector_keys() == {"hubspot"}


def test_nothing_is_withheld_by_default(settings, monkeypatch):
    if hasattr(settings, "TERNO_DISABLED_CONNECTORS"):
        del settings.TERNO_DISABLED_CONNECTORS
    monkeypatch.delenv("TERNO_DISABLED_CONNECTORS", raising=False)
    assert disabled_connector_keys() == frozenset()


def test_an_unknown_key_is_logged(settings, caplog):
    """A typo would silently offer the connector it meant to hide."""
    settings.TERNO_DISABLED_CONNECTORS = "hubspott-typo"
    with caplog.at_level(logging.WARNING, logger="terno_dbi.catalog.availability"):
        disabled_connector_keys()
    assert "hubspott-typo" in caplog.text


@pytest.mark.django_db
def test_offered_connectors_excludes_withheld_and_disabled(catalog, withheld):
    from terno_dbi.core.models import ConnectorCatalog

    ConnectorCatalog.objects.filter(key=OFFERED).update(enabled=False)
    keys = set(offered_connectors().values_list("key", flat=True))
    assert WITHHELD not in keys
    assert OFFERED not in keys

    ConnectorCatalog.objects.filter(key=OFFERED).update(enabled=True)
    assert OFFERED in set(offered_connectors().values_list("key", flat=True))


# ------------------------------------------------------- every surface agrees

@pytest.mark.django_db
def test_connect_refuses_a_withheld_connector(org, catalog, withheld, monkeypatch):
    monkeypatch.setattr(
        "terno_dbi.connectors.api.auth.oauth.start_authorization",
        lambda **kw: {"authorization_url": "https://provider/auth", "state": "s"},
    )

    def connect(key):
        request = RequestFactory().get(f"/connect?connector={key}")
        request.user, request.org_id = org.owner, org.id
        return web.connect(request)

    assert connect(WITHHELD).status_code == 400
    assert connect(OFFERED).status_code == 302


@pytest.mark.django_db
def test_in_app_connectors_list_omits_a_withheld_connector(org, catalog, withheld):
    from terno_dbi.connectors.api import management_service

    keys = {c["key"] for c in management_service.list_connectors(org.owner, org)["connectors"]}
    assert WITHHELD not in keys
    assert OFFERED in keys


@pytest.mark.django_db
def test_agent_and_mcp_catalogue_omit_a_withheld_connector(client, org, catalog, withheld):
    """`/api/query/connectors/` backs the agent's list_connectors and the MCP tool."""
    body = client.get("/api/query/connectors/", **_auth(org)).json()
    keys = {c["key"] for c in body["connectors"]}
    assert WITHHELD not in keys
    assert OFFERED in keys


@pytest.mark.django_db
def test_list_datasources_does_not_offer_a_withheld_connector(client, org, catalog, withheld):
    body = client.get("/api/query/datasources/", **_auth(org)).json()
    available = {a["key"] for a in body["available"]}
    assert WITHHELD not in available
    assert OFFERED in available


@pytest.mark.django_db
def test_an_already_connected_source_keeps_working(client, org, catalog, withheld):
    from terno_dbi.core.models import ConnectorCatalog, DataSource

    DataSource.objects.create(
        display_name="HubSpot", type="hubspot", connection_str="", organisation=org,
        catalog=ConnectorCatalog.objects.get(key=WITHHELD),
        auth_status=DataSource.AuthStatus.CONNECTED,
    )
    body = client.get("/api/query/datasources/", **_auth(org)).json()
    assert "HubSpot" in {d["name"] for d in body["datasources"]}
