"""The in-process transport, used when MCP runs inside the Django process.

The point of these tests is that the in-process path must enforce *exactly* what
the HTTP path enforces. It skips the socket, not the permission checks — and a
drift here is a permission bug, not a performance regression.
"""

import pytest

from terno_dbi.client import TernoDBIClient
from terno_dbi.transport import (
    HttpTransport,
    InProcessTransport,
    build_transport,
    set_default_transport_factory,
)


def test_http_is_the_default():
    """stdio and every out-of-process caller must be unaffected."""
    client = TernoDBIClient(base_url="http://example.test", api_key="dbi_query_x")
    assert isinstance(client._http, HttpTransport)


def test_default_factory_is_overridable_and_restorable():
    import terno_dbi.transport as transport_module

    original = transport_module._default_transport_factory
    try:
        set_default_transport_factory(InProcessTransport)
        assert isinstance(build_transport("k"), InProcessTransport)
    finally:
        transport_module._default_transport_factory = original
    assert isinstance(build_transport("k"), HttpTransport)


def test_transport_is_built_per_client_not_shared():
    """A shared transport instance would carry one caller's API key into
    another's request — the cross-tenant bleed mcp/context.py exists to stop."""
    import terno_dbi.transport as transport_module

    original = transport_module._default_transport_factory
    try:
        set_default_transport_factory(InProcessTransport)
        a = TernoDBIClient(base_url="http://x", api_key="key-a")
        b = TernoDBIClient(base_url="http://x", api_key="key-b")
        assert a._http is not b._http
        assert a._http.api_key == "key-a"
        assert b._http.api_key == "key-b"
    finally:
        transport_module._default_transport_factory = original


@pytest.fixture
def in_process_org(db):
    from django.contrib.auth.models import Group, User

    from terno_dbi.core.models import (
        CoreOrganisation,
        DataSource,
        OrganisationUser,
        Table,
    )
    from terno_dbi.oauth.minting import (
        generate_oauth_access_token,
        mint_service_token_for_key,
    )

    owner = User.objects.create(username="tp-owner")
    org = CoreOrganisation.objects.create(name="Acme", subdomain="tp-acme", owner=owner)
    other = CoreOrganisation.objects.create(name="Globex", subdomain="tp-globex", owner=owner)

    user = User.objects.create(username="tp-ada")
    membership = OrganisationUser.objects.create(user=user, organisation=org)
    membership.groups.set([Group.objects.create(name="Org Admin")])

    ds = DataSource.objects.create(
        display_name="acme-db", type="sqlite",
        connection_str="sqlite:///:memory:", organisation=org,
    )
    DataSource.objects.create(
        display_name="globex-db", type="sqlite",
        connection_str="sqlite:///:memory:", organisation=other,
    )
    table = Table.objects.create(data_source=ds, name="orders", public_name="orders")

    write_key = generate_oauth_access_token()
    mint_service_token_for_key(
        write_key, user, org, ["query:read", "query:execute", "admin:write"]
    )
    read_key = generate_oauth_access_token()
    mint_service_token_for_key(read_key, user, org, ["query:read"])

    def client_for(key):
        client = TernoDBIClient(base_url="http://in-process", api_key=key)
        client._http = InProcessTransport(api_key=key)
        return client

    return {
        "client_for": client_for, "write_key": write_key, "read_key": read_key,
        "table": table, "org": org,
    }


@pytest.mark.django_db
def test_in_process_reaches_the_real_view(in_process_org):
    client = in_process_org["client_for"](in_process_org["write_key"])
    names = [d["name"] for d in client.list_datasources()]
    assert "acme-db" in names


@pytest.mark.django_db
def test_org_scoping_holds_in_process(in_process_org):
    """The failure this would represent is a cross-tenant leak, not a bug."""
    client = in_process_org["client_for"](in_process_org["write_key"])
    names = [d["name"] for d in client.list_datasources()]
    assert "globex-db" not in names


@pytest.mark.django_db
def test_scope_enforcement_holds_in_process(in_process_org):
    """Asserted against a table that genuinely exists — otherwise a 404 from the
    decorator would make this pass without the scope check ever running."""
    table_id = in_process_org["table"].id

    writer = in_process_org["client_for"](in_process_org["write_key"])
    assert writer.update_table(table_id, public_name="orders_v2")["status"] == "success"

    reader = in_process_org["client_for"](in_process_org["read_key"])
    with pytest.raises(Exception, match="Insufficient scope"):
        reader.update_table(table_id, public_name="nope")


@pytest.mark.django_db
def test_invalid_key_is_rejected_in_process(in_process_org):
    client = in_process_org["client_for"]("dbi_oauth_not_a_real_key")
    with pytest.raises(Exception, match="Invalid or expired"):
        client.list_datasources()


@pytest.mark.django_db
def test_unknown_route_returns_404_not_an_exception(in_process_org):
    response = InProcessTransport(api_key=in_process_org["write_key"]).get(
        "http://x/api/nope/"
    )
    assert response.status_code == 404
    assert "No route" in response.json()["error"]


@pytest.mark.django_db
def test_response_shim_exposes_what_the_client_uses(in_process_org):
    response = InProcessTransport(api_key=in_process_org["write_key"]).get(
        "http://x/api/query/datasources/"
    )
    assert response.status_code == 200
    assert isinstance(response.json(), (dict, list))
    assert response.request.method == "GET"
    response.raise_for_status()  # must not raise on 2xx
