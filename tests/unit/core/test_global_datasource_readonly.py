"""Global (shared demo) datasources are read-only through the service API: a
token from one org must not rename, describe, sync or delete them for all.
"""
import hashlib
import json

import pytest

from terno_dbi.core.models import (
    CoreOrganisation, DataSource, ServiceToken, Table, TableColumn,
)


def _token(org, token_type, key, scopes=None, datasources=()):
    t = ServiceToken.objects.create(
        name=f"{token_type} token",
        token_type=token_type,
        key_prefix=key[:10],
        key_hash=hashlib.sha256(key.encode()).hexdigest(),
        is_active=True,
        organisation=org,
        scopes=scopes,
    )
    if datasources:
        t.datasources.set(datasources)
    return t


def _auth(key):
    return {"HTTP_AUTHORIZATION": f"Bearer {key}"}


@pytest.fixture
def org(db):
    from django.contrib.auth.models import User
    owner = User.objects.create_user("guard-owner", "g@example.com", "pw")
    return CoreOrganisation.objects.create(name="Guard", subdomain="guard", owner=owner)


@pytest.fixture
def db_ds(org):
    ds = DataSource.objects.create(
        display_name="warehouse", type="postgres",
        connection_str="postgresql://localhost/w", organisation=org,
    )
    t = Table.objects.create(name="orders", public_name="orders", data_source=ds)
    TableColumn.objects.create(name="id", public_name="id", table=t, data_type="integer")
    return ds


@pytest.fixture
def global_ds(db):
    ds = DataSource.objects.create(
        display_name="demo", type="postgres",
        connection_str="postgresql://localhost/demo", is_global=True,
    )
    t = Table.objects.create(name="customers", public_name="customers", data_source=ds)
    TableColumn.objects.create(name="email", public_name="email", table=t, data_type="text")
    return ds


ADMIN_SCOPES = ["admin:read", "admin:write", "admin:sync", "query:read", "query:execute"]


# --- 1. global datasources are read-only ------------------------------------

@pytest.mark.django_db
class TestGlobalDatasourceIsReadOnly:
    @pytest.fixture
    def key(self, org, global_ds):
        key = "dbi_admin_globalguard"
        _token(org, ServiceToken.TokenType.ADMIN, key, ADMIN_SCOPES, [global_ds])
        return key

    def test_delete_refused_and_row_kept(self, client, key, global_ds):
        r = client.delete(f"/api/admin/datasources/{global_ds.id}/delete/", **_auth(key))
        assert r.status_code == 403
        assert "read-only" in r.json()["error"]
        assert DataSource.objects.filter(id=global_ds.id).exists()

    def test_update_datasource_refused(self, client, key, global_ds):
        r = client.patch(f"/api/admin/datasources/{global_ds.id}/",
                         data=json.dumps({"description": "x"}),
                         content_type="application/json", **_auth(key))
        assert r.status_code == 403
        global_ds.refresh_from_db()
        assert global_ds.description != "x"

    def test_sync_refused(self, client, key, global_ds):
        r = client.post(f"/api/admin/datasources/{global_ds.id}/sync/",
                        data="{}", content_type="application/json", **_auth(key))
        assert r.status_code == 403

    def test_rename_table_refused(self, client, key, global_ds):
        table = global_ds.table_set.get()
        r = client.patch(f"/api/admin/tables/{table.id}/",
                         data=json.dumps({"public_name": "renamed"}),
                         content_type="application/json", **_auth(key))
        assert r.status_code == 403
        table.refresh_from_db()
        assert table.public_name == "customers"

    def test_rename_column_refused(self, client, key, global_ds):
        column = TableColumn.objects.get(table__data_source=global_ds)
        r = client.patch(f"/api/admin/columns/{column.id}/",
                         data=json.dumps({"public_name": "renamed"}),
                         content_type="application/json", **_auth(key))
        assert r.status_code == 403
        column.refresh_from_db()
        assert column.public_name == "email"

    def test_reads_still_allowed(self, client, key, global_ds):
        r = client.get(f"/api/query/datasources/{global_ds.id}/tables/", **_auth(key))
        assert r.status_code == 200

    def test_own_datasource_still_deletable(self, client, org, db_ds):
        key = "dbi_admin_owndelete"
        _token(org, ServiceToken.TokenType.ADMIN, key, ADMIN_SCOPES)
        r = client.delete(f"/api/admin/datasources/{db_ds.id}/delete/", **_auth(key))
        assert r.status_code == 200
        assert not DataSource.objects.filter(id=db_ds.id).exists()



# --- deleting an API connector disconnects it instead ----------------------

@pytest.mark.django_db
def test_delete_api_connector_soft_disconnects(client, org):
    from terno_dbi.core.models import ConnectorCatalog
    from terno_dbi.services.secrets import encrypt_dict

    cat, _ = ConnectorCatalog.objects.get_or_create(
        key="google_ads",
        defaults=dict(display_name="Google Ads", family="api", auth_type="oauth"),
    )
    ds = DataSource.objects.create(
        display_name="Google Ads", type="google_ads", connection_str="",
        organisation=org, catalog=cat,
        connection_json=encrypt_dict({"ACCESS_TOKEN": "x"}),
        auth_status=DataSource.AuthStatus.CONNECTED,
    )
    key = "dbi_admin_softdelete"
    _token(org, ServiceToken.TokenType.ADMIN, key, ADMIN_SCOPES)

    r = client.delete(f"/api/admin/datasources/{ds.id}/delete/", **_auth(key))

    assert r.status_code == 200
    assert "disconnected" in r.json()["message"]
    ds.refresh_from_db()  # row kept, so its memory and selections survive
    assert ds.auth_status == DataSource.AuthStatus.NOT_AUTHENTICATED
    assert (ds.decrypted_connection_json or {}) == {}
