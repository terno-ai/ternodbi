"""The connector-management service layer (`management_service`).

These test the framework-agnostic functions DIRECTLY — the same functions the
browser views and the desktop connector proxy both call. The point is to lock
the contract both callers depend on: the org-admin gate on the mutating ops, the
typed errors, and the returned shapes — without going through a request/response.
"""

import pytest

from terno_dbi.connectors.api import management_service as svc
from terno_dbi.connectors.api.model.types import Account


class _FakeConnector:
    def list_accounts(self):
        return [Account(id="1", name="Alpha"), Account(id="2", name="Beta")]


@pytest.fixture
def owner(db):
    from django.contrib.auth.models import User
    return User.objects.create_user("ms-owner", "o@example.com", "pw")


@pytest.fixture
def org(owner):
    from terno_dbi.core.models import CoreOrganisation, OrganisationUser
    org = CoreOrganisation.objects.create(name="Acme", subdomain="ms-acme", owner=owner)
    OrganisationUser.objects.create(user=owner, organisation=org)
    return org


@pytest.fixture
def member(org):
    """A plain org member — not owner, not admin."""
    from django.contrib.auth.models import User
    from terno_dbi.core.models import OrganisationUser
    u = User.objects.create_user("ms-member", "m@example.com", "pw")
    OrganisationUser.objects.create(user=u, organisation=org)
    return u


@pytest.fixture
def admin_member(org):
    """A member whose group in this org holds the admin permissions."""
    from django.contrib.auth.models import User
    from tests.unit._org_roles import make_org_admin
    u = User.objects.create_user("ms-admin", "a@example.com", "pw")
    make_org_admin(u, org)
    return u


@pytest.fixture
def catalog(db):
    from terno_dbi.core.models import ConnectorCatalog
    cat, _ = ConnectorCatalog.objects.get_or_create(
        key="google_ads",
        defaults=dict(display_name="Google Ads", family="api", auth_type="oauth"),
    )
    return cat


@pytest.fixture
def ds(org, catalog):
    from terno_dbi.core.models import DataSource
    from terno_dbi.services.secrets import encrypt_dict
    return DataSource.objects.create(
        display_name="Google Ads", type="google_ads", connection_str="",
        organisation=org, catalog=catalog,
        connection_json=encrypt_dict({"ACCESS_TOKEN": "x", "CONNECTED_EMAIL": "a@b.com"}),
        auth_status=DataSource.AuthStatus.CONNECTED,
    )


# --- is_org_admin -------------------------------------------------------------

@pytest.mark.django_db
def test_is_org_admin_owner(owner, org):
    assert svc.is_org_admin(owner, org) is True


@pytest.mark.django_db
def test_is_org_admin_group_member(admin_member, org):
    assert svc.is_org_admin(admin_member, org) is True


@pytest.mark.django_db
def test_is_org_admin_superuser(org):
    from django.contrib.auth.models import User
    su = User.objects.create_superuser("ms-su", "su@example.com", "pw")
    assert svc.is_org_admin(su, org) is True


@pytest.mark.django_db
def test_is_org_admin_plain_member_false(member, org):
    assert svc.is_org_admin(member, org) is False


@pytest.mark.django_db
def test_is_org_admin_ignores_the_global_org_admin_group(member, org):
    """Only groups on the membership count; the old global group grants nothing."""
    from django.contrib.auth.models import Group
    member.groups.add(Group.objects.get_or_create(name="Org Admin")[0])
    assert svc.is_org_admin(member, org) is False


@pytest.mark.django_db
def test_is_org_admin_is_per_organisation(admin_member, org, owner):
    """Admin in one org is a plain member in another."""
    from terno_dbi.core.models import CoreOrganisation, OrganisationUser
    other = CoreOrganisation.objects.create(name="Globex", subdomain="ms-globex", owner=owner)
    OrganisationUser.objects.create(user=admin_member, organisation=other)
    assert svc.is_org_admin(admin_member, org) is True
    assert svc.is_org_admin(admin_member, other) is False


@pytest.mark.django_db
def test_is_org_admin_needs_add_datasource(member, org):
    """A group in the org without `core.add_datasource` does not make an admin."""
    from terno_dbi.core.models import OrganisationUser
    from tests.unit._org_roles import admin_group
    writer = admin_group(org, name="Writers", perms=("core.use_dbi_admin_mode",))
    OrganisationUser.objects.get(user=member, organisation=org).groups.add(writer)
    assert svc.is_org_admin(member, org) is False


@pytest.mark.django_db
def test_is_org_admin_inactive_user_false(admin_member, org):
    admin_member.is_active = False
    admin_member.save()
    assert svc.is_org_admin(admin_member, org) is False


# --- list_connectors ----------------------------------------------------------

@pytest.mark.django_db
def test_list_connectors_can_manage_for_admin(owner, org, catalog):
    out = svc.list_connectors(owner, org)
    assert out["can_manage"] is True
    assert any(c["key"] == "google_ads" for c in out["connectors"])


@pytest.mark.django_db
def test_list_connectors_cannot_manage_for_member(member, org, catalog):
    out = svc.list_connectors(member, org)
    assert out["can_manage"] is False
    # every member still sees the catalog of connectors.
    assert any(c["key"] == "google_ads" for c in out["connectors"])


@pytest.mark.django_db
def test_list_connectors_reports_connected_state(owner, org, ds):
    out = svc.list_connectors(owner, org)
    card = next(c for c in out["connectors"] if c["key"] == "google_ads")
    assert card["status"] == "connected"
    assert card["connected_email"] == "a@b.com"
    assert card["datasource_id"] == ds.id


# --- disconnect_connector -----------------------------------------------------

@pytest.mark.django_db
def test_disconnect_clears_tokens_but_keeps_row(owner, org, ds):
    from terno_dbi.core.models import DataSource
    out = svc.disconnect_connector(owner, org, "google_ads")
    assert out == {"status": "disconnected", "key": "google_ads"}
    ds.refresh_from_db()
    assert ds.auth_status == DataSource.AuthStatus.NOT_AUTHENTICATED
    # connection_json is an encrypted field; the cleared value decrypts to {}.
    assert (ds.decrypted_connection_json or {}) == {}
    # row is kept (not deleted) on purpose.
    assert DataSource.objects.filter(id=ds.id).exists()


@pytest.mark.django_db
def test_disconnect_non_admin_denied(member, org, ds):
    with pytest.raises(svc.ConnectorPermissionDenied):
        svc.disconnect_connector(member, org, "google_ads")


@pytest.mark.django_db
def test_disconnect_not_connected_raises(owner, org, catalog):
    with pytest.raises(svc.ConnectorNotConnected):
        svc.disconnect_connector(owner, org, "google_ads")


# --- list_accounts ------------------------------------------------------------

@pytest.mark.django_db
def test_list_accounts_returns_rows_and_email(owner, org, ds, monkeypatch):
    monkeypatch.setattr(svc.registry, "build_connector", lambda d: _FakeConnector())
    out = svc.list_accounts(owner, org, "google_ads")
    assert out["count"] == 2
    assert out["email"] == "a@b.com"
    assert {r["account_id"] for r in out["accounts"]} == {"1", "2"}


@pytest.mark.django_db
def test_list_accounts_allowlist_uses_the_callers_groups_in_this_org(
        admin_member, org, ds, monkeypatch):
    """The account allowlist is matched against the caller's groups in the
    datasource's organisation, not their global groups."""
    from django.contrib.auth.models import Group
    from terno_dbi.core.models import GroupAccountAllowlist, OrganisationUser
    monkeypatch.setattr(svc.registry, "build_connector", lambda d: _FakeConnector())

    in_org = Group.objects.create(name=f"{org.pk}:Analysts")
    OrganisationUser.objects.get(user=admin_member, organisation=org).groups.add(in_org)
    global_only = Group.objects.create(name="global-analysts")
    admin_member.groups.add(global_only)
    GroupAccountAllowlist.objects.create(group=in_org, data_source=ds, account_id="1")
    GroupAccountAllowlist.objects.create(group=global_only, data_source=ds, account_id="2")

    out = svc.list_accounts(admin_member, org, "google_ads")
    assert {r["account_id"] for r in out["accounts"]} == {"1"}


@pytest.mark.django_db
def test_list_accounts_non_admin_denied(member, org, ds, monkeypatch):
    monkeypatch.setattr(svc.registry, "build_connector", lambda d: _FakeConnector())
    with pytest.raises(svc.ConnectorPermissionDenied):
        svc.list_accounts(member, org, "google_ads")


@pytest.mark.django_db
def test_list_accounts_not_connected_raises(owner, org, catalog):
    with pytest.raises(svc.ConnectorNotConnected):
        svc.list_accounts(owner, org, "google_ads")


@pytest.mark.django_db
def test_list_accounts_provider_error_becomes_bad_request(owner, org, ds, monkeypatch):
    from terno_dbi.connectors.api.model.errors import ApiError, ErrorCode

    class _Broken:
        def list_accounts(self):
            raise ApiError(ErrorCode.UPSTREAM_ERROR, "boom")

    monkeypatch.setattr(svc.registry, "build_connector", lambda d: _Broken())
    with pytest.raises(svc.ConnectorBadRequest):
        svc.list_accounts(owner, org, "google_ads")


# --- save_accounts ------------------------------------------------------------

@pytest.mark.django_db
def test_save_accounts_whole_set(owner, org, ds, monkeypatch):
    monkeypatch.setattr(svc.registry, "build_connector", lambda d: _FakeConnector())
    svc.list_accounts(owner, org, "google_ads")  # materialise selections
    out = svc.save_accounts(owner, org, "google_ads", {"account_ids": ["1"]})
    assert out["status"] == "saved"
    assert out["enabled_count"] == 1


@pytest.mark.django_db
def test_save_accounts_delta_shape(owner, org, ds, monkeypatch):
    monkeypatch.setattr(svc.registry, "build_connector", lambda d: _FakeConnector())
    svc.list_accounts(owner, org, "google_ads")
    out = svc.save_accounts(owner, org, "google_ads",
                            {"enabled_deltas": {"1": False}})
    assert out["status"] == "saved"
    assert "enabled_count" in out


@pytest.mark.django_db
def test_save_accounts_non_admin_denied(member, org, ds):
    with pytest.raises(svc.ConnectorPermissionDenied):
        svc.save_accounts(member, org, "google_ads", {"account_ids": ["1"]})


@pytest.mark.django_db
def test_save_accounts_not_connected_raises(owner, org, catalog):
    with pytest.raises(svc.ConnectorNotConnected):
        svc.save_accounts(owner, org, "google_ads", {"account_ids": ["1"]})


@pytest.mark.django_db
def test_save_accounts_rejects_non_object_deltas(owner, org, ds):
    with pytest.raises(svc.ConnectorBadRequest):
        svc.save_accounts(owner, org, "google_ads", {"enabled_deltas": ["nope"]})


@pytest.mark.django_db
def test_save_accounts_rejects_non_list_account_ids(owner, org, ds):
    with pytest.raises(svc.ConnectorBadRequest):
        svc.save_accounts(owner, org, "google_ads", {"account_ids": "nope"})


@pytest.mark.django_db
def test_save_accounts_sets_login_customer_id(owner, org, ds):
    out = svc.save_accounts(owner, org, "google_ads",
                            {"login_customer_id": "123-456-7890"})
    assert out["login_customer_id"] == "1234567890"


# --- set_connector_enabled ----------------------------------------------------

@pytest.mark.django_db
def test_set_connector_enabled_toggles_flag(owner, org, ds):
    from terno_dbi.core.models import DataSource
    out = svc.set_connector_enabled(owner, org, "google_ads", False)
    assert out["enabled"] is False
    ds.refresh_from_db()
    assert ds.enabled is False
    out = svc.set_connector_enabled(owner, org, "google_ads", True)
    assert out["enabled"] is True
    ds.refresh_from_db()
    assert ds.enabled is True


@pytest.mark.django_db
def test_set_connector_enabled_non_admin_denied(member, org, ds):
    with pytest.raises(svc.ConnectorPermissionDenied):
        svc.set_connector_enabled(member, org, "google_ads", False)


@pytest.mark.django_db
def test_set_connector_enabled_not_connected_raises(owner, org, catalog):
    with pytest.raises(svc.ConnectorNotConnected):
        svc.set_connector_enabled(owner, org, "google_ads", False)
