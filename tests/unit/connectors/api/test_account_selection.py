"""Unit tests for per-connection account selection (auto-select-all + hard-restrict)."""

from __future__ import annotations
from dataclasses import dataclass

import pytest

from terno_dbi.connectors.api.auth import account_selection as sel


@dataclass
class _Acct:
    id: str
    name: str = ""


def _ds(org):
    from terno_dbi.core.models import DataSource
    return DataSource.objects.create(
        display_name=f"ds-{org.subdomain}", type="google_ads",
        connection_str="", organisation=org,
    )


@pytest.fixture
def org(db):
    from django.contrib.auth import get_user_model
    from terno_dbi.core.models import CoreOrganisation
    User = get_user_model()
    owner = User.objects.create(username="sel-owner")
    return CoreOrganisation.objects.create(
        name="Acme", subdomain="sel-acme", owner=owner)


@pytest.mark.django_db
def test_first_sync_enables_every_account(org):
    ds = _ds(org)
    rows = sel.sync_account_selections(ds, [_Acct("1", "Beta"), _Acct("2", "Alpha")])
    assert [r["account_id"] for r in rows] == ["2", "1"]      # sorted by name
    assert all(r["enabled"] for r in rows)
    assert sel.enabled_account_ids(ds) == {"1", "2"}


@pytest.mark.django_db
def test_no_selection_rows_reads_as_unrestricted(org):
    ds = _ds(org)
    assert sel.enabled_account_ids(ds) is None               # never listed


@pytest.mark.django_db
def test_deselection_survives_a_refresh(org):
    ds = _ds(org)
    sel.sync_account_selections(ds, [_Acct("1", "A"), _Acct("2", "B")])
    sel.set_enabled_accounts(ds, ["1"])                       # user turns 2 off
    assert sel.enabled_account_ids(ds) == {"1"}
    # A later refresh returns both accounts again; the "off" choice must stick.
    sel.sync_account_selections(ds, [_Acct("1", "A"), _Acct("2", "B")])
    assert sel.enabled_account_ids(ds) == {"1"}


@pytest.mark.django_db
def test_refresh_updates_a_changed_account_name(org):
    ds = _ds(org)
    sel.sync_account_selections(ds, [_Acct("1", "Old Name")])
    rows = sel.sync_account_selections(ds, [_Acct("1", "New Name")])
    assert rows[0]["account_name"] == "New Name"     # cached name refreshed


@pytest.mark.django_db
def test_new_account_on_refresh_is_enabled_and_revoked_is_pruned(org):
    ds = _ds(org)
    sel.sync_account_selections(ds, [_Acct("1", "A")])
    # 1 disappears (access revoked), 2 appears.
    rows = sel.sync_account_selections(ds, [_Acct("2", "B")])
    assert {r["account_id"] for r in rows} == {"2"}
    assert sel.enabled_account_ids(ds) == {"2"}


@pytest.mark.django_db
def test_all_disabled_denies_everything(org):
    ds = _ds(org)
    sel.sync_account_selections(ds, [_Acct("1", "A")])
    sel.set_enabled_accounts(ds, [])                          # turn all off
    assert sel.enabled_account_ids(ds) == set()              # empty != None


@pytest.mark.django_db
def test_set_enabled_ignores_unknown_ids(org):
    ds = _ds(org)
    sel.sync_account_selections(ds, [_Acct("1", "A")])
    count = sel.set_enabled_accounts(ds, ["1", "999"])        # 999 has no row
    assert count == 1
    assert sel.enabled_account_ids(ds) == {"1"}


@pytest.mark.django_db
def test_restrict_intersects_rbac_and_selection(org):
    ds = _ds(org)
    sel.sync_account_selections(ds, [_Acct("1"), _Acct("2"), _Acct("3")])
    sel.set_enabled_accounts(ds, ["1", "2"])
    # RBAC permits {2,3}; enabled is {1,2} -> only 2 is queryable.
    assert sel.restrict_to_selection(ds, {"2", "3"}) == {"2"}
    # No RBAC allowlist (None) -> restricted to the enabled set.
    assert sel.restrict_to_selection(ds, None) == {"1", "2"}


@pytest.mark.django_db
def test_restrict_is_noop_without_a_selection(org):
    ds = _ds(org)
    assert sel.restrict_to_selection(ds, {"7"}) == {"7"}      # selection None
    assert sel.restrict_to_selection(ds, None) is None


def test_connected_email_reads_id_token_claim():
    import base64 as _b64, json as _json
    from terno_dbi.connectors.api.auth.oauth import _connected_email

    def _jwt(claims):
        body = _b64.urlsafe_b64encode(_json.dumps(claims).encode()).rstrip(b"=").decode()
        return f"header.{body}.sig"

    assert _connected_email({"id_token": _jwt({"email": "a@b.com"})}) == "a@b.com"
    assert _connected_email({"id_token": "not-a-jwt"}) == ""
    assert _connected_email({}) == ""                         # no id_token
