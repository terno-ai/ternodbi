"""Account-level RBAC resolver (§7, Phase 3)."""

import pytest
from django.contrib.auth.models import Group, User

from terno_dbi.catalog.refresh import refresh_catalog
from terno_dbi.connectors.api.auth import rbac
from terno_dbi.connectors.api.model.types import Account
from terno_dbi.core.models import (
    ConnectorCatalog,
    CoreOrganisation,
    DataSource,
    GroupAccountAllowlist,
)


@pytest.fixture
def datasource(db):
    user = User.objects.create_user("rbac", "r@x.com", "pw")
    org = CoreOrganisation.objects.create(name="Acme", subdomain="acme", owner=user)
    refresh_catalog()
    return DataSource.objects.create(
        display_name="GA4", type="googleanalytics4", connection_str="",
        organisation=org,
        catalog=ConnectorCatalog.objects.get(key="googleanalytics4"),
    )


@pytest.mark.django_db
class TestResolver:
    def test_unconfigured_source_is_unrestricted(self, datasource):
        # No allowlist rows at all -> None (usable right after connecting).
        assert rbac.permitted_accounts(datasource, []) is None

    def test_configured_source_restricts_to_group_rows(self, datasource):
        g = Group.objects.create(name="analysts")
        GroupAccountAllowlist.objects.create(
            group=g, data_source=datasource, account_id="111")
        GroupAccountAllowlist.objects.create(
            group=g, data_source=datasource, account_id="222")
        assert rbac.permitted_accounts(datasource, [g]) == {"111", "222"}

    def test_configured_source_denies_a_group_with_no_rows(self, datasource):
        # The datasource is configured (another group has rows), but this
        # caller's group has none -> empty set -> deny all. Never "all".
        other = Group.objects.create(name="others")
        caller = Group.objects.create(name="caller")
        GroupAccountAllowlist.objects.create(
            group=other, data_source=datasource, account_id="999")
        assert rbac.permitted_accounts(datasource, [caller]) == set()

    def test_union_across_multiple_groups(self, datasource):
        g1 = Group.objects.create(name="g1")
        g2 = Group.objects.create(name="g2")
        GroupAccountAllowlist.objects.create(
            group=g1, data_source=datasource, account_id="1")
        GroupAccountAllowlist.objects.create(
            group=g2, data_source=datasource, account_id="2")
        assert rbac.permitted_accounts(datasource, [g1, g2]) == {"1", "2"}


class TestFilterAccounts:
    def test_none_passes_everything(self):
        accounts = [Account("1", "A"), Account("2", "B")]
        assert rbac.filter_accounts(accounts, None) == accounts

    def test_only_permitted_accounts_survive(self):
        accounts = [Account("1", "A"), Account("2", "B"), Account("3", "C")]
        kept = rbac.filter_accounts(accounts, {"1", "3"})
        assert {a.id for a in kept} == {"1", "3"}

    def test_empty_permitted_hides_everything(self):
        accounts = [Account("1", "A")]
        assert rbac.filter_accounts(accounts, set()) == []
