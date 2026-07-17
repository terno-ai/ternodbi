"""
Regression tests for the role-resolution security fix.

Before this fix, `_resolve_roles` accepted a client-supplied `roles` value
(from a GET query param on list_tables, or a `roles` key in the JSON body of
execute_query/stream_query/export_query) and resolved it via a raw
`Group.objects.filter(id__in=role_ids)` — with NO check that the calling
token actually belonged to that group.

This was exploitable two ways, both reproduced here against the real
production functions (not reimplementations):

1. Visibility expansion: `get_all_group_tables`/`get_all_group_columns` UNION
   group-granted tables/columns onto the base visible set. Naming any group
   with a GroupTableSelector/GroupColumnSelector grant — including a group
   the caller was never added to — exposed tables/columns that PrivateTable
   Selector/PrivateColumnSelector had deliberately hidden.

2. Row-filter bypass: naming a group not in the token's own membership
   REPLACED the resolved role set rather than adding to it, so the caller's
   own mandatory GroupTableRowFilter never matched `group__in=roles` and was
   silently dropped — the row-level-security restriction simply didn't apply.

The fix: `_resolve_roles(request)` now takes no client-facing role_ids at
all and always returns `request.service_token.groups.all()`. No first-party
caller (client.py, either MCP server, terno-ai) has ever sent a `roles`
value, so this changes no legitimate behaviour — only closes the bypass.
"""
import pytest
from django.contrib.auth.models import User, Group
from django.test import RequestFactory

from terno_dbi.core.models import (
    CoreOrganisation, DataSource, Table, TableColumn,
    PrivateTableSelector, GroupTableSelector,
    PrivateColumnSelector, GroupColumnSelector,
    GroupTableRowFilter, ServiceToken,
)
from terno_dbi.core.query_service import views
from terno_dbi.core.query_service.views import _resolve_roles
from terno_dbi.services.access import get_admin_config_object
from terno_dbi.services.shield import _get_grp_filters


@pytest.fixture
def org(db):
    owner = User.objects.create(username="owner_rls")
    return CoreOrganisation.objects.create(name="Acme RLS", subdomain="acme-rls", owner=owner)


@pytest.fixture
def datasource(db, org):
    return DataSource.objects.create(
        display_name="RLS-DS", type="postgres", organisation=org,
        connection_str="postgresql://localhost/rlstest", enabled=True,
    )


@pytest.fixture
def schema(db, datasource):
    """A public table + a deliberately private table/column, plus a group
    that has been explicitly re-granted access to both."""
    public_tbl = Table.objects.create(data_source=datasource, name="public_orders", public_name="Orders")
    secret_tbl = Table.objects.create(data_source=datasource, name="secret_salaries", public_name="Salaries")
    col_amount = TableColumn.objects.create(table=public_tbl, name="amount", data_type="int")
    col_ssn = TableColumn.objects.create(table=public_tbl, name="ssn", data_type="text")

    pts = PrivateTableSelector.objects.create(data_source=datasource)
    pts.tables.set([secret_tbl])
    pcs = PrivateColumnSelector.objects.create(data_source=datasource)
    pcs.columns.set([col_ssn])

    priv_group = Group.objects.create(name="HR-Privileged")
    gts = GroupTableSelector.objects.create(group=priv_group)
    gts.tables.set([secret_tbl])
    gcs = GroupColumnSelector.objects.create(group=priv_group)
    gcs.columns.set([col_ssn])

    eu_group = Group.objects.create(name="EU-Analyst")
    GroupTableRowFilter.objects.create(
        data_source=datasource, table=public_tbl, group=eu_group, filter_str="region = 'EU'"
    )

    return {
        "public_tbl": public_tbl, "secret_tbl": secret_tbl,
        "col_amount": col_amount, "col_ssn": col_ssn,
        "priv_group": priv_group, "eu_group": eu_group,
    }


@pytest.fixture
def attacker_token(db, org, schema):
    """Owns EU-Analyst only. NOT a member of HR-Privileged."""
    token = ServiceToken.objects.create(
        name="attacker", token_type=ServiceToken.TokenType.QUERY,
        key_hash="attacker-hash", key_prefix="dbi_query_",
        organisation=org, is_active=True,
    )
    token.groups.set([schema["eu_group"]])
    return token


@pytest.fixture
def hr_token(db, org, schema):
    """Legitimately owns HR-Privileged — the control case that must still work."""
    token = ServiceToken.objects.create(
        name="hr-legit", token_type=ServiceToken.TokenType.QUERY,
        key_hash="hr-hash", key_prefix="dbi_query_",
        organisation=org, is_active=True,
    )
    token.groups.set([schema["priv_group"]])
    return token


@pytest.mark.django_db
class TestResolveRolesIgnoresClientInput:
    """_resolve_roles must depend ONLY on the token's own group membership."""

    def test_returns_tokens_own_groups(self, attacker_token, schema):
        request = RequestFactory().get("/")
        request.service_token = attacker_token

        roles = _resolve_roles(request)

        assert list(roles) == [schema["eu_group"]]

    def test_no_longer_accepts_a_role_ids_argument(self, attacker_token):
        """The client-facing override parameter must not exist at all —
        this is the actual fix, not just an unused parameter."""
        import inspect
        sig = inspect.signature(_resolve_roles)
        assert list(sig.parameters) == ["request"], (
            "_resolve_roles must take only `request` — any second parameter "
            "re-introduces a client-controllable role override"
        )

    def test_no_token_returns_no_roles(self):
        request = RequestFactory().get("/")
        roles = _resolve_roles(request)
        assert list(roles) == []


@pytest.mark.django_db
class TestVisibilityExpansionBlocked:
    """A group re-grant (GroupTableSelector/GroupColumnSelector) must only
    apply to callers who actually belong to that group."""

    def test_attacker_cannot_see_private_table_or_column(self, attacker_token, datasource, schema):
        request = RequestFactory().get("/")
        request.service_token = attacker_token
        roles = _resolve_roles(request)

        tables, columns = get_admin_config_object(datasource, roles)
        table_names = set(tables.values_list("name", flat=True))
        column_names = set(columns.values_list("name", flat=True))

        assert "secret_salaries" not in table_names
        assert "ssn" not in column_names
        assert "public_orders" in table_names
        assert "amount" in column_names

    def test_legit_hr_token_still_sees_granted_table_and_column(self, hr_token, datasource, schema):
        """Regression guard: the fix must not break real, legitimate grants."""
        request = RequestFactory().get("/")
        request.service_token = hr_token
        roles = _resolve_roles(request)

        tables, columns = get_admin_config_object(datasource, roles)
        table_names = set(tables.values_list("name", flat=True))
        column_names = set(columns.values_list("name", flat=True))

        assert "secret_salaries" in table_names
        assert "ssn" in column_names


@pytest.mark.django_db
class TestRowFilterBypassBlocked:
    """A caller must never be able to make their own mandatory
    GroupTableRowFilter silently stop applying."""

    def test_attackers_own_row_filter_is_enforced(self, attacker_token, datasource, schema):
        request = RequestFactory().get("/")
        request.service_token = attacker_token
        roles = _resolve_roles(request)

        filters = _get_grp_filters(datasource, roles)

        assert filters.get("public_orders") == ["(region = 'EU')"]

    def test_token_with_no_groups_gets_no_group_filter(self, org, datasource, schema):
        """Sanity check: a token with zero group membership gets zero
        group-sourced filters (not an error, just nothing to enforce)."""
        bare_token = ServiceToken.objects.create(
            name="bare", token_type=ServiceToken.TokenType.QUERY,
            key_hash="bare-hash", key_prefix="dbi_query_",
            organisation=org, is_active=True,
        )
        request = RequestFactory().get("/")
        request.service_token = bare_token
        roles = _resolve_roles(request)

        filters = _get_grp_filters(datasource, roles)
        assert filters.get("public_orders") is None


@pytest.mark.django_db
class TestHttpLevelRoleOverrideIsInert:
    """End-to-end: even a raw ?roles= override in a live request must not
    change what the attacker's own token is allowed to see."""

    def test_roles_query_param_cannot_expand_visibility(self, attacker_token, datasource, schema):
        request = RequestFactory().get(
            f"/api/query/datasources/{datasource.id}/tables/",
            {"roles": str(schema["priv_group"].id)},
        )
        request.service_token = attacker_token

        response = views.list_tables(request, datasource_identifier=str(datasource.id))

        assert response.status_code == 200
        import json
        body = json.loads(response.content)
        # list_tables returns each table's public_name under "name"
        names = {t["name"] for t in body["tables"]}
        assert "Salaries" not in names, (
            "attacker supplied ?roles=<HR-Privileged id> in the URL and must "
            "not see the private table it grants access to"
        )
        assert "Orders" in names
