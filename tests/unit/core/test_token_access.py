"""
Comprehensive tests for Token-Based Access Control.

This module tests the hybrid access control system where tokens can be scoped to:
1. Explicit datasource links (highest priority)
2. Organisation scope (all DS in org)
3. Supertoken (configurable - access all or deny all)
"""

import pytest
from django.test import RequestFactory, override_settings
from django.http import JsonResponse
from django.contrib.auth.models import User

from terno_dbi.core.models import (
    CoreOrganisation, DataSource, ServiceToken, Table, TableColumn
)
from terno_dbi.decorators import require_service_auth


@pytest.fixture
def test_user(db):
    """Create a test user."""
    return User.objects.create_user(
        username='testuser',
        password='testpass',
        email='test@example.com'
    )


@pytest.fixture
def org1(db, test_user):
    """Create test organisation 1."""
    return CoreOrganisation.objects.create(
        name="Organisation 1",
        subdomain="org1",
        owner=test_user
    )


@pytest.fixture
def org2(db, test_user):
    """Create test organisation 2."""
    return CoreOrganisation.objects.create(
        name="Organisation 2",
        subdomain="org2",
        owner=test_user
    )


@pytest.fixture
def ds1_org1(db, org1):
    """Create datasource 1 in org1."""
    return DataSource.objects.create(
        display_name="DS1 Org1",
        type="postgresql",
        connection_str="postgresql://localhost/ds1",
        organisation=org1,
        enabled=True
    )


@pytest.fixture
def ds2_org1(db, org1):
    """Create datasource 2 in org1."""
    return DataSource.objects.create(
        display_name="DS2 Org1",
        type="postgresql",
        connection_str="postgresql://localhost/ds2",
        organisation=org1,
        enabled=True
    )


@pytest.fixture
def ds_disabled_org1(db, org1):
    """Create disabled datasource in org1."""
    return DataSource.objects.create(
        display_name="DS Disabled Org1",
        type="postgresql",
        connection_str="postgresql://localhost/ds_disabled",
        organisation=org1,
        enabled=False
    )


@pytest.fixture
def ds1_org2(db, org2):
    """Create datasource 1 in org2."""
    return DataSource.objects.create(
        display_name="DS1 Org2",
        type="postgresql",
        connection_str="postgresql://localhost/ds1_org2",
        organisation=org2,
        enabled=True
    )


@pytest.fixture
def ds_no_org(db):
    """Create datasource without org (legacy)."""
    return DataSource.objects.create(
        display_name="DS No Org",
        type="postgresql",
        connection_str="postgresql://localhost/ds_no_org",
        organisation=None,
        enabled=True
    )


@pytest.fixture
def table1(db, ds1_org1):
    """Create test table in ds1_org1."""
    return Table.objects.create(
        name="table1",
        public_name="Table 1",
        data_source=ds1_org1
    )


@pytest.fixture
def column1(db, table1):
    """Create test column in table1."""
    return TableColumn.objects.create(
        name="column1",
        public_name="Column 1",
        table=table1,
        data_type="varchar"
    )


@pytest.fixture
def request_factory():
    """Create request factory."""
    return RequestFactory()


# =============================================================================
# Test: Organisation-scoped token
# =============================================================================

@pytest.mark.django_db
class TestOrgScopedToken:
    """Tests for organisation-scoped tokens."""

    def test_sees_only_org_datasources(self, org1, ds1_org1, ds2_org1, ds_disabled_org1, ds1_org2, ds_no_org):
        """Token with org scope should only see enabled datasources in that org."""
        token = ServiceToken.objects.create(
            name="Org1 Token",
            key_hash="testhash1",
            key_prefix="dbi_query_",
            organisation=org1
        )

        accessible = token.get_accessible_datasources()
        accessible_ids = list(accessible.values_list('id', flat=True))

        # Should include enabled DS in org1
        assert ds1_org1.id in accessible_ids
        assert ds2_org1.id in accessible_ids

        # Should NOT include disabled DS
        assert ds_disabled_org1.id not in accessible_ids

        # Should NOT include DS from org2
        assert ds1_org2.id not in accessible_ids

        # Should NOT include DS without org
        assert ds_no_org.id not in accessible_ids

    def test_different_org_sees_only_their_ds(self, org2, ds1_org1, ds2_org1, ds1_org2):
        """Token scoped to org2 should only see org2 datasources."""
        token = ServiceToken.objects.create(
            name="Org2 Token",
            key_hash="testhash2",
            key_prefix="dbi_query_",
            organisation=org2
        )

        accessible = token.get_accessible_datasources()

        # Should only include DS from org2
        assert ds1_org2 in accessible
        assert accessible.count() == 1

        # Should NOT include DS from org1
        assert ds1_org1 not in accessible
        assert ds2_org1 not in accessible


# =============================================================================
# Test: Explicit datasource-scoped token
# =============================================================================

@pytest.mark.django_db
class TestExplicitDSScopedToken:
    """Tests for tokens with explicit datasource links."""

    def test_explicit_ds_overrides_org(self, org1, ds1_org1, ds2_org1):
        """Token with explicit DS links should ONLY see those DS, ignoring org."""
        token = ServiceToken.objects.create(
            name="Explicit DS Token",
            key_hash="testhash3",
            key_prefix="dbi_query_",
            organisation=org1  # Has org, but also has explicit DS links
        )
        token.datasources.add(ds1_org1)  # Only DS1, not DS2

        accessible = token.get_accessible_datasources()

        # Should ONLY include explicitly linked DS
        assert ds1_org1 in accessible
        assert accessible.count() == 1

        # Should NOT include DS2 even though it's in same org
        assert ds2_org1 not in accessible

    def test_cross_org_ds_links(self, ds1_org1, ds1_org2):
        """Token can have explicit DS links from multiple orgs."""
        token = ServiceToken.objects.create(
            name="Cross Org Token",
            key_hash="testhash4",
            key_prefix="dbi_query_"
        )
        token.datasources.add(ds1_org1, ds1_org2)

        accessible = token.get_accessible_datasources()

        # Should include both explicit DS from different orgs
        assert ds1_org1 in accessible
        assert ds1_org2 in accessible
        assert accessible.count() == 2

    def test_excludes_disabled_ds(self, ds1_org1, ds_disabled_org1):
        """Explicit DS links should still exclude disabled datasources."""
        token = ServiceToken.objects.create(
            name="Token with disabled DS",
            key_hash="testhash5",
            key_prefix="dbi_query_"
        )
        token.datasources.add(ds1_org1, ds_disabled_org1)

        accessible = token.get_accessible_datasources()

        # Should include enabled DS
        assert ds1_org1 in accessible

        # Should NOT include disabled DS even if explicitly linked
        assert ds_disabled_org1 not in accessible
        assert accessible.count() == 1


# =============================================================================
# Test: No scope token (supertoken behavior)
# =============================================================================

@pytest.mark.django_db
class TestNoScopeToken:
    """Tests for tokens without scope (supertoken behavior)."""

    def test_denied_by_default(self, ds1_org1, ds2_org1, ds1_org2, ds_no_org):
        """Token with no org and no DS links should be denied access by default."""
        token = ServiceToken.objects.create(
            name="No Scope Token",
            key_hash="testhash6",
            key_prefix="dbi_query_",
            organisation=None
        )

        accessible = token.get_accessible_datasources()

        # Should return empty queryset (secure default)
        assert accessible.count() == 0

    @override_settings(DBI_LAYER={'ALLOW_SUPERTOKEN': True})
    def test_allowed_when_supertoken_enabled(self, ds1_org1, ds2_org1, ds1_org2, ds_no_org, ds_disabled_org1):
        """Token with no scope can access all when ALLOW_SUPERTOKEN=True."""
        token = ServiceToken.objects.create(
            name="Supertoken",
            key_hash="testhash7",
            key_prefix="dbi_query_",
            organisation=None
        )

        accessible = token.get_accessible_datasources()
        accessible_ids = list(accessible.values_list('id', flat=True))

        # Should include all enabled datasources
        assert ds1_org1.id in accessible_ids
        assert ds2_org1.id in accessible_ids
        assert ds1_org2.id in accessible_ids
        assert ds_no_org.id in accessible_ids

        # Should still exclude disabled
        assert ds_disabled_org1.id not in accessible_ids


# =============================================================================
# Test: Helper methods
# =============================================================================

@pytest.mark.django_db
class TestHelperMethods:
    """Tests for ServiceToken helper methods."""

    def test_has_access_to_datasource(self, org1, ds1_org1, ds2_org1, ds1_org2):
        """Test has_access_to_datasource helper method."""
        token = ServiceToken.objects.create(
            name="Test Token",
            key_hash="testhash8",
            key_prefix="dbi_query_",
            organisation=org1
        )

        # Should have access to org1 datasources
        assert token.has_access_to_datasource(ds1_org1) is True
        assert token.has_access_to_datasource(ds2_org1) is True

        # Should NOT have access to org2 datasources
        assert token.has_access_to_datasource(ds1_org2) is False

    def test_has_access_to_table(self, org1, table1):
        """Test has_access_to_table helper method."""
        token = ServiceToken.objects.create(
            name="Test Token",
            key_hash="testhash9",
            key_prefix="dbi_query_",
            organisation=org1
        )

        # Should have access to table in org1 datasource
        assert token.has_access_to_table(table1) is True

    def test_has_access_to_column(self, org1, column1):
        """Test has_access_to_column helper method."""
        token = ServiceToken.objects.create(
            name="Test Token",
            key_hash="testhash10",
            key_prefix="dbi_query_",
            organisation=org1
        )

        # Should have access to column in org1 datasource
        assert token.has_access_to_column(column1) is True


# =============================================================================
# Test: Decorator functionality
# =============================================================================

@pytest.mark.django_db
class TestRequireServiceAuthDecorator:
    """Tests for the @require_service_auth() decorator."""

    def test_no_token_returns_401(self, request_factory):
        """Request without service_token should return 401."""
        @require_service_auth()
        def view(request):
            return JsonResponse({"status": "success"})

        request = request_factory.get('/test/')
        # Don't set request.service_token

        response = view(request)
        assert response.status_code == 401

    def test_admin_only_rejects_query_token(self, request_factory, org1):
        """ADMIN-only endpoint should reject QUERY token."""
        @require_service_auth(allowed_types=[ServiceToken.TokenType.ADMIN])
        def view(request):
            return JsonResponse({"status": "success"})

        token = ServiceToken.objects.create(
            name="Query Token",
            key_hash="test_query_hash",
            key_prefix="dbi_query_",
            token_type=ServiceToken.TokenType.QUERY,
            organisation=org1
        )

        request = request_factory.get('/test/')
        request.service_token = token
        response = view(request)

        assert response.status_code == 403

    def test_admin_only_accepts_admin_token(self, request_factory, org1):
        """ADMIN-only endpoint should accept ADMIN token."""
        @require_service_auth(allowed_types=[ServiceToken.TokenType.ADMIN])
        def view(request):
            return JsonResponse({"status": "success"})

        token = ServiceToken.objects.create(
            name="Admin Token",
            key_hash="test_admin_hash",
            key_prefix="dbi_admin_",
            token_type=ServiceToken.TokenType.ADMIN,
            organisation=org1
        )

        request = request_factory.get('/test/')
        request.service_token = token
        response = view(request)

        assert response.status_code == 200

    def test_authorized_datasource_access(self, request_factory, org1, ds1_org1):
        """Token should be able to access datasource in its org."""
        @require_service_auth()
        def view(request, datasource_identifier):
            return JsonResponse({
                "status": "success",
                "ds_id": request.resolved_datasource.id
            })

        token = ServiceToken.objects.create(
            name="Org Token",
            key_hash="test_org_hash",
            key_prefix="dbi_query_",
            organisation=org1
        )

        request = request_factory.get('/test/')
        request.service_token = token
        response = view(request, datasource_identifier=str(ds1_org1.id))

        assert response.status_code == 200

    def test_unauthorized_datasource_returns_403(self, request_factory, org1, ds_no_org):
        """Token should NOT be able to access datasource outside its org."""
        @require_service_auth()
        def view(request, datasource_identifier):
            return JsonResponse({"status": "success"})

        token = ServiceToken.objects.create(
            name="Org Token",
            key_hash="test_org_hash2",
            key_prefix="dbi_query_",
            organisation=org1
        )

        request = request_factory.get('/test/')
        request.service_token = token
        response = view(request, datasource_identifier=str(ds_no_org.id))

        assert response.status_code == 403

    def test_sets_allowed_datasources(self, request_factory, org1):
        """Decorator should set request.allowed_datasources."""
        result = {}

        @require_service_auth()
        def view(request):
            result['has_attr'] = hasattr(request, 'allowed_datasources')
            return JsonResponse({"status": "success"})

        token = ServiceToken.objects.create(
            name="Test Token",
            key_hash="test_attr_hash",
            key_prefix="dbi_query_",
            organisation=org1
        )

        request = request_factory.get('/test/')
        request.service_token = token
        view(request)

        assert result['has_attr'] is True

    def test_sets_token_organisation(self, request_factory, org1):
        """Decorator should set request.token_organisation."""
        result = {}

        @require_service_auth()
        def view(request):
            result['org'] = request.token_organisation
            return JsonResponse({"status": "success"})

        token = ServiceToken.objects.create(
            name="Test Token",
            key_hash="test_org_attr_hash",
            key_prefix="dbi_query_",
            organisation=org1
        )

        request = request_factory.get('/test/')
        request.service_token = token
        view(request)

        assert result['org'] == org1


# =============================================================================
# Test: Real-world scenarios
# =============================================================================

@pytest.mark.django_db
class TestAccessControlScenarios:
    """Integration-style tests for common access control scenarios."""

    def test_employee_full_org_access(self, test_user):
        """Employee gets full access to all datasources in their org."""
        acme_corp = CoreOrganisation.objects.create(
            name="ACME Corporation",
            subdomain="acme",
            owner=test_user
        )

        acme_prod = DataSource.objects.create(
            display_name="ACME Production",
            type="postgresql",
            connection_str="postgresql://localhost/acme_prod",
            organisation=acme_corp,
            enabled=True
        )
        acme_staging = DataSource.objects.create(
            display_name="ACME Staging",
            type="postgresql",
            connection_str="postgresql://localhost/acme_staging",
            organisation=acme_corp,
            enabled=True
        )

        token = ServiceToken.objects.create(
            name="ACME Employee Token",
            key_hash="employee_hash",
            key_prefix="dbi_query_",
            organisation=acme_corp
        )

        accessible = token.get_accessible_datasources()

        assert acme_prod in accessible
        assert acme_staging in accessible
        assert accessible.count() == 2

    def test_contractor_restricted_access(self, test_user):
        """External contractor gets access to only staging DB."""
        acme_corp = CoreOrganisation.objects.create(
            name="ACME Corporation",
            subdomain="acme2",
            owner=test_user
        )

        acme_prod = DataSource.objects.create(
            display_name="ACME Production",
            type="postgresql",
            connection_str="postgresql://localhost/acme_prod2",
            organisation=acme_corp,
            enabled=True
        )
        acme_staging = DataSource.objects.create(
            display_name="ACME Staging",
            type="postgresql",
            connection_str="postgresql://localhost/acme_staging2",
            organisation=acme_corp,
            enabled=True
        )

        token = ServiceToken.objects.create(
            name="Contractor Token",
            key_hash="contractor_hash",
            key_prefix="dbi_query_",
            organisation=acme_corp
        )
        token.datasources.add(acme_staging)  # Explicit override

        accessible = token.get_accessible_datasources()

        # Only staging, not production (despite same org)
        assert acme_staging in accessible
        assert acme_prod not in accessible
        assert accessible.count() == 1

    def test_new_datasource_auto_included(self, test_user):
        """When a new datasource is added to org, org-scoped tokens automatically have access."""
        acme_corp = CoreOrganisation.objects.create(
            name="ACME Corporation",
            subdomain="acme3",
            owner=test_user
        )

        DataSource.objects.create(
            display_name="ACME Production",
            type="postgresql",
            connection_str="postgresql://localhost/acme_prod3",
            organisation=acme_corp,
            enabled=True
        )

        token = ServiceToken.objects.create(
            name="ACME Employee Token",
            key_hash="employee_new_ds_hash",
            key_prefix="dbi_query_",
            organisation=acme_corp
        )

        # Before adding new DS
        assert token.get_accessible_datasources().count() == 1

        # Add new datasource to ACME
        new_ds = DataSource.objects.create(
            display_name="ACME Analytics",
            type="bigquery",
            connection_str="bigquery://project/dataset",
            organisation=acme_corp,
            enabled=True
        )

        # After adding new DS - should automatically be included
        accessible = token.get_accessible_datasources()
        assert new_ds in accessible
        assert accessible.count() == 2


# =============================================================================
# Test: REQUIRE_TOKEN_SCOPE decorator enforcement (MISSING COVERAGE)
# =============================================================================

@pytest.mark.django_db
class TestRequireTokenScopeEnforcement:
    """Tests for REQUIRE_TOKEN_SCOPE setting enforcement at decorator level."""

    @override_settings(DBI_LAYER={'REQUIRE_TOKEN_SCOPE': True, 'ALLOW_SUPERTOKEN': False})
    def test_no_scope_token_rejected_by_decorator(self, request_factory, db):
        """Token with no scope should be rejected by decorator when REQUIRE_TOKEN_SCOPE=True."""
        @require_service_auth()
        def view(request):
            return JsonResponse({"status": "success"})

        token = ServiceToken.objects.create(
            name="No Scope Token",
            key_hash="no_scope_decorator_hash",
            key_prefix="dbi_query_",
            organisation=None  # No org
        )
        # Token also has no explicit datasources

        request = request_factory.get('/test/')
        request.service_token = token
        response = view(request)

        # Should be rejected with 403 (not 401 - token is valid, just no scope)
        assert response.status_code == 403

    @override_settings(DBI_LAYER={'REQUIRE_TOKEN_SCOPE': False, 'ALLOW_SUPERTOKEN': False})
    def test_no_scope_token_allowed_when_require_scope_disabled(self, request_factory, db):
        """Token with no scope should be allowed when REQUIRE_TOKEN_SCOPE=False."""
        @require_service_auth()
        def view(request):
            return JsonResponse({"status": "success"})

        token = ServiceToken.objects.create(
            name="No Scope Token",
            key_hash="no_scope_allowed_hash",
            key_prefix="dbi_query_",
            organisation=None
        )

        request = request_factory.get('/test/')
        request.service_token = token
        response = view(request)

        # Should be allowed
        assert response.status_code == 200


# =============================================================================
# Test: Table-level decorator enforcement (MISSING COVERAGE)
# =============================================================================

@pytest.mark.django_db
class TestTableLevelEnforcement:
    """Tests for table-level access control in decorator."""

    def test_unauthorized_table_access_returns_403(self, request_factory, test_user):
        """Accessing a table from unauthorized datasource should return 403."""
        # Create org and DS that token DOES have access to
        org1 = CoreOrganisation.objects.create(
            name="Org1",
            subdomain="tabletest1",
            owner=test_user
        )

        # Create a different org and DS that token does NOT have access to
        org2 = CoreOrganisation.objects.create(
            name="Org2",
            subdomain="tabletest2",
            owner=test_user
        )
        other_ds = DataSource.objects.create(
            display_name="Other DS",
            type="postgresql",
            connection_str="postgresql://localhost/other",
            organisation=org2,
            enabled=True
        )
        other_table = Table.objects.create(
            name="other_table",
            public_name="Other Table",
            data_source=other_ds
        )

        @require_service_auth()
        def view(request, table_id):
            return JsonResponse({"status": "success"})

        # Token scoped to org1 (not org2)
        token = ServiceToken.objects.create(
            name="Org1 Token",
            key_hash="table_test_hash",
            key_prefix="dbi_query_",
            organisation=org1
        )

        request = request_factory.get('/test/')
        request.service_token = token
        response = view(request, table_id=other_table.id)

        # Should be 403 (not 404 - enumeration safe)
        assert response.status_code == 403

    def test_authorized_table_access_succeeds(self, request_factory, org1, ds1_org1, table1):
        """Accessing a table from authorized datasource should succeed."""
        @require_service_auth()
        def view(request, table_id):
            return JsonResponse({
                "status": "success",
                "resolved_table_id": request.resolved_table.id
            })

        token = ServiceToken.objects.create(
            name="Org1 Token",
            key_hash="table_access_hash",
            key_prefix="dbi_query_",
            organisation=org1
        )

        request = request_factory.get('/test/')
        request.service_token = token
        response = view(request, table_id=table1.id)

        assert response.status_code == 200


# =============================================================================
# Test: Column-level decorator enforcement (MISSING COVERAGE)
# =============================================================================

@pytest.mark.django_db
class TestColumnLevelEnforcement:
    """Tests for column-level access control in decorator."""

    def test_unauthorized_column_access_returns_403(self, request_factory, test_user):
        """Accessing a column from unauthorized datasource should return 403."""
        # Create org and DS that token DOES have access to
        org1 = CoreOrganisation.objects.create(
            name="Org1",
            subdomain="coltest1",
            owner=test_user
        )

        # Create a different org/DS/table/column that token does NOT have access to
        org2 = CoreOrganisation.objects.create(
            name="Org2",
            subdomain="coltest2",
            owner=test_user
        )
        other_ds = DataSource.objects.create(
            display_name="Other DS",
            type="postgresql",
            connection_str="postgresql://localhost/other_col",
            organisation=org2,
            enabled=True
        )
        other_table = Table.objects.create(
            name="other_table",
            public_name="Other Table",
            data_source=other_ds
        )
        other_column = TableColumn.objects.create(
            name="secret_column",
            public_name="Secret Column",
            table=other_table,
            data_type="varchar"
        )

        @require_service_auth()
        def view(request, column_id):
            return JsonResponse({"status": "success"})

        # Token scoped to org1 (not org2)
        token = ServiceToken.objects.create(
            name="Org1 Token",
            key_hash="column_test_hash",
            key_prefix="dbi_query_",
            organisation=org1
        )

        request = request_factory.get('/test/')
        request.service_token = token
        response = view(request, column_id=other_column.id)

        # Should be 403 (enumeration safe)
        assert response.status_code == 403

    def test_authorized_column_access_succeeds(self, request_factory, org1, column1):
        """Accessing a column from authorized datasource should succeed."""
        @require_service_auth()
        def view(request, column_id):
            return JsonResponse({
                "status": "success",
                "resolved_column_id": request.resolved_column.id
            })

        token = ServiceToken.objects.create(
            name="Org1 Token",
            key_hash="column_access_hash",
            key_prefix="dbi_query_",
            organisation=org1
        )

        request = request_factory.get('/test/')
        request.service_token = token
        response = view(request, column_id=column1.id)

        assert response.status_code == 200


# =============================================================================
# Test: Resolver failure paths (MISSING COVERAGE)
# =============================================================================

@pytest.mark.django_db
class TestResolverFailurePaths:
    """Tests for 404 behavior when resources don't exist."""

    def test_nonexistent_datasource_returns_404(self, request_factory, org1):
        """Requesting a non-existent datasource should return 404."""
        @require_service_auth()
        def view(request, datasource_identifier):
            return JsonResponse({"status": "success"})

        token = ServiceToken.objects.create(
            name="Org1 Token",
            key_hash="nonexist_ds_hash",
            key_prefix="dbi_query_",
            organisation=org1
        )

        request = request_factory.get('/test/')
        request.service_token = token
        response = view(request, datasource_identifier="99999")  # Non-existent ID

        # Should be 404 (resource not found)
        assert response.status_code == 404

    def test_nonexistent_table_returns_404(self, request_factory, org1):
        """Requesting a non-existent table should return 404."""
        @require_service_auth()
        def view(request, table_id):
            return JsonResponse({"status": "success"})

        token = ServiceToken.objects.create(
            name="Org1 Token",
            key_hash="nonexist_table_hash",
            key_prefix="dbi_query_",
            organisation=org1
        )

        request = request_factory.get('/test/')
        request.service_token = token
        response = view(request, table_id=99999)  # Non-existent ID

        # Should be 404 (resource not found)
        assert response.status_code == 404

    def test_nonexistent_column_returns_404(self, request_factory, org1):
        """Requesting a non-existent column should return 404."""
        @require_service_auth()
        def view(request, column_id):
            return JsonResponse({"status": "success"})

        token = ServiceToken.objects.create(
            name="Org1 Token",
            key_hash="nonexist_col_hash",
            key_prefix="dbi_query_",
            organisation=org1
        )

        request = request_factory.get('/test/')
        request.service_token = token
        response = view(request, column_id=99999)  # Non-existent ID

        # Should be 404 (resource not found)
        assert response.status_code == 404


# =============================================================================
# Test: Inactive token behavior (MISSING COVERAGE)
# =============================================================================

@pytest.mark.django_db
class TestInactiveTokenBehavior:
    """Tests for inactive/revoked token handling.

    Note: The is_active check is typically done in the middleware, 
    not the decorator. These tests verify the expected end-to-end behavior.
    """

    def test_inactive_token_still_resolves_datasources(self, org1, ds1_org1):
        """Inactive token's get_accessible_datasources still works.

        The authorization logic is separate from authentication.
        Middleware should reject inactive tokens before they reach views.
        """
        token = ServiceToken.objects.create(
            name="Inactive Token",
            key_hash="inactive_hash",
            key_prefix="dbi_query_",
            organisation=org1,
            is_active=False  # Revoked
        )

        # Token still has get_accessible_datasources capability
        # (middleware would block before this in real request)
        accessible = token.get_accessible_datasources()

        # Authorization logic still works - it's the middleware that blocks
        assert ds1_org1 in accessible


# =============================================================================
# Test: Token type + scope interaction (MISSING COVERAGE)
# =============================================================================

@pytest.mark.django_db
class TestTokenTypeScopeInteraction:
    """Tests for edge cases where token type and scope interact."""

    def test_admin_token_still_respects_scope(self, request_factory, test_user):
        """ADMIN token should still respect datasource scope (not bypass it)."""
        org1 = CoreOrganisation.objects.create(
            name="Org1",
            subdomain="adminscope1",
            owner=test_user
        )
        org2 = CoreOrganisation.objects.create(
            name="Org2",
            subdomain="adminscope2",
            owner=test_user
        )
        ds_org2 = DataSource.objects.create(
            display_name="DS Org2",
            type="postgresql",
            connection_str="postgresql://localhost/ds_org2",
            organisation=org2,
            enabled=True
        )

        @require_service_auth(allowed_types=[ServiceToken.TokenType.ADMIN])
        def view(request, datasource_identifier):
            return JsonResponse({"status": "success"})

        # ADMIN token scoped to org1
        token = ServiceToken.objects.create(
            name="Admin Token",
            key_hash="admin_scope_hash",
            key_prefix="dbi_admin_",
            token_type=ServiceToken.TokenType.ADMIN,
            organisation=org1
        )

        request = request_factory.get('/test/')
        request.service_token = token

        # Try to access DS in org2 - should fail even though it's ADMIN
        response = view(request, datasource_identifier=str(ds_org2.id))

        assert response.status_code == 403

    def test_admin_token_with_explicit_ds_can_access(self, request_factory, test_user):
        """ADMIN token with explicit DS link can access that DS."""
        org = CoreOrganisation.objects.create(
            name="Org",
            subdomain="adminds",
            owner=test_user
        )
        ds = DataSource.objects.create(
            display_name="DS",
            type="postgresql",
            connection_str="postgresql://localhost/ds_admin",
            organisation=org,
            enabled=True
        )

        @require_service_auth(allowed_types=[ServiceToken.TokenType.ADMIN])
        def view(request, datasource_identifier):
            return JsonResponse({
                "status": "success",
                "ds_id": request.resolved_datasource.id
            })

        # ADMIN token with explicit DS link
        token = ServiceToken.objects.create(
            name="Admin Token",
            key_hash="admin_explicit_ds_hash",
            key_prefix="dbi_admin_",
            token_type=ServiceToken.TokenType.ADMIN
        )
        token.datasources.add(ds)

        request = request_factory.get('/test/')
        request.service_token = token
        response = view(request, datasource_identifier=str(ds.id))

        assert response.status_code == 200

@pytest.mark.django_db
class TestServiceTokenVisibilityChecks:
    @pytest.fixture
    def setup_data(self, org1, ds1_org1):
        from terno_dbi.core.models import PrivateTableSelector, PrivateColumnSelector
        # Create a token
        token = ServiceToken.objects.create(
            name="Visibility Test Token",
            organisation=org1
        )

        # Create tables and columns
        table_public = Table.objects.create(name="public_table", public_name="Public", data_source=ds1_org1)
        table_private = Table.objects.create(name="private_table", public_name="Private", data_source=ds1_org1)

        col_public = TableColumn.objects.create(name="col_pub", table=table_public)
        col_private = TableColumn.objects.create(name="col_priv", table=table_public)

        # Mark as private
        pts = PrivateTableSelector.objects.create(data_source=ds1_org1)
        pts.tables.add(table_private)

        pcs = PrivateColumnSelector.objects.create(data_source=ds1_org1)
        pcs.columns.add(col_private)

        return {
            "token": token,
            "table_public": table_public,
            "table_private": table_private,
            "col_public": col_public,
            "col_private": col_private,
        }

    def test_has_access_to_table_hides_private(self, setup_data):
        token = setup_data["token"]
        assert token.has_access_to_table(setup_data["table_public"]) is True
        assert token.has_access_to_table(setup_data["table_private"]) is False

    def test_has_access_to_column_hides_private(self, setup_data):
        token = setup_data["token"]
        # Can access public column on public table
        assert token.has_access_to_column(setup_data["col_public"]) is True
        # Cannot access private column on public table
        assert token.has_access_to_column(setup_data["col_private"]) is False

        # Now try a column on a private table
        table_private = setup_data["table_private"]
        col_on_private_table = TableColumn.objects.create(name="col_on_priv", table=table_private)

        # Even if the column isn't in PrivateColumnSelector, the parent table is private
        assert token.has_access_to_column(col_on_private_table) is False
