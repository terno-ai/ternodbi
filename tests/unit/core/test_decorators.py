"""
Unit tests for decorators.py (require_service_auth decorator).

Tests authentication, authorization, and resource resolution.
"""
import pytest
from unittest.mock import MagicMock, patch
from django.http import JsonResponse
from django.test import RequestFactory

from terno_dbi.core.models import DataSource, Table, TableColumn, ServiceToken


@pytest.fixture
def request_factory():
    return RequestFactory()


@pytest.fixture
def datasource(db):
    return DataSource.objects.create(
        display_name='decorator_test_db',
        type='postgres',
        connection_str='postgresql://localhost/test',
        enabled=True
    )


@pytest.fixture
def table(datasource):
    return Table.objects.create(
        name='decorator_table',
        public_name='Decorator Table',
        data_source=datasource
    )


@pytest.fixture
def column(table):
    return TableColumn.objects.create(
        name='decorator_col',
        public_name='Decorator Column',
        table=table,
        data_type='varchar'
    )


@pytest.fixture
def mock_token(datasource):
    token = MagicMock(spec=ServiceToken)
    token.token_type = ServiceToken.TokenType.QUERY
    token.get_accessible_datasources.return_value = DataSource.objects.filter(id=datasource.id)
    token.datasources.exists.return_value = True
    token.organisation = None
    return token


@pytest.mark.django_db
class TestRequireServiceAuthNoToken:
    """Tests for requests without a service token."""

    def test_returns_401_when_no_token(self, request_factory):
        """Should return 401 when request has no service_token."""
        from terno_dbi.decorators import require_service_auth
        
        @require_service_auth()
        def test_view(request):
            return JsonResponse({"status": "ok"})
        
        request = request_factory.get('/test/')
        # No service_token attached
        
        response = test_view(request)
        
        assert response.status_code == 401
        assert 'Authentication required' in response.content.decode()


@pytest.mark.django_db
class TestRequireServiceAuthTokenType:
    """Tests for token type validation."""

    def test_returns_403_for_wrong_token_type(self, request_factory, mock_token):
        """Should return 403 when token type not in allowed_types."""
        from terno_dbi.decorators import require_service_auth
        
        mock_token.token_type = ServiceToken.TokenType.QUERY
        
        @require_service_auth(allowed_types=[ServiceToken.TokenType.ADMIN])
        def test_view(request):
            return JsonResponse({"status": "ok"})
        
        request = request_factory.get('/test/')
        request.service_token = mock_token
        
        response = test_view(request)
        
        assert response.status_code == 403
        assert 'Insufficient permissions' in response.content.decode()

    def test_allows_correct_token_type(self, request_factory, mock_token):
        """Should allow request when token type is in allowed_types."""
        from terno_dbi.decorators import require_service_auth
        
        mock_token.token_type = ServiceToken.TokenType.QUERY
        
        @require_service_auth(allowed_types=[ServiceToken.TokenType.QUERY])
        def test_view(request):
            return JsonResponse({"status": "ok"})
        
        request = request_factory.get('/test/')
        request.service_token = mock_token
        
        response = test_view(request)
        
        assert response.status_code == 200


@pytest.mark.django_db
class TestRequireServiceAuthDatasourceResolution:
    """Tests for datasource resolution."""

    def test_resolves_datasource_by_id(self, request_factory, mock_token, datasource):
        """Should resolve datasource from datasource_identifier kwarg."""
        from terno_dbi.decorators import require_service_auth
        
        @require_service_auth()
        def test_view(request, datasource_identifier):
            return JsonResponse({"ds_id": request.resolved_datasource.id})
        
        request = request_factory.get('/test/')
        request.service_token = mock_token
        
        response = test_view(request, datasource_identifier=datasource.id)
        
        assert response.status_code == 200
        assert hasattr(request, 'resolved_datasource')

    def test_returns_403_for_unauthorized_datasource(self, request_factory, mock_token, db):
        """Should return 403 when token doesn't have access to datasource."""
        from terno_dbi.decorators import require_service_auth
        
        # Create another datasource the token doesn't have access to
        other_ds = DataSource.objects.create(
            display_name='other_db',
            type='postgres',
            connection_str='postgresql://localhost/other'
        )
        
        @require_service_auth()
        def test_view(request, datasource_identifier):
            return JsonResponse({"status": "ok"})
        
        request = request_factory.get('/test/')
        request.service_token = mock_token
        
        response = test_view(request, datasource_identifier=other_ds.id)
        
        assert response.status_code == 403

    def test_returns_404_for_nonexistent_datasource(self, request_factory, mock_token):
        """Should return 404 when datasource doesn't exist."""
        from terno_dbi.decorators import require_service_auth
        
        @require_service_auth()
        def test_view(request, datasource_identifier):
            return JsonResponse({"status": "ok"})
        
        request = request_factory.get('/test/')
        request.service_token = mock_token
        
        response = test_view(request, datasource_identifier=99999)
        
        assert response.status_code == 404


@pytest.mark.django_db
class TestRequireServiceAuthTableResolution:
    """Tests for table resolution."""

    def test_resolves_table_by_id(self, request_factory, mock_token, table, datasource):
        """Should resolve table from table_id kwarg."""
        from terno_dbi.decorators import require_service_auth
        
        @require_service_auth()
        def test_view(request, table_id):
            return JsonResponse({"table_id": request.resolved_table.id})
        
        request = request_factory.get('/test/')
        request.service_token = mock_token
        
        response = test_view(request, table_id=table.id)
        
        assert response.status_code == 200
        assert hasattr(request, 'resolved_table')

    def test_returns_404_for_nonexistent_table(self, request_factory, mock_token):
        """Should return 404 when table doesn't exist."""
        from terno_dbi.decorators import require_service_auth
        
        @require_service_auth()
        def test_view(request, table_id):
            return JsonResponse({"status": "ok"})
        
        request = request_factory.get('/test/')
        request.service_token = mock_token
        
        response = test_view(request, table_id=99999)
        
        assert response.status_code == 404


@pytest.mark.django_db
class TestRequireServiceAuthColumnResolution:
    """Tests for column resolution."""

    def test_resolves_column_by_id(self, request_factory, mock_token, column, datasource):
        """Should resolve column from column_id kwarg."""
        from terno_dbi.decorators import require_service_auth
        
        @require_service_auth()
        def test_view(request, column_id):
            return JsonResponse({"column_id": request.resolved_column.id})
        
        request = request_factory.get('/test/')
        request.service_token = mock_token
        
        response = test_view(request, column_id=column.id)
        
        assert response.status_code == 200
        assert hasattr(request, 'resolved_column')

    def test_returns_404_for_nonexistent_column(self, request_factory, mock_token):
        """Should return 404 when column doesn't exist."""
        from terno_dbi.decorators import require_service_auth
        
        @require_service_auth()
        def test_view(request, column_id):
            return JsonResponse({"status": "ok"})
        
        request = request_factory.get('/test/')
        request.service_token = mock_token
        
        response = test_view(request, column_id=99999)
        
        assert response.status_code == 404


@pytest.mark.django_db
class TestRequireServiceAuthSupertoken:
    """Tests for supertoken and token scope requirements."""

    @patch('terno_dbi.core.conf.get')
    def test_rejects_scopeless_token_when_required(self, mock_conf, request_factory):
        """Should reject tokens without scope when REQUIRE_TOKEN_SCOPE is True."""
        from terno_dbi.decorators import require_service_auth
        
        def conf_get(key):
            if key == 'REQUIRE_TOKEN_SCOPE':
                return True
            if key == 'ALLOW_SUPERTOKEN':
                return False
            return None
        
        mock_conf.side_effect = conf_get
        
        token = MagicMock(spec=ServiceToken)
        token.token_type = ServiceToken.TokenType.QUERY
        token.get_accessible_datasources.return_value = DataSource.objects.none()
        token.datasources.exists.return_value = False
        token.organisation = None
        
        @require_service_auth()
        def test_view(request):
            return JsonResponse({"status": "ok"})
        
        request = RequestFactory().get('/test/')
        request.service_token = token
        
        response = test_view(request)
        
        assert response.status_code == 403
        assert 'no datasource or organisation scope' in response.content.decode()
