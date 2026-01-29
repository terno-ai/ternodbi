"""
Unit tests for ServiceTokenMiddleware.

Tests the authentication middleware that validates Bearer tokens
on all /api/ endpoints except health and info.
"""
import pytest
from unittest.mock import Mock, patch, MagicMock
from django.http import JsonResponse
from django.test import RequestFactory

from terno_dbi.middleware import ServiceTokenMiddleware


@pytest.fixture
def request_factory():
    return RequestFactory()


@pytest.fixture
def mock_get_response():
    """Mock the get_response callable."""
    response = Mock()
    response.status_code = 200
    return Mock(return_value=response)


@pytest.fixture
def middleware(mock_get_response):
    return ServiceTokenMiddleware(mock_get_response)


class TestMiddlewarePathFiltering:
    """Tests for path-based middleware bypass."""

    def test_skips_non_api_paths(self, middleware, mock_get_response, request_factory):
        """Non-/api/ paths should pass through without authentication."""
        request = request_factory.get('/admin/')
        
        response = middleware(request)
        
        mock_get_response.assert_called_once_with(request)
        assert not hasattr(request, 'service_token')

    def test_skips_static_paths(self, middleware, mock_get_response, request_factory):
        """Static file paths should pass through."""
        request = request_factory.get('/static/css/style.css')
        
        response = middleware(request)
        
        mock_get_response.assert_called_once_with(request)

    def test_skips_health_endpoint(self, middleware, mock_get_response, request_factory):
        """Health check endpoint should not require auth."""
        request = request_factory.get('/api/query/health/')
        
        response = middleware(request)
        
        mock_get_response.assert_called_once_with(request)

    def test_skips_info_endpoint(self, middleware, mock_get_response, request_factory):
        """Info endpoint should not require auth."""
        request = request_factory.get('/api/query/info/')
        
        response = middleware(request)
        
        mock_get_response.assert_called_once_with(request)


class TestMiddlewareAuthorizationHeader:
    """Tests for Authorization header validation."""

    def test_missing_auth_header_returns_401(self, middleware, request_factory):
        """Missing Authorization header should return 401."""
        request = request_factory.get('/api/query/datasources/')
        
        response = middleware(request)
        
        assert response.status_code == 401
        assert b'Missing or invalid Authorization header' in response.content

    def test_empty_auth_header_returns_401(self, middleware, request_factory):
        """Empty Authorization header should return 401."""
        request = request_factory.get('/api/query/datasources/')
        request.META['HTTP_AUTHORIZATION'] = ''
        
        response = middleware(request)
        
        assert response.status_code == 401

    def test_non_bearer_auth_returns_401(self, middleware, request_factory):
        """Non-Bearer authorization should return 401."""
        request = request_factory.get('/api/query/datasources/')
        request.META['HTTP_AUTHORIZATION'] = 'Basic dXNlcjpwYXNz'
        
        response = middleware(request)
        
        assert response.status_code == 401
        assert b'Missing or invalid Authorization header' in response.content

    def test_malformed_bearer_returns_401(self, middleware, request_factory):
        """Malformed Bearer token (no space) should return 401."""
        request = request_factory.get('/api/query/datasources/')
        request.META['HTTP_AUTHORIZATION'] = 'Bearertoken123'
        
        response = middleware(request)
        
        assert response.status_code == 401


class TestMiddlewareTokenValidation:
    """Tests for token verification."""

    @patch('terno_dbi.middleware.verify_token')
    def test_invalid_token_returns_401(self, mock_verify, middleware, request_factory):
        """Invalid token hash should return 401."""
        mock_verify.return_value = None
        request = request_factory.get('/api/query/datasources/')
        request.META['HTTP_AUTHORIZATION'] = 'Bearer dbi_query_invalidtoken123'
        
        response = middleware(request)
        
        assert response.status_code == 401
        assert b'Invalid or expired Service Token' in response.content
        mock_verify.assert_called_once_with('dbi_query_invalidtoken123')

    @patch('terno_dbi.middleware.verify_token')
    def test_expired_token_returns_401(self, mock_verify, middleware, request_factory):
        """Expired token should return 401 (verify_token returns None for expired)."""
        mock_verify.return_value = None
        request = request_factory.get('/api/query/datasources/')
        request.META['HTTP_AUTHORIZATION'] = 'Bearer dbi_query_expiredtoken'
        
        response = middleware(request)
        
        assert response.status_code == 401

    @patch('terno_dbi.middleware.update_token_usage')
    @patch('terno_dbi.middleware.verify_token')
    def test_valid_token_sets_request_attribute(
        self, mock_verify, mock_update, middleware, mock_get_response, request_factory
    ):
        """Valid token should set request.service_token and pass through."""
        mock_token = MagicMock()
        mock_token.name = 'Test Token'
        mock_verify.return_value = mock_token
        
        request = request_factory.get('/api/query/datasources/')
        request.META['HTTP_AUTHORIZATION'] = 'Bearer dbi_query_validtoken123'
        
        response = middleware(request)
        
        mock_verify.assert_called_once_with('dbi_query_validtoken123')
        assert request.service_token == mock_token
        mock_get_response.assert_called_once_with(request)

    @patch('terno_dbi.middleware.update_token_usage')
    @patch('terno_dbi.middleware.verify_token')
    def test_valid_token_updates_usage(
        self, mock_verify, mock_update, middleware, mock_get_response, request_factory
    ):
        """Valid token should trigger update_token_usage."""
        mock_token = MagicMock()
        mock_verify.return_value = mock_token
        
        request = request_factory.get('/api/query/datasources/')
        request.META['HTTP_AUTHORIZATION'] = 'Bearer dbi_query_validtoken123'
        
        middleware(request)
        
        mock_update.assert_called_once_with(mock_token)


class TestMiddlewareIntegration:
    """Integration-style tests with database access."""

    @pytest.mark.django_db
    @patch('terno_dbi.middleware.update_token_usage')
    def test_inactive_token_returns_401(self, mock_update, middleware, request_factory):
        """Inactive token (is_active=False) should return 401."""
        from django.contrib.auth.models import User
        from terno_dbi.core.models import ServiceToken
        import hashlib
        
        # Create user and inactive token
        user = User.objects.create_user('testuser', 'test@example.com', 'password')
        token_key = 'dbi_query_testinactivetoken123'
        key_hash = hashlib.sha256(token_key.encode()).hexdigest()
        
        ServiceToken.objects.create(
            name='Inactive Token',
            token_type=ServiceToken.TokenType.QUERY,
            key_prefix='dbi_query_',
            key_hash=key_hash,
            is_active=False,
            created_by=user
        )
        
        request = request_factory.get('/api/query/datasources/')
        request.META['HTTP_AUTHORIZATION'] = f'Bearer {token_key}'
        
        response = middleware(request)
        
        assert response.status_code == 401

    @pytest.mark.django_db
    @patch('terno_dbi.middleware.update_token_usage')
    def test_active_token_passes_through(self, mock_update, middleware, mock_get_response, request_factory):
        """Active token should allow request to pass through."""
        from django.contrib.auth.models import User
        from terno_dbi.core.models import ServiceToken
        import hashlib
        
        # Create user and active token
        user = User.objects.create_user('testuser2', 'test2@example.com', 'password')
        token_key = 'dbi_query_testactivetoken456'
        key_hash = hashlib.sha256(token_key.encode()).hexdigest()
        
        ServiceToken.objects.create(
            name='Active Token',
            token_type=ServiceToken.TokenType.QUERY,
            key_prefix='dbi_query_',
            key_hash=key_hash,
            is_active=True,
            created_by=user
        )
        
        request = request_factory.get('/api/query/datasources/')
        request.META['HTTP_AUTHORIZATION'] = f'Bearer {token_key}'
        
        response = middleware(request)
        
        mock_get_response.assert_called_once()
        assert hasattr(request, 'service_token')
