"""
Unit tests for ServiceToken scopes feature and require_scope decorator.

Tests the new scopes functionality added to ServiceToken model including:
- has_scope() method
- Wildcard scope matching
- Legacy token compatibility
- require_scope decorator
"""
import pytest
from datetime import timedelta
from unittest.mock import Mock, patch
from django.utils import timezone
from django.http import JsonResponse
from django.contrib.auth.models import User

from terno_dbi.core.models import ServiceToken, DataSource, CoreOrganisation
from terno_dbi.services.auth import generate_service_token, verify_token
from terno_dbi.decorators import require_scope, require_service_auth


@pytest.fixture
def user(db):
    """Create a test user."""
    return User.objects.create_user('scopeuser', 'scope@example.com', 'password')


@pytest.fixture
def organisation(db, user):
    """Create a test organisation."""
    return CoreOrganisation.objects.create(name='Test Org', owner=user)


@pytest.fixture
def datasource(db):
    """Create a test datasource."""
    return DataSource.objects.create(
        display_name='scope_test_db',
        type='postgres',
        connection_str='postgresql://localhost/scopetest',
        enabled=True
    )


@pytest.mark.django_db
class TestServiceTokenScopes:
    """Tests for ServiceToken.has_scope() method."""

    def test_has_scope_exact_match(self, user):
        """Should return True for exact scope match."""
        token = ServiceToken.objects.create(
            name='Exact Scope Test',
            token_type=ServiceToken.TokenType.QUERY,
            key_prefix='dbi_query_',
            key_hash='hash_exact',
            created_by=user,
            scopes=['query:read', 'query:execute']
        )
        
        assert token.has_scope('query:read') is True
        assert token.has_scope('query:execute') is True
        assert token.has_scope('admin:read') is False

    def test_has_scope_wildcard_match(self, user):
        """Should return True for wildcard scope match."""
        token = ServiceToken.objects.create(
            name='Wildcard Scope Test',
            token_type=ServiceToken.TokenType.ADMIN,
            key_prefix='dbi_admin_',
            key_hash='hash_wildcard',
            created_by=user,
            scopes=['query:*', 'admin:read']
        )
        
        # query:* should match any query scope
        assert token.has_scope('query:read') is True
        assert token.has_scope('query:execute') is True
        assert token.has_scope('query:export') is True
        
        # admin:read is exact, not wildcard
        assert token.has_scope('admin:read') is True
        assert token.has_scope('admin:write') is False

    def test_has_scope_legacy_token_without_scopes(self, user):
        """Legacy tokens without scopes should fall back to token_type check."""
        query_token = ServiceToken.objects.create(
            name='Legacy Query Token',
            token_type=ServiceToken.TokenType.QUERY,
            key_prefix='dbi_query_',
            key_hash='hash_legacy_query',
            created_by=user,
            scopes=[]
        )
        
        admin_token = ServiceToken.objects.create(
            name='Legacy Admin Token',
            token_type=ServiceToken.TokenType.ADMIN,
            key_prefix='dbi_admin_',
            key_hash='hash_legacy_admin',
            created_by=user,
            scopes=[]
        )
        
        # Query token should have query:* access
        assert query_token.has_scope('query:read') is True
        assert query_token.has_scope('query:execute') is True
        assert query_token.has_scope('admin:read') is False
        
        # Admin token should have admin:* access
        assert admin_token.has_scope('admin:read') is True
        assert admin_token.has_scope('admin:write') is True
        assert admin_token.has_scope('query:read') is False

    def test_has_scope_empty_scopes_list(self, user):
        """Empty scopes should fall back to token_type."""
        token = ServiceToken.objects.create(
            name='Empty Scopes',
            token_type=ServiceToken.TokenType.QUERY,
            key_prefix='dbi_query_',
            key_hash='hash_empty',
            created_by=user,
            scopes=[]
        )
        
        assert token.has_scope('query:read') is True
        assert token.has_scope('admin:read') is False

    def test_has_scope_unknown_scope(self, user):
        """Unknown scopes should return False."""
        token = ServiceToken.objects.create(
            name='Unknown Scope Test',
            token_type=ServiceToken.TokenType.QUERY,
            key_prefix='dbi_query_',
            key_hash='hash_unknown',
            created_by=user,
            scopes=['query:read']
        )
        
        assert token.has_scope('unknown:scope') is False
        assert token.has_scope('random') is False


@pytest.mark.django_db
class TestGenerateServiceTokenWithScopes:
    """Tests for generate_service_token function with scopes parameter."""

    def test_generate_token_with_scopes(self, user, organisation):
        """Should create token with specified scopes."""
        token, key = generate_service_token(
            name='Scoped Token',
            token_type=ServiceToken.TokenType.QUERY,
            created_by=user,
            organisation=organisation,
            scopes=['query:read', 'query:execute']
        )
        
        assert token.scopes == ['query:read', 'query:execute']
        assert token.has_scope('query:read') is True
        assert token.has_scope('query:execute') is True
        assert token.has_scope('admin:read') is False

    def test_generate_token_with_expiry_and_scopes(self, user):
        """Should create token with both expiry and scopes."""
        expires = timezone.now() + timedelta(hours=24)
        
        token, key = generate_service_token(
            name='Expiring Scoped Token',
            token_type=ServiceToken.TokenType.QUERY,
            created_by=user,
            expires_at=expires,
            scopes=['query:read']
        )
        
        assert token.scopes == ['query:read']
        assert token.expires_at is not None

    def test_generate_token_without_scopes(self, user):
        """Should create token with empty scopes when not specified."""
        token, key = generate_service_token(
            name='No Scopes Token',
            token_type=ServiceToken.TokenType.QUERY,
            created_by=user
        )
        
        assert token.scopes == []
        # Should fall back to token_type
        assert token.has_scope('query:read') is True

    def test_verify_token_returns_token_with_scopes(self, user):
        """Verified token should have access to scopes."""
        token, key = generate_service_token(
            name='Verify Scopes Test',
            token_type=ServiceToken.TokenType.QUERY,
            created_by=user,
            scopes=['query:execute']
        )
        
        verified = verify_token(key)
        
        assert verified is not None
        assert verified.id == token.id
        assert verified.scopes == ['query:execute']
        assert verified.has_scope('query:execute') is True


@pytest.mark.django_db
class TestRequireScopeDecorator:
    """Tests for @require_scope decorator."""

    def test_require_scope_allows_matching_scope(self, user):
        """Should allow request when token has required scope."""
        token = ServiceToken.objects.create(
            name='Decorator Test',
            token_type=ServiceToken.TokenType.QUERY,
            key_prefix='dbi_query_',
            key_hash='hash_decorator',
            created_by=user,
            scopes=['query:execute']
        )
        
        request = Mock()
        request.service_token = token
        
        @require_scope('query:execute')
        def test_view(request):
            return JsonResponse({'status': 'ok'})
        
        response = test_view(request)
        assert response.status_code == 200

    def test_require_scope_denies_missing_scope(self, user):
        """Should return 403 when token lacks required scope."""
        token = ServiceToken.objects.create(
            name='Missing Scope Test',
            token_type=ServiceToken.TokenType.QUERY,
            key_prefix='dbi_query_',
            key_hash='hash_missing',
            created_by=user,
            scopes=['query:read']
        )
        
        request = Mock()
        request.service_token = token
        
        @require_scope('admin:write')
        def test_view(request):
            return JsonResponse({'status': 'ok'})
        
        response = test_view(request)
        assert response.status_code == 403

    def test_require_scope_returns_401_without_token(self):
        """Should return 401 when no service_token on request."""
        request = Mock(spec=[])  # No service_token attribute
        
        @require_scope('query:execute')
        def test_view(request):
            return JsonResponse({'status': 'ok'})
        
        response = test_view(request)
        assert response.status_code == 401

    def test_require_scope_multiple_scopes(self, user):
        """Should require ALL specified scopes."""
        token = ServiceToken.objects.create(
            name='Multiple Scopes Test',
            token_type=ServiceToken.TokenType.ADMIN,
            key_prefix='dbi_admin_',
            key_hash='hash_multiple',
            created_by=user,
            scopes=['admin:read', 'admin:write']
        )
        
        request = Mock()
        request.service_token = token
        
        @require_scope('admin:read', 'admin:write')
        def test_view(request):
            return JsonResponse({'status': 'ok'})
        
        response = test_view(request)
        assert response.status_code == 200

    def test_require_scope_fails_partial_match(self, user):
        """Should fail if only some scopes are present."""
        token = ServiceToken.objects.create(
            name='Partial Scope Test',
            token_type=ServiceToken.TokenType.ADMIN,
            key_prefix='dbi_admin_',
            key_hash='hash_partial',
            created_by=user,
            scopes=['admin:read']  # Missing admin:write
        )
        
        request = Mock()
        request.service_token = token
        
        @require_scope('admin:read', 'admin:write')
        def test_view(request):
            return JsonResponse({'status': 'ok'})
        
        response = test_view(request)
        assert response.status_code == 403

    def test_require_scope_with_wildcard(self, user):
        """Should work with wildcard scopes."""
        token = ServiceToken.objects.create(
            name='Wildcard Decorator Test',
            token_type=ServiceToken.TokenType.QUERY,
            key_prefix='dbi_query_',
            key_hash='hash_wild',
            created_by=user,
            scopes=['query:*']
        )
        
        request = Mock()
        request.service_token = token
        
        @require_scope('query:execute')
        def test_view(request):
            return JsonResponse({'status': 'ok'})
        
        response = test_view(request)
        assert response.status_code == 200


@pytest.mark.django_db
class TestTokenExpiry:
    """Tests for token expiry with scopes."""

    def test_expired_token_not_verified(self, user):
        """Expired token should return None from verify_token."""
        expired = timezone.now() - timedelta(hours=1)
        
        token, key = generate_service_token(
            name='Expired Token',
            token_type=ServiceToken.TokenType.QUERY,
            created_by=user,
            expires_at=expired,
            scopes=['query:read']
        )
        
        verified = verify_token(key)
        assert verified is None

    def test_valid_token_verified(self, user):
        """Valid (non-expired) token should be verified."""
        future = timezone.now() + timedelta(hours=24)
        
        token, key = generate_service_token(
            name='Valid Token',
            token_type=ServiceToken.TokenType.QUERY,
            created_by=user,
            expires_at=future,
            scopes=['query:execute']
        )
        
        verified = verify_token(key)
        assert verified is not None
        assert verified.id == token.id
