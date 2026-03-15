"""
Unit tests for Auth Service.

Tests the token generation, verification, and usage tracking functions
in terno_dbi.services.auth.
"""
import pytest
import hashlib
from datetime import timedelta
from unittest.mock import patch, MagicMock
from django.utils import timezone

from terno_dbi.services.auth import (
    generate_service_token,
    verify_token,
    update_token_usage
)
from terno_dbi.core.models import ServiceToken


@pytest.mark.django_db
class TestGenerateServiceToken:
    """Tests for generate_service_token function."""

    @pytest.fixture
    def user(self):
        from django.contrib.auth.models import User
        import uuid
        uname = f"testuser_{uuid.uuid4().hex[:8]}"
        return User.objects.create_user(uname, f"{uname}@example.com", 'password')

    def test_generates_unique_key(self, user):
        """Each generated token should have a unique key."""
        token1, key1 = generate_service_token(name='Token 1', created_by=user)
        token2, key2 = generate_service_token(name='Token 2', created_by=user)
        
        assert key1 != key2
        assert token1.key_hash != token2.key_hash

    def test_key_format_has_dbi_prefix(self, user):
        """Token key should start with dbi_<type>_ prefix."""
        token, key = generate_service_token(
            name='Query Token',
            token_type=ServiceToken.TokenType.QUERY,
            created_by=user
        )
        
        assert key.startswith('dbi_query_')
        assert token.key_prefix == 'dbi_query_'

    def test_admin_token_prefix(self, user):
        """Admin token should have dbi_admin_ prefix."""
        token, key = generate_service_token(
            name='Admin Token',
            token_type=ServiceToken.TokenType.ADMIN,
            created_by=user
        )
        
        assert key.startswith('dbi_admin_')
        assert token.key_prefix == 'dbi_admin_'

    def test_stores_hash_not_plain_key(self, user):
        """Token should store SHA-256 hash, not the plain key."""
        token, key = generate_service_token(name='Test Token', created_by=user)
        
        expected_hash = hashlib.sha256(key.encode()).hexdigest()
        assert token.key_hash == expected_hash
        assert key not in token.key_hash

    def test_token_is_active_by_default(self, user):
        """Generated token should be active by default."""
        token, key = generate_service_token(name='Test Token', created_by=user)
        
        assert token.is_active is True

    def test_assigns_created_by(self, user):
        """Token should record who created it."""
        token, key = generate_service_token(name='Test Token', created_by=user)
        
        assert token.created_by == user

    def test_assigns_organisation(self, user):
        """Token should be assignable to an organisation."""
        from terno_dbi.core.models import CoreOrganisation
        
        org = CoreOrganisation.objects.create(
            name='Test Org',
            subdomain='testorg',
            owner=user
        )
        
        token, key = generate_service_token(
            name='Org Token',
            created_by=user,
            organisation=org
        )
        
        assert token.organisation == org

    def test_sets_expiry(self, user):
        """Token should accept expiry datetime."""
        expires = timezone.now() + timedelta(days=30)
        
        token, key = generate_service_token(
            name='Expiring Token',
            created_by=user,
            expires_at=expires
        )
        
        assert token.expires_at is not None
        assert abs((token.expires_at - expires).total_seconds()) < 1

    def test_assigns_datasources(self, user):
        """Token should be linkable to specific datasources."""
        from terno_dbi.core.models import DataSource
        
        ds1 = DataSource.objects.create(display_name='DS1', type='postgres')
        ds2 = DataSource.objects.create(display_name='DS2', type='mysql')
        
        token, key = generate_service_token(
            name='Scoped Token',
            created_by=user,
            datasource_ids=[ds1.id, ds2.id]
        )
        
        assert token.datasources.count() == 2
        assert ds1 in token.datasources.all()
        assert ds2 in token.datasources.all()

    def test_assigns_groups(self, user):
        """Token should inherit specific Django Groups."""
        from django.contrib.auth.models import Group
        
        group1 = Group.objects.create(name='Token Group 1')
        group2 = Group.objects.create(name='Token Group 2')
        
        token, key = generate_service_token(
            name='Grouped Token',
            created_by=user,
            groups=[group1, group2]
        )
        
        assert token.groups.count() == 2
        assert group1 in token.groups.all()
        assert group2 in token.groups.all()

    def test_returns_tuple_of_token_and_key(self, user):
        """Function should return (ServiceToken, str) tuple."""
        result = generate_service_token(name='Test Token', created_by=user)
        
        assert isinstance(result, tuple)
        assert len(result) == 2
        assert isinstance(result[0], ServiceToken)
        assert isinstance(result[1], str)


@pytest.mark.django_db
class TestVerifyToken:
    """Tests for verify_token function."""

    @pytest.fixture
    def user(self):
        from django.contrib.auth.models import User
        import uuid
        uname = f"verifyuser_{uuid.uuid4().hex[:8]}"
        return User.objects.create_user(uname, f"{uname}@example.com", 'password')

    def test_valid_token_returns_token_obj(self, user):
        """Valid token string should return the ServiceToken object."""
        token, key = generate_service_token(name='Valid Token', created_by=user)
        
        result = verify_token(key)
        
        assert result is not None
        assert result.id == token.id
        assert result.name == 'Valid Token'

    def test_invalid_hash_returns_none(self, user):
        """Token with wrong hash should return None."""
        generate_service_token(name='Test Token', created_by=user)
        
        result = verify_token('dbi_query_wronghashvalue123456')
        
        assert result is None

    def test_inactive_token_returns_none(self, user):
        """Inactive token should return None."""
        token, key = generate_service_token(name='Inactive Token', created_by=user)
        token.is_active = False
        token.save()
        
        result = verify_token(key)
        
        assert result is None

    def test_expired_token_returns_none(self, user):
        """Expired token should return None."""
        expired_time = timezone.now() - timedelta(days=1)
        token, key = generate_service_token(
            name='Expired Token',
            created_by=user,
            expires_at=expired_time
        )
        
        result = verify_token(key)
        
        assert result is None

    def test_non_dbi_prefix_returns_none(self):
        """Token without dbi_ prefix should return None."""
        result = verify_token('invalid_prefix_token123')
        
        assert result is None

    def test_empty_token_returns_none(self):
        """Empty string should return None."""
        result = verify_token('')
        
        assert result is None

    def test_none_token_returns_none(self):
        """None input should return None."""
        result = verify_token(None)
        
        assert result is None

    def test_non_expired_token_valid(self, user):
        """Token with future expiry should be valid."""
        future_expiry = timezone.now() + timedelta(days=30)
        token, key = generate_service_token(
            name='Future Token',
            created_by=user,
            expires_at=future_expiry
        )
        
        result = verify_token(key)
        
        assert result is not None
        assert result.id == token.id


@pytest.mark.django_db
class TestUpdateTokenUsage:
    """Tests for update_token_usage function."""

    @pytest.fixture
    def user(self):
        from django.contrib.auth.models import User
        import uuid
        uname = f"usageuser_{uuid.uuid4().hex[:8]}"
        return User.objects.create_user(uname, f"{uname}@example.com", 'password')

    def test_updates_last_used_timestamp(self, user):
        """update_token_usage should update last_used field."""
        token, key = generate_service_token(name='Usage Token', created_by=user)
        
        assert token.last_used is None
        
        before_update = timezone.now()
        update_token_usage(token)
        
        token.refresh_from_db()
        
        assert token.last_used is not None
        assert token.last_used >= before_update

    def test_multiple_updates_change_timestamp(self, user):
        """Multiple calls should update the timestamp each time."""
        token, key = generate_service_token(name='Multi Usage', created_by=user)
        
        update_token_usage(token)
        token.refresh_from_db()
        first_usage = token.last_used
        
        # Small delay to ensure different timestamp
        import time
        time.sleep(0.01)
        
        update_token_usage(token)
        token.refresh_from_db()
        second_usage = token.last_used
        
        assert second_usage >= first_usage
