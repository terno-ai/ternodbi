"""
Unit tests for issue_token management command.
"""
import pytest
from io import StringIO
from django.core.management import call_command
from django.utils import timezone
from unittest.mock import patch, MagicMock
from terno_dbi.core.models import ServiceToken

@pytest.mark.django_db
class TestIssueTokenCommand:
    """Tests for issue_token command."""

    def test_handle_success(self):
        """Should successfully issue a token."""
        out = StringIO()
        call_command('issue_token', '--name', 'test_token', stdout=out)
        
        output = out.getvalue()
        assert "Successfully issued token" in output
        assert "KEY" in output
        assert "TOKEN TYPE: QUERY" in output  # Default
        
        # Verify token created in DB
        assert ServiceToken.objects.filter(name='test_token').exists()

    def test_handle_admin_type(self):
        """Should issue admin token."""
        out = StringIO()
        call_command('issue_token', '--name', 'admin_tok', '--type', 'admin', stdout=out)
        
        output = out.getvalue()
        assert "TOKEN TYPE: ADMIN" in output
        
        token = ServiceToken.objects.get(name='admin_tok')
        assert token.token_type == ServiceToken.TokenType.ADMIN

    def test_handle_expires(self):
        """Should set expiry date."""
        out = StringIO()
        call_command('issue_token', '--name', 'exp_token', '--expires', '30', stdout=out)
        
        token = ServiceToken.objects.get(name='exp_token')
        assert token.expires_at is not None
        # Roughly 30 days from now (allow small delta)
        expected = timezone.now() + timezone.timedelta(days=30)
        assert abs((token.expires_at - expected).total_seconds()) < 60 # 1 min tolerance

    def test_handle_datasource_scope(self):
        """Should limit scope to datasources."""
        out = StringIO()
        # Need existing datasources? The command just passes IDs.
        # But generate_service_token might validate?
        # Let's check generate_service_token logic or if it just links.
        # Assuming it just links IDs.
        
        call_command('issue_token', '--name', 'scoped', '--datasource', '1', '--datasource', '2', stdout=out)
        
        output = out.getvalue()
        assert "Limited to datasources [1, 2]" in output or "Limited to datasources ['1', '2']" in output 
        # Output format depends on list str repr.
        
        # Verify DB connection if relevant, but command output check confirms arguments passed.

    def test_handle_error(self):
        """Should handle errors gracefully."""
        with patch('terno_dbi.core.management.commands.issue_token.generate_service_token') as mock_gen:
            mock_gen.side_effect = Exception("DB Error")
            
            out = StringIO()
            call_command('issue_token', '--name', 'fail', stdout=out)
            
            assert "Error creating token: DB Error" in out.getvalue()

    def test_handle_org_and_user_success(self):
        """Should issue token bound to org and user."""
        from terno_dbi.core.models import CoreOrganisation
        from django.contrib.auth import get_user_model
        
        User = get_user_model()
        user = User.objects.create_user(username='tok_user')
        org = CoreOrganisation.objects.create(name='Tok Org', subdomain='tok_org', owner=user)
        
        out = StringIO()
        call_command('issue_token', '--name', 'bound_token', '--org', 'tok_org', '--user', 'tok_user', stdout=out)
        
        output = out.getvalue()
        assert "ORG       : tok_org" in output
        assert "USER      : tok_user" in output
        
        token = ServiceToken.objects.get(name='bound_token')
        assert token.organisation == org
        assert token.created_for == user
        assert token.created_by is None

    def test_handle_invalid_org(self):
        """Should fail if organisation subdomain is invalid."""
        from django.core.management.base import CommandError
        out = StringIO()
        with pytest.raises(CommandError, match="No organisation with subdomain 'bad_org'"):
            call_command('issue_token', '--name', 'bad_org_token', '--org', 'bad_org', stdout=out)

    def test_handle_invalid_user(self):
        """Should fail if username is invalid."""
        from django.core.management.base import CommandError
        out = StringIO()
        with pytest.raises(CommandError, match="No user 'bad_user'"):
            call_command('issue_token', '--name', 'bad_user_token', '--user', 'bad_user', stdout=out)
