import pytest
import sys
import json
from unittest.mock import patch, MagicMock
from io import StringIO

from terno_dbi.cli import main, print_welcome_message, create_default_superuser


@pytest.mark.django_db
class TestCLI:

    def test_print_welcome_message(self, caplog):
        with caplog.at_level("INFO"):
            print_welcome_message("8000")
        output = caplog.text
        assert "TernoDBI server is live and ready." in output
        assert "http://127.0.0.1:8000" in output

    @patch('terno_dbi.cli.django.setup')
    def test_create_default_superuser_new(self, mock_setup):
        from django.contrib.auth import get_user_model
        from terno_dbi.core.models import CoreOrganisation
        User = get_user_model()
        User.objects.all().delete()
        
        create_default_superuser()
        
        mock_setup.assert_called_once()
        assert User.objects.filter(username='admin').exists()
        assert CoreOrganisation.objects.filter(subdomain='default').exists()

    @patch('terno_dbi.cli.django.setup')
    def test_create_default_superuser_existing(self, mock_setup):
        from django.contrib.auth import get_user_model
        from terno_dbi.core.models import CoreOrganisation
        User = get_user_model()
        User.objects.all().delete()
        user = User.objects.create_superuser('existing_admin', 'admin@example.com', 'admin')
        
        create_default_superuser()
        
        # Should not create a new user named 'admin', should just use 'existing_admin'
        assert not User.objects.filter(username='admin').exists()
        org = CoreOrganisation.objects.get(subdomain='default')
        assert org.owner == user

    @patch('sys.exit')
    def test_main_no_args(self, mock_exit, caplog):
        mock_exit.side_effect = SystemExit(1)
        with patch.object(sys, 'argv', ['ternodbi']):
            with caplog.at_level("INFO"):
                try:
                    main()
                except SystemExit:
                    pass
            mock_exit.assert_called_once_with(1)
            assert "Usage: ternodbi <command>" in caplog.text

    @patch('sys.exit')
    def test_main_unknown_args(self, mock_exit, caplog):
        mock_exit.side_effect = SystemExit(1)
        with patch.object(sys, 'argv', ['ternodbi', 'invalidcommand']):
            with caplog.at_level("ERROR"):
                try:
                    main()
                except SystemExit:
                    pass
            mock_exit.assert_called_once_with(1)
            assert "Unknown command: invalidcommand" in caplog.text

    @patch('terno_dbi.cli.execute_from_command_line')
    @patch('terno_dbi.cli.create_default_superuser')
    @patch('terno_dbi.cli.print_welcome_message')
    def test_main_start(self, mock_print, mock_create_user, mock_execute):
        with patch.object(sys, 'argv', ['ternodbi', 'start']):
            main()
            # It should call it twice: once for migrate, once for runserver
            assert mock_execute.call_count == 2
            mock_execute.assert_any_call(['manage.py', 'migrate', '--verbosity', '0'])
            mock_execute.assert_any_call(['manage.py', 'runserver', '--noreload', '127.0.0.1:8376'])
            mock_create_user.assert_called_once()
            mock_print.assert_called_once_with("8376")

    @patch('terno_dbi.cli.execute_from_command_line')
    def test_main_manage(self, mock_execute):
        with patch.object(sys, 'argv', ['ternodbi', 'manage', 'makemigrations']):
            main()
            mock_execute.assert_called_once_with(['manage.py', 'makemigrations'])

    def test_main_mcp_config(self, caplog):
        with patch.object(sys, 'argv', ['ternodbi', 'mcp-config']):
            with caplog.at_level("INFO"):
                main()
            output = caplog.text
            assert "MCP Configuration Snippet" in output
            assert "ternodbi-query" in output
            assert "ternodbi-admin" in output
