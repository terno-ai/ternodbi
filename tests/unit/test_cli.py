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
        with patch('terno_dbi.cli.get_user_model') as mock_get_user_model:
            mock_user_model = MagicMock()
            mock_get_user_model.return_value = mock_user_model

            # Simulate no superuser exists
            mock_user_model.objects.filter().exists.return_value = False

            create_default_superuser()
            mock_setup.assert_called_once()
            mock_user_model.objects.create_superuser.assert_called_once_with('admin', 'admin@example.com', 'admin')

    @patch('terno_dbi.cli.django.setup')
    def test_create_default_superuser_existing(self, mock_setup):
        with patch('terno_dbi.cli.get_user_model') as mock_get_user_model:
            mock_user_model = MagicMock()
            mock_get_user_model.return_value = mock_user_model

            # Simulate superuser already exists
            mock_user_model.objects.filter().exists.return_value = True

            create_default_superuser()
            mock_user_model.objects.create_superuser.assert_not_called()

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
