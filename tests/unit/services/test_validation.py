"""
Unit tests for Validation Service (services/validation.py).

Tests datasource input validation including connection string parsing
and database-specific validation.
"""
import pytest
import json
from unittest.mock import patch, MagicMock


class TestValidateDatasourceInput:
    """Tests for validate_datasource_input function."""

    def test_rejects_empty_connection_string(self):
        """Should reject empty connection string."""
        from terno_dbi.services.validation import validate_datasource_input

        error = validate_datasource_input('postgres', '')

        assert error is not None
        assert 'required' in error.lower()

    def test_rejects_empty_type(self):
        """Should reject empty database type."""
        from terno_dbi.services.validation import validate_datasource_input

        error = validate_datasource_input('', 'postgresql://localhost/db')

        assert error is not None
        assert 'required' in error.lower()

    def test_rejects_none_connection_string(self):
        """Should reject None connection string."""
        from terno_dbi.services.validation import validate_datasource_input

        error = validate_datasource_input('postgres', None)

        assert error is not None
        assert 'required' in error.lower()

    def test_sqlalchemy_parsing_error(self):
        """Should handle SQLAlchemy ArgumentError gracefully."""
        from terno_dbi.services.validation import validate_datasource_input

        with patch('terno_dbi.services.validation.make_url') as mock_make:
            from sqlalchemy.exc import ArgumentError
            mock_make.side_effect = ArgumentError("Bad URL")

            error = validate_datasource_input('postgres', 'bad://url')

            assert error is not None
            assert 'parse sqlalchemy url' in error.lower()

    def test_rejects_invalid_url_format(self):
        """Should reject connection strings without protocol."""
        from terno_dbi.services.validation import validate_datasource_input

        error = validate_datasource_input('postgres', 'localhost/db')

        assert error is not None
        assert '://' in error

    def test_rejects_multiple_protocol_separators(self):
        """Should reject connection strings with multiple ://."""
        from terno_dbi.services.validation import validate_datasource_input

        error = validate_datasource_input('postgres', 'postgresql://host://db')

        assert error is not None
        assert 'multiple' in error.lower()

    def test_rejects_mismatched_dialect(self):
        """Should reject connection string that doesn't match type."""
        from terno_dbi.services.validation import validate_datasource_input

        error = validate_datasource_input('postgres', 'mysql://localhost/db')

        assert error is not None
        assert 'does not match' in error.lower()

    def test_strips_whitespace(self):
        """Should strip whitespace from connection string."""
        from terno_dbi.services.validation import validate_datasource_input

        with patch('terno_dbi.services.validation.ConnectorFactory') as mock_factory:
            mock_connector = MagicMock()
            mock_factory.create_connector.return_value = mock_connector
            mock_connector.get_connection.return_value.__enter__ = MagicMock()
            mock_connector.get_connection.return_value.__exit__ = MagicMock()

            error = validate_datasource_input(
                'postgres', 
                '  postgresql://localhost/db  '
            )

            assert error is None

    def test_accepts_postgresql_prefix(self):
        """Should accept postgresql:// for postgres type."""
        from terno_dbi.services.validation import validate_datasource_input

        with patch('terno_dbi.services.validation.ConnectorFactory') as mock_factory:
            mock_connector = MagicMock()
            mock_factory.create_connector.return_value = mock_connector
            mock_connector.get_connection.return_value.__enter__ = MagicMock()
            mock_connector.get_connection.return_value.__exit__ = MagicMock()

            error = validate_datasource_input('postgres', 'postgresql://localhost/db')

            # If connection succeeds, no error
            assert error is None

    def test_accepts_postgresql_psycopg2_prefix(self):
        """Should accept postgresql+psycopg2:// for postgres type."""
        from terno_dbi.services.validation import validate_datasource_input
        
        with patch('terno_dbi.services.validation.ConnectorFactory') as mock_factory:
            mock_connector = MagicMock()
            mock_factory.create_connector.return_value = mock_connector
            mock_connector.get_connection.return_value.__enter__ = MagicMock()
            mock_connector.get_connection.return_value.__exit__ = MagicMock()
            
            error = validate_datasource_input(
                'postgres', 
                'postgresql+psycopg2://localhost/db'
            )
            
            assert error is None

    def test_accepts_mysql_prefix(self):
        """Should accept mysql:// for mysql type."""
        from terno_dbi.services.validation import validate_datasource_input
        
        with patch('terno_dbi.services.validation.ConnectorFactory') as mock_factory:
            mock_connector = MagicMock()
            mock_factory.create_connector.return_value = mock_connector
            mock_connector.get_connection.return_value.__enter__ = MagicMock()
            mock_connector.get_connection.return_value.__exit__ = MagicMock()
            
            error = validate_datasource_input('mysql', 'mysql://localhost/db')
            
            assert error is None

    def test_accepts_mysql_pymysql_prefix(self):
        """Should accept mysql+pymysql:// for mysql type."""
        from terno_dbi.services.validation import validate_datasource_input
        
        with patch('terno_dbi.services.validation.ConnectorFactory') as mock_factory:
            mock_connector = MagicMock()
            mock_factory.create_connector.return_value = mock_connector
            mock_connector.get_connection.return_value.__enter__ = MagicMock()
            mock_connector.get_connection.return_value.__exit__ = MagicMock()
            
            error = validate_datasource_input('mysql', 'mysql+pymysql://localhost/db')
            
            assert error is None

    def test_accepts_type_case_insensitive(self):
        """Should handle type in different cases."""
        from terno_dbi.services.validation import validate_datasource_input
        
        with patch('terno_dbi.services.validation.ConnectorFactory') as mock_factory:
            mock_connector = MagicMock()
            mock_factory.create_connector.return_value = mock_connector
            mock_connector.get_connection.return_value.__enter__ = MagicMock()
            mock_connector.get_connection.return_value.__exit__ = MagicMock()
            
            error = validate_datasource_input('POSTGRES', 'postgresql://localhost/db')
            
            assert error is None

    def test_accepts_unknown_type(self):
        """Should allow unknown database types (no prefix validation)."""
        from terno_dbi.services.validation import validate_datasource_input
        
        with patch('terno_dbi.services.validation.ConnectorFactory') as mock_factory:
            mock_connector = MagicMock()
            mock_factory.create_connector.return_value = mock_connector
            mock_connector.get_connection.return_value.__enter__ = MagicMock()
            mock_connector.get_connection.return_value.__exit__ = MagicMock()
            
            error = validate_datasource_input('custom', 'custom://localhost/db')
            
            assert error is None


class TestBigQueryValidation:
    """Tests for BigQuery-specific validation."""

    def test_bigquery_requires_connection_json(self):
        """BigQuery should require connection_json."""
        from terno_dbi.services.validation import validate_datasource_input
        
        error = validate_datasource_input(
            'bigquery', 
            'bigquery://project/dataset',
            connection_json=None
        )
        
        assert error is not None
        assert 'credentials' in error.lower() or 'connection_json' in error.lower()

    def test_bigquery_validates_url_format(self):
        """BigQuery URL should be project_id/dataset_id."""
        from terno_dbi.services.validation import validate_datasource_input
        
        # Too few parts
        error = validate_datasource_input(
            'bigquery',
            'bigquery://project_only',
            connection_json='{"type": "service_account"}'
        )
        
        assert error is not None
        assert 'project_id/dataset_id' in error.lower() or 'format' in error.lower()

    def test_bigquery_accepts_json_string(self):
        """BigQuery should parse JSON string credentials."""
        from terno_dbi.services.validation import validate_datasource_input
        
        # Will fail on BigQuery client because of bad credentials, but parses JSON
        error = validate_datasource_input(
            'bigquery',
            'bigquery://project/dataset',
            connection_json='{"type": "service_account", "private_key": "test"}'
        )
        
        # Error should not be about JSON parsing
        if error:
            assert 'json' not in error.lower() or 'validating' in error.lower()

    def test_bigquery_handles_invalid_json(self):
        """BigQuery should handle malformed JSON."""
        from terno_dbi.services.validation import validate_datasource_input
        
        error = validate_datasource_input(
            'bigquery',
            'bigquery://project/dataset',
            connection_json='not valid json'
        )
        
        assert error is not None

    def test_bigquery_accepts_dict_credentials(self):
        """BigQuery should accept dict credentials."""
        from terno_dbi.services.validation import validate_datasource_input
        
        # Will fail on actual validation but dict should be accepted
        error = validate_datasource_input(
            'bigquery',
            'bigquery://project/dataset',
            connection_json={"type": "service_account"}
        )
        
        # Some error expected but not about dict type
        if error:
            assert 'dict' not in error.lower()

    @patch('terno_dbi.services.validation.json.loads')
    def test_bq_client_not_found_error(self, mock_json):
        """Should handle BigQuery NotFound exception."""
        from terno_dbi.services.validation import validate_datasource_input
        # We need to mock bigquery module usage inside the function
        # Since it's imported INSIDE the function (line 62), we mock via sys.modules or patch.dict?
        # Standard patch might work if we target where it's used if it was global, but it's local.
        # Best way: patch 'google.cloud.bigquery' if available. 
        # But we can assume it's installed in test env or mocked.
        # If we patch 'terno_dbi.services.validation.bigquery' it won't work because it's not imported at top level.
        # We must use patch.dict('sys.modules', ...) or just assume logic flow if we can control it.
        # Actually, python `patch` can patch lazy imports if we target the location where it is looked up?
        # No.
        # Strategy: We can mock `google.cloud.bigquery` in `sys.modules` BEFORE calling the function.
        
        with patch.dict('sys.modules', {'google.cloud': MagicMock(), 'google.cloud.bigquery': MagicMock(), 'google.cloud.exceptions': MagicMock()}):
            from google.cloud import exceptions
            from google.cloud import bigquery
            
            # Setup Exception
            exceptions.NotFound = Exception 
            # We must use a real class for try/except to catch it matchingly? 
            # Or just `type('NotFound', (Exception,), {})`
            class NotFound(Exception): pass
            exceptions.NotFound = NotFound
            
            # Setup Client
            mock_client = MagicMock()
            bigquery.Client.from_service_account_info.return_value = mock_client
            mock_client.list_datasets.side_effect = NotFound("Project not found")
            
            error = validate_datasource_input(
                'bigquery', 
                'bigquery://proj/data', 
                connection_json='{}'
            )
            
            assert error is not None
            assert 'project \'proj\' does not exist' in error.lower()

    @patch('terno_dbi.services.validation.json.loads')
    def test_bq_dataset_not_found_error(self, mock_json):
        """Should handle BigQuery dataset NotFound exception."""
        from terno_dbi.services.validation import validate_datasource_input
        try:
            from google.cloud import exceptions
        except ImportError:
            pytest.skip("google.cloud not installed")

        with patch('google.cloud.bigquery.Client') as mock_client_cls:
            mock_client = MagicMock()
            mock_client_cls.from_service_account_info.return_value = mock_client
            mock_client.list_datasets.return_value = None
            mock_client.get_dataset.side_effect = exceptions.NotFound("Dataset not found")

            error = validate_datasource_input(
                'bigquery', 
                'bigquery://proj/data', 
                connection_json='{}'
            )

            assert error is not None
            assert "Dataset 'data' does not exist" in error

    @patch('terno_dbi.services.validation.json.loads')
    def test_bq_dataset_permission_denied_error(self, mock_json):
        """Should handle BigQuery dataset PermissionDenied exception."""
        from terno_dbi.services.validation import validate_datasource_input
        try:
            from google.cloud import exceptions
        except ImportError:
            pytest.skip("google.cloud not installed")

        with patch('google.cloud.bigquery.Client') as mock_client_cls:
            mock_client = MagicMock()
            mock_client_cls.from_service_account_info.return_value = mock_client
            mock_client.list_datasets.return_value = None
            mock_client.get_dataset.side_effect = exceptions.Forbidden("Denied")

            error = validate_datasource_input(
                'bigquery', 
                'bigquery://proj/data', 
                connection_json='{}'
            )

            assert error is not None
            assert 'Cannot access dataset' in error

    def test_bq_import_error(self):
        """Should handle missing bigquery library."""
        from terno_dbi.services.validation import validate_datasource_input
        import sys

        # When sys.modules has None for a key, import raises ImportError/ModuleNotFoundError
        with patch.dict(sys.modules, {'google.cloud.bigquery': None, 'google.cloud': None}):
            # We must ensure references in the module are re-evaluated or specific import fails
            # Since the import is inside the function `from google.cloud import bigquery`,
            # this patch should work effectively.

            error = validate_datasource_input(
                'bigquery', 
                'bigquery://proj/data',
                connection_json='{}'
            )
            with patch('terno_dbi.services.validation.logger') as mock_logger:
                # We expect "not installed" warning
                validate_datasource_input('bigquery', 'bigquery://proj/data', connection_json='{}')
                mock_logger.warning.assert_called_with("google-cloud-bigquery not installed, skipping detailed BigQuery validation")

    @patch('google.cloud.bigquery.Client')
    def test_bq_permission_denied(self, mock_client_cls):
        """Should handle BigQuery PermissionDenied."""
        from terno_dbi.services.validation import validate_datasource_input

        try:
            from google.cloud import exceptions
        except ImportError:

            pytest.skip("google.cloud not installed")

        # Define exception class
        class Forbidden(Exception): pass

        # Patch the module to have Forbidden attribute
        with patch.object(exceptions, 'Forbidden', Forbidden, create=True):
            mock_client = MagicMock()
            mock_client_cls.from_service_account_info.return_value = mock_client
            mock_client.list_datasets.side_effect = Forbidden("Denied")

            error = validate_datasource_input(
                'bigquery', 
                'bigquery://proj/data', 
                connection_json='{"type": "service_account"}'
            )

            assert error is not None
            assert 'permission denied' in error.lower()


class TestConnectionValidation:
    """Tests for actual connection validation."""

    @patch('terno_dbi.services.validation.ConnectorFactory')
    def test_returns_none_on_success(self, mock_factory):
        """Should return None when connection succeeds."""
        from terno_dbi.services.validation import validate_datasource_input

        mock_connector = MagicMock()
        mock_factory.create_connector.return_value = mock_connector
        mock_connector.get_connection.return_value.__enter__ = MagicMock()
        mock_connector.get_connection.return_value.__exit__ = MagicMock()

        error = validate_datasource_input(
            'postgres',
            'postgresql://localhost/db'
        )

        assert error is None

    @patch('terno_dbi.services.validation.ConnectorFactory')
    def test_returns_error_on_connection_failure(self, mock_factory):
        """Should return error message when connection fails."""
        from terno_dbi.services.validation import validate_datasource_input

        mock_factory.create_connector.side_effect = Exception("Connection refused")

        error = validate_datasource_input(
            'postgres',
            'postgresql://localhost/db'
        )

        assert error is not None
        assert 'Could not connect' in error

    @patch('terno_dbi.services.validation.ConnectorFactory')
    def test_closes_connector_after_validation(self, mock_factory):
        """Should close connector after successful validation."""
        from terno_dbi.services.validation import validate_datasource_input

        mock_connector = MagicMock()
        mock_factory.create_connector.return_value = mock_connector
        mock_connector.get_connection.return_value.__enter__ = MagicMock()
        mock_connector.get_connection.return_value.__exit__ = MagicMock()

        validate_datasource_input('postgres', 'postgresql://localhost/db')

        mock_connector.close.assert_called_once()


class TestDialectPrefixes:
    """Tests for dialect prefix mappings."""

    def test_postgres_prefixes(self):
        """Postgres should accept postgresql and postgresql+psycopg2."""
        from terno_dbi.services.validation import DIALECT_PREFIXES

        postgres_prefixes = DIALECT_PREFIXES.get('postgres')
        assert 'postgresql://' in postgres_prefixes
        assert 'postgresql+psycopg2://' in postgres_prefixes

    def test_mysql_prefixes(self):
        """MySQL should accept mysql and mysql+pymysql."""
        from terno_dbi.services.validation import DIALECT_PREFIXES

        mysql_prefixes = DIALECT_PREFIXES.get('mysql')
        assert 'mysql://' in mysql_prefixes
        assert 'mysql+pymysql://' in mysql_prefixes

    def test_bigquery_prefix(self):
        """BigQuery should accept bigquery://."""
        from terno_dbi.services.validation import DIALECT_PREFIXES

        bq_prefixes = DIALECT_PREFIXES.get('bigquery')
        assert 'bigquery://' in bq_prefixes

    def test_snowflake_prefix(self):
        """Snowflake should accept snowflake://."""
        from terno_dbi.services.validation import DIALECT_PREFIXES

        sf_prefixes = DIALECT_PREFIXES.get('snowflake')
        assert 'snowflake://' in sf_prefixes

    def test_databricks_prefix(self):
        """Databricks should accept databricks://."""
        from terno_dbi.services.validation import DIALECT_PREFIXES

        db_prefixes = DIALECT_PREFIXES.get('databricks')
        assert 'databricks://' in db_prefixes

    def test_oracle_prefix(self):
        """Oracle should accept oracle+oracledb://."""
        from terno_dbi.services.validation import DIALECT_PREFIXES

        oracle_prefixes = DIALECT_PREFIXES.get('oracle')
        assert 'oracle+oracledb://' in oracle_prefixes
