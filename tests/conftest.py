"""
Pytest configuration for TernoDBI tests.

Provides fixtures for database connections, mock connectors, and Django setup.
"""

import os
import sys
import pytest

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))


def pytest_configure(config):
    """Configure Django settings before tests run."""
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dbi_server.settings')
    
    # Add server directory to path for settings
    server_path = os.path.join(os.path.dirname(__file__), '..', 'server')
    if server_path not in sys.path:
        sys.path.insert(0, server_path)
    
    import django
    django.setup()


@pytest.fixture
def mock_connector():
    """Fixture for creating mock database connector."""
    from tests.integration.test_pagination_integration import MockConnector
    
    def _create(rows, columns):
        return MockConnector(rows, columns)
    
    return _create


@pytest.fixture
def pagination_service(mock_connector):
    """Fixture for creating PaginationService with mock connector."""
    from terno_dbi.services.pagination import PaginationService
    
    def _create(rows, columns, dialect="postgres"):
        connector = mock_connector(rows, columns)
        return PaginationService(
            connector=connector,
            dialect=dialect,
            secret_key="test-secret-key"
        )
    
    return _create


@pytest.fixture
def sample_rows():
    """Fixture providing sample row data."""
    return [(i, f"item_{i}", f"desc_{i}") for i in range(1, 101)]


@pytest.fixture
def cursor_codec():
    """Fixture for CursorCodec."""
    from terno_dbi.services.pagination import CursorCodec
    return CursorCodec("test-secret-key", ttl_seconds=3600)
