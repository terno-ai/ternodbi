import os
import sys
import pytest
from cryptography.fernet import Fernet

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

# Set at module import — before pytest-django configures Django from
# DJANGO_SETTINGS_MODULE, so `settings.MCP_ENCRYPTION_KEY` is populated. Credentials
# are encrypted at rest, so saving a DataSource needs a key; this throwaway one
# serves the whole suite. Tests exercising the key ring or the missing-key path
# override it via the `settings` fixture.
os.environ.setdefault('MCP_ENCRYPTION_KEY', Fernet.generate_key().decode())


def pytest_configure(config):
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'terno_dbi.server.settings')

    import django
    django.setup()


@pytest.fixture(autouse=True)
def _encryption_key(settings):
    """Every test can store credentials: ensure an encryption key and a clean
    key-ring cache. Deterministic regardless of import/collection order — tests
    that need a specific key (rotation, missing-key) override it after this runs.
    """
    from terno_dbi.services import secrets
    if not getattr(settings, 'MCP_ENCRYPTION_KEY', '') and \
       not getattr(settings, 'MCP_ENCRYPTION_KEYS', ''):
        settings.MCP_ENCRYPTION_KEY = Fernet.generate_key().decode()
    secrets.reset_cache()
    yield
    secrets.reset_cache()


@pytest.fixture
def mock_connector():
    from tests.integration.test_pagination_integration import MockConnector

    def _create(rows, columns):
        return MockConnector(rows, columns)

    return _create


@pytest.fixture
def pagination_service(mock_connector):
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
    return [(i, f"item_{i}", f"desc_{i}") for i in range(1, 101)]


@pytest.fixture
def cursor_codec():
    from terno_dbi.services.pagination import CursorCodec
    return CursorCodec("test-secret-key", ttl_seconds=3600)


@pytest.fixture
def telemetry_spy():
    from unittest.mock import MagicMock
    from terno_dbi.services.pagination.telemetry import PaginationTelemetry
    spy = PaginationTelemetry()
    spy.emit = MagicMock()
    return spy


@pytest.fixture
def cursor_factory():
    from terno_dbi.services.pagination import CursorCodec, OrderColumn
    import time
    import base64

    def _create(values, order_by=None, expired=False, tampered=False, wrong_key=False):
        order = order_by or [OrderColumn("id", "DESC")]
        key = "wrong-key" if wrong_key else "test-secret-key"
        ttl = -1 if expired else 3600
        codec = CursorCodec(key, ttl_seconds=ttl)
        cursor = codec.encode(values, order)
        if tampered:
            parts = cursor.split(".")
            cursor = parts[0] + ".TAMPERED_SIG123"
        return cursor

    return _create

