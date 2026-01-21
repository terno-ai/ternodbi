"""
Integration tests for pagination with real database.

These tests require a running database and Django configuration.
Run with: pytest tests/integration/ -v
"""

import pytest
import os
import django
from terno_dbi.services.pagination import (
    PaginationService,
    PaginationConfig,
    PaginationMode,
    OrderColumn,
)

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dbi_server.settings')
django.setup()


class MockConnector:
    def __init__(self, rows, columns):
        self.rows = rows
        self.columns = columns

    def get_connection(self):
        return MockConnection(self.rows, self.columns)


class MockConnection:
    def __init__(self, rows, columns):
        self.rows = rows
        self.columns = columns

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def execute(self, query, params=None):
        return MockResult(self.rows, self.columns)


class MockResult:
    def __init__(self, rows, columns):
        self._rows = rows
        self._columns = columns
        self._rowcount = len(rows)

    def fetchall(self):
        return self._rows

    def keys(self):
        return self._columns

    def scalar(self):
        return len(self._rows)

    @property
    def rowcount(self):
        return self._rowcount


class TestPaginationServiceOffset:

    def test_first_page(self):
        rows = [(i, f"item_{i}") for i in range(1, 101)]
        connector = MockConnector(rows[:51], ["id", "name"])
        service = PaginationService(
            connector=connector,
            dialect="postgres",
            secret_key="test-secret"
        )

        config = PaginationConfig(
            mode=PaginationMode.OFFSET,
            page=1,
            per_page=50
        )

        result = service.paginate("SELECT * FROM items", config)

        assert len(result.data) == 50
        assert result.page == 1
        assert result.has_next is True
        assert result.has_prev is False

    def test_middle_page(self):
        rows = [(i, f"item_{i}") for i in range(51, 101)]
        connector = MockConnector(rows[:51], ["id", "name"])

        service = PaginationService(
            connector=connector,
            dialect="postgres",
            secret_key="test-secret"
        )

        config = PaginationConfig(
            mode=PaginationMode.OFFSET,
            page=2,
            per_page=50
        )

        result = service.paginate("SELECT * FROM items", config)

        assert result.page == 2
        assert result.has_prev is True

    def test_last_page(self):
        rows = [(i, f"item_{i}") for i in range(1, 26)]
        connector = MockConnector(rows, ["id", "name"])

        service = PaginationService(
            connector=connector,
            dialect="postgres",
            secret_key="test-secret"
        )

        config = PaginationConfig(
            mode=PaginationMode.OFFSET,
            page=1,
            per_page=50
        )

        result = service.paginate("SELECT * FROM items", config)

        assert len(result.data) == 25
        assert result.has_next is False


class TestPaginationServiceCursor:

    def test_first_page_no_cursor(self):
        rows = [(i, f"item_{i}") for i in range(100, 49, -1)]
        connector = MockConnector(rows[:51], ["id", "name"])

        service = PaginationService(
            connector=connector,
            dialect="postgres",
            secret_key="test-secret"
        )

        config = PaginationConfig(
            mode=PaginationMode.CURSOR,
            per_page=50,
            order_by=[OrderColumn("id", "DESC")]
        )

        result = service.paginate("SELECT * FROM items", config)

        assert len(result.data) == 50
        assert result.has_next is True
        assert result.has_prev is False
        assert result.next_cursor is not None

    def test_cursor_decode_and_use(self):
        rows = [(i, f"item_{i}") for i in range(100, 0, -1)]
        connector = MockConnector(rows[:51], ["id", "name"])

        service = PaginationService(
            connector=connector,
            dialect="postgres",
            secret_key="test-secret"
        )

        config1 = PaginationConfig(
            mode=PaginationMode.CURSOR,
            per_page=50,
            order_by=[OrderColumn("id", "DESC")]
        )
        result1 = service.paginate("SELECT * FROM items", config1)

        cursor = result1.next_cursor
        assert cursor is not None, f"Expected cursor but got None. has_next={result1.has_next}, data_len={len(result1.data)}"

        config2 = PaginationConfig(
            mode=PaginationMode.CURSOR,
            per_page=50,
            cursor=cursor,
            order_by=[OrderColumn("id", "DESC")]
        )

        result2 = service.paginate("SELECT * FROM items", config2)
        assert result2.has_prev is True


class TestPaginationServiceDialects:

    def test_postgres_limit_offset(self):
        connector = MockConnector([], ["id"])
        service = PaginationService(connector, "postgres", "secret")  
        sql = service._wrap_with_limit_offset("SELECT * FROM t", 10, 20)
        assert "LIMIT 10 OFFSET 20" in sql

    def test_mysql_limit_offset(self):
        connector = MockConnector([], ["id"])
        service = PaginationService(connector, "mysql", "secret")
        sql = service._wrap_with_limit_offset("SELECT * FROM t", 10, 20)
        assert "LIMIT 20, 10" in sql

    def test_oracle_rownum(self):
        connector = MockConnector([], ["id"])
        service = PaginationService(connector, "oracle", "secret")

        sql = service._wrap_with_limit_offset("SELECT * FROM t", 10, 20)
        assert "ROWNUM" in sql

    def test_snowflake_limit_offset(self):
        connector = MockConnector([], ["id"])
        service = PaginationService(connector, "snowflake", "secret")

        sql = service._wrap_with_limit_offset("SELECT * FROM t", 10, 20)
        assert "LIMIT 10 OFFSET 20" in sql


class TestPaginationServiceValidation:
    def test_warns_cursor_without_order_by(self):
        connector = MockConnector([(1, "a")], ["id", "name"])
        service = PaginationService(connector, "postgres", "secret")

        config = PaginationConfig(mode=PaginationMode.CURSOR)
        result = service.paginate("SELECT * FROM items", config)

        assert len(result.warnings) > 0
        assert any("ORDER BY" in w for w in result.warnings)

    def test_warns_deep_offset(self):
        """Test warning for deep offset pagination."""
        connector = MockConnector([(1, "a")], ["id", "name"])
        service = PaginationService(connector, "postgres", "secret")
        config = PaginationConfig(
            mode=PaginationMode.OFFSET,
            page=1000,
            per_page=50
        )
        result = service.paginate("SELECT * FROM items", config)

        assert len(result.warnings) > 0
        assert any("Deep offset" in w for w in result.warnings)


class TestCursorExpiration:

    def test_expired_cursor_rejected(self):
        import time
        from terno_dbi.services.pagination.codecs import CursorCodec
        codec = CursorCodec("secret", ttl_seconds=1)
        order = [OrderColumn("id", "DESC")]
        cursor = codec.encode({"id": 123}, order)

        time.sleep(1.5)

        with pytest.raises(ValueError) as exc_info:
            codec.decode(cursor)

        assert "expired" in str(exc_info.value).lower()

    def test_valid_cursor_accepted(self):
        from terno_dbi.services.pagination.codecs import CursorCodec

        codec = CursorCodec("secret", ttl_seconds=3600)
        order = [OrderColumn("id", "DESC")]
        cursor = codec.encode({"id": 456}, order)
        decoded = codec.decode(cursor)

        assert decoded["values"]["id"] == 456


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
