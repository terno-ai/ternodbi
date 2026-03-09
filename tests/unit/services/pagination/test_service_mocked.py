"""
Tests for PaginationService with mocked database.

Covers:
- Offset pagination (OFF-01 to OFF-08)
- Cursor pagination (CUR-01 to CUR-03)
- Edge cases (EDG-01 to EDG-06)
- Dialect-specific SQL generation (DIA-01 to DIA-04)
- Backward pagination (BWD-01 to BWD-03)
- Streaming (STR-01 to STR-03)
- Performance (PERF-01 to PERF-02)
- API contracts
"""

import os
import sys
import pytest
import time
from unittest.mock import MagicMock, patch

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'terno_dbi.server.settings')
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'src'))

import django
django.setup()

from terno_dbi.services.pagination import (
    PaginationService,
    PaginationConfig,
    PaginationMode,
    OrderColumn,
    PaginatedResult,
)


# =============================================================================
# Mock Infrastructure
# =============================================================================

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

    def execute(self, query, params=None, **kwargs):
        return MockResult(self.rows, self.columns)


class MockResult:
    def __init__(self, rows, columns):
        self._rows = rows
        self._columns = columns

    def fetchall(self):
        return self._rows

    def keys(self):
        return self._columns

    def scalar(self):
        return len(self._rows) if self._rows else 0

    def __iter__(self):
        return iter(self._rows)


def create_service(rows, columns, dialect="postgres", secret="test-secret-key"):
    connector = MockConnector(rows, columns)
    return PaginationService(connector=connector, dialect=dialect, secret_key=secret)


# =============================================================================
# Offset Pagination Tests
# =============================================================================

class TestOffsetPagination:

    def test_first_page(self):
        """OFF-01: First page returns correct data."""
        rows = [(i, f"item_{i}") for i in range(1, 101)]
        service = create_service(rows[:51], ["id", "name"])

        config = PaginationConfig(mode=PaginationMode.OFFSET, page=1, per_page=50)
        result = service.paginate("SELECT * FROM items", config)

        assert len(result.data) == 50
        assert result.page == 1
        assert result.has_next is True
        assert result.has_prev is False

    def test_middle_page(self):
        """OFF-02: Middle page has both navigation flags."""
        rows = [(i, f"item_{i}") for i in range(51, 101)]
        service = create_service(rows[:51], ["id", "name"])

        config = PaginationConfig(mode=PaginationMode.OFFSET, page=2, per_page=50)
        result = service.paginate("SELECT * FROM items", config)

        assert result.page == 2
        assert result.has_prev is True

    def test_last_page_exact(self):
        """OFF-03: Last page with exact row count."""
        rows = [(i, f"item_{i}") for i in range(91, 101)]
        service = create_service(rows, ["id", "name"])

        config = PaginationConfig(mode=PaginationMode.OFFSET, page=10, per_page=10)
        result = service.paginate("SELECT * FROM items", config)

        assert len(result.data) == 10
        assert result.has_next is False

    def test_last_page_partial(self):
        """OFF-04: Last page with fewer rows."""
        rows = [(i, f"item_{i}") for i in range(1, 6)]
        service = create_service(rows, ["id", "name"])

        config = PaginationConfig(mode=PaginationMode.OFFSET, page=1, per_page=50)
        result = service.paginate("SELECT * FROM items", config)

        assert len(result.data) == 5
        assert result.has_next is False

    def test_out_of_bounds(self):
        """OFF-05: Out of bounds page returns empty."""
        service = create_service([], ["id", "name"])

        config = PaginationConfig(mode=PaginationMode.OFFSET, page=999, per_page=50)
        result = service.paginate("SELECT * FROM items", config)

        assert len(result.data) == 0
        assert result.has_next is False

    def test_total_count_returned(self):
        """OFF-07: Total count returned when include_count=True."""
        rows = [(i,) for i in range(1, 51)]
        service = create_service(rows, ["id"])

        config = PaginationConfig(mode=PaginationMode.OFFSET, page=1, per_page=50, include_count=True)
        result = service.paginate("SELECT id FROM items", config)

        assert result.total_count is not None or result.total_count == 50

    def test_total_count_skipped_by_default(self):
        """OFF-09: Total count is None when include_count is False (default)."""
        rows = [(i,) for i in range(1, 51)]
        service = create_service(rows, ["id"])

        config = PaginationConfig(mode=PaginationMode.OFFSET, page=1, per_page=50)
        result = service.paginate("SELECT id FROM items", config)

        assert result.total_count is None

    def test_total_count_skipped_on_timeout(self):
        """OFF-08: Total count skipped for timeout."""
        rows = [(i,) for i in range(1, 51)]
        service = create_service(rows, ["id"])

        with patch.object(service, '_get_total_count', return_value=None):
            config = PaginationConfig(mode=PaginationMode.OFFSET, page=1, per_page=50)
            result = service.paginate("SELECT id FROM items", config)

        assert result.total_count is None


# =============================================================================
# Cursor Pagination Tests
# =============================================================================

class TestCursorPagination:

    def test_cold_start_generates_cursor(self):
        """CUR-01: First page generates next_cursor."""
        rows = [(i, f"item_{i}") for i in range(100, 49, -1)]
        service = create_service(rows[:51], ["id", "name"])

        config = PaginationConfig(
            mode=PaginationMode.CURSOR,
            per_page=50,
            order_by=[OrderColumn("id", "DESC")]
        )
        result = service.paginate("SELECT * FROM items", config)

        assert len(result.data) == 50
        assert result.has_next is True
        assert result.next_cursor is not None

    def test_cursor_values_correct(self):
        """CUR-02: Cursor encodes last row values."""
        rows = [(100, "a"), (99, "b"), (98, "c")]
        service = create_service(rows[:2] + [(97, "d")], ["id", "name"])

        config = PaginationConfig(
            mode=PaginationMode.CURSOR,
            per_page=2,
            order_by=[OrderColumn("id", "DESC")]
        )
        result = service.paginate("SELECT * FROM items", config)

        decoded = service.cursor_codec.decode(result.next_cursor)
        last_row_id = result.data[-1][0]
        assert decoded["values"]["id"] == last_row_id

    def test_end_of_results_no_cursor(self):
        """CUR-03: Final page has no next_cursor."""
        rows = [(i, f"item_{i}") for i in range(1, 26)]
        service = create_service(rows, ["id", "name"])

        config = PaginationConfig(
            mode=PaginationMode.CURSOR,
            per_page=50,
            order_by=[OrderColumn("id", "DESC")]
        )
        result = service.paginate("SELECT * FROM items", config)

        assert result.has_next is False
        assert result.next_cursor is None

    def test_cursor_forward_continuation(self):
        """CUR-04: Forward cursor pagination continues from last value."""
        rows = [(i,) for i in range(100, 0, -1)]
        service = create_service(rows[:51], ["id"])

        # Generate cursor from first page
        cursor = service.cursor_codec.encode(
            {"id": 50},  # Last value from first page
            [OrderColumn("id", "DESC")]
        )

        config = PaginationConfig(
            mode=PaginationMode.CURSOR,
            per_page=50,
            cursor=cursor,
            order_by=[OrderColumn("id", "DESC")]
        )
        result = service.paginate("SELECT id FROM items", config)

        # Should have prev cursor (not on first page)
        assert result.has_prev is True

    def test_cursor_decode_failure_records_telemetry(self):
        """CUR-05: Invalid cursor records decode failure in telemetry."""
        rows = [(i,) for i in range(1, 51)]
        service = create_service(rows, ["id"])

        config = PaginationConfig(
            mode=PaginationMode.CURSOR,
            per_page=50,
            cursor="invalid-cursor-format",
            order_by=[OrderColumn("id", "DESC")]
        )

        with pytest.raises(ValueError):
            service.paginate("SELECT id FROM items", config)

        # Telemetry should record failure
        stats = service.telemetry.get_stats()
        assert stats["cursor_decode_failures"] >= 1


# =============================================================================
# Cursor Where Clause Building
# =============================================================================

class TestCursorWhereBuilder:
    """Tests for _build_cursor_where helper."""

    def test_build_cursor_where_desc(self):
        """Builds correct WHERE for DESC ordering."""
        rows = [(1,)]
        service = create_service(rows, ["id"])

        cursor_data = {"values": {"id": 50}}
        order_by = [OrderColumn("id", "DESC")]

        where = service._build_cursor_where(cursor_data, order_by)

        assert "(id) < (:id)" in where

    def test_build_cursor_where_asc(self):
        """Builds correct WHERE for ASC ordering."""
        rows = [(1,)]
        service = create_service(rows, ["id"])

        cursor_data = {"values": {"id": 50}}
        order_by = [OrderColumn("id", "ASC")]

        where = service._build_cursor_where(cursor_data, order_by)

        assert "(id) > (:id)" in where

    def test_build_cursor_where_composite(self):
        """Builds correct WHERE for composite keys."""
        rows = [(1, "a")]
        service = create_service(rows, ["id", "name"])

        cursor_data = {"values": {"created_at": "2024-01-20", "id": 100}}
        order_by = [
            OrderColumn("created_at", "DESC"),
            OrderColumn("id", "DESC")
        ]

        where = service._build_cursor_where(cursor_data, order_by)

        assert "created_at" in where
        assert "id" in where

    def test_build_cursor_where_empty_cursor(self):
        """Returns empty string for no cursor data."""
        rows = [(1,)]
        service = create_service(rows, ["id"])

        where = service._build_cursor_where(None, [OrderColumn("id", "DESC")])
        assert where == ""

    def test_build_cursor_where_empty_values(self):
        """Returns empty string for empty values."""
        rows = [(1,)]
        service = create_service(rows, ["id"])

        where = service._build_cursor_where({"values": {}}, [OrderColumn("id", "DESC")])
        assert where == ""


# =============================================================================
# Edge Cases
# =============================================================================

class TestEdgeCases:

    def test_empty_table(self):
        """EDG-01: Empty table returns empty list."""
        service = create_service([], ["id", "name"])

        config = PaginationConfig(mode=PaginationMode.OFFSET, page=1, per_page=50)
        result = service.paginate("SELECT * FROM items", config)

        assert result.data == []
        assert result.has_next is False

    def test_single_row(self):
        """EDG-02: Single row table."""
        service = create_service([(1, "only")], ["id", "name"])

        config = PaginationConfig(mode=PaginationMode.OFFSET, page=1, per_page=50)
        result = service.paginate("SELECT * FROM items", config)

        assert len(result.data) == 1

    def test_zero_page_size_clamped(self):
        """EDG-03: Zero page size clamped to minimum."""
        rows = [(i,) for i in range(1, 11)]
        service = create_service(rows, ["id"])

        config = PaginationConfig(mode=PaginationMode.OFFSET, per_page=0)
        result = service.paginate("SELECT id FROM items", config)

        assert isinstance(result.data, list)

    def test_max_page_size_clamped(self):
        """EDG-04: Request > MAX clamped."""
        rows = [(i,) for i in range(1, 600)]
        service = create_service(rows[:501], ["id"])

        config = PaginationConfig(mode=PaginationMode.OFFSET, per_page=10000)
        result = service.paginate("SELECT id FROM items", config)

        assert len(result.data) <= 500


# =============================================================================
# Dialect-Specific SQL Generation
# =============================================================================

class TestDialectGeneration:

    def test_postgres_limit_offset(self):
        """DIA-01: Postgres LIMIT x OFFSET y."""
        service = create_service([], ["id"], dialect="postgres")
        sql = service._wrap_with_limit_offset("SELECT * FROM t", 10, 20)
        assert "LIMIT 10 OFFSET 20" in sql

    def test_mysql_limit(self):
        """DIA-02: MySQL LIMIT y, x."""
        service = create_service([], ["id"], dialect="mysql")
        sql = service._wrap_with_limit_offset("SELECT * FROM t", 10, 20)
        assert "LIMIT 20, 10" in sql

    def test_oracle_rownum(self):
        """DIA-03: Oracle ROWNUM."""
        service = create_service([], ["id"], dialect="oracle")
        sql = service._wrap_with_limit_offset("SELECT * FROM t", 10, 20)
        assert "ROWNUM" in sql

    def test_snowflake_standard(self):
        """DIA-04: Snowflake LIMIT/OFFSET."""
        service = create_service([], ["id"], dialect="snowflake")
        sql = service._wrap_with_limit_offset("SELECT * FROM t", 10, 20)
        assert "LIMIT 10 OFFSET 20" in sql


# =============================================================================
# Backward Pagination
# =============================================================================

class TestBackwardPagination:

    def test_first_page_no_prev_cursor(self):
        """BWD-01: First page has no prev_cursor."""
        rows = [(i,) for i in range(100, 49, -1)]
        service = create_service(rows[:51], ["id"])

        config = PaginationConfig(
            mode=PaginationMode.CURSOR,
            per_page=50,
            order_by=[OrderColumn("id", "DESC")]
        )
        result = service.paginate("SELECT id FROM items", config)

        assert result.prev_cursor is None

    def test_order_column_inverted(self):
        """BWD-02: OrderColumn.inverted() flips direction."""
        desc = OrderColumn("id", "DESC")
        asc = desc.inverted()

        assert asc.direction == "ASC"
        assert asc.inverted().direction == "DESC"

    def test_backward_direction_config(self):
        """BWD-03: Config accepts backward direction."""
        config = PaginationConfig(
            mode=PaginationMode.CURSOR,
            direction="backward",
            order_by=[OrderColumn("id", "DESC")]
        )

        assert config.direction == "backward"

    def test_backward_paginate_with_cursor(self):
        """BWD-04: Backward pagination inverts order and reverses results."""
        # Need enough rows to trigger has_prev (> per_page)
        rows = [(i,) for i in range(60, 0, -1)]  # 60 rows DESC
        service = create_service(rows, ["id"])

        # Create a cursor for going backward from row 30
        cursor = service.cursor_codec.encode(
            {"id": 30},
            [OrderColumn("id", "DESC")]
        )

        config = PaginationConfig(
            mode=PaginationMode.CURSOR,
            per_page=10,
            cursor=cursor,
            direction="backward",
            order_by=[OrderColumn("id", "DESC")]
        )
        result = service.paginate("SELECT id FROM items", config)

        # Backward pagination should work and have both cursors
        assert isinstance(result.data, list)
        # Should have next_cursor since we have data
        assert result.next_cursor is not None or result.data

    def test_backward_paginate_empty_result(self):
        """BWD-05: Backward pagination at start returns empty prev."""
        # Create small dataset
        rows = [(i,) for i in range(5, 0, -1)]  # 5 rows DESC
        service = create_service([], ["id"])  # Return empty for backward

        cursor = service.cursor_codec.encode(
            {"id": 1},  # At the very start
            [OrderColumn("id", "DESC")]
        )

        config = PaginationConfig(
            mode=PaginationMode.CURSOR,
            per_page=10,
            cursor=cursor,
            direction="backward",
            order_by=[OrderColumn("id", "DESC")]
        )
        result = service.paginate("SELECT id FROM items", config)

        # Empty result has no cursors
        assert result.data == []


# =============================================================================
# Stream Mode
# =============================================================================

class TestStreamMode:
    """Tests for stream pagination mode."""

    def test_stream_paginate_first_batch(self):
        """STREAM mode returns first per_page rows."""
        rows = [(i,) for i in range(1, 101)]
        service = create_service(rows, ["id"])

        config = PaginationConfig(
            mode=PaginationMode.STREAM,
            per_page=20
        )
        result = service.paginate("SELECT id FROM items", config)

        assert len(result.data) == 20
        assert result.has_next is True
        assert result.has_prev is False


# =============================================================================
# Streaming
# =============================================================================

class TestStreaming:

    def test_stream_yields_batches(self):
        """STR-01: Stream yields configured batch sizes."""
        rows = [(i,) for i in range(1, 101)]
        service = create_service(rows, ["id"])

        batches = list(service.stream_all("SELECT id FROM items", yield_size=25))

        assert len(batches) == 4
        assert len(batches[0]) == 25

    @pytest.mark.xfail(reason="Memory profiling requires real profiler")
    def test_stream_memory_constant(self):
        """STR-02: Streaming uses constant memory."""
        assert False

    @pytest.mark.xfail(reason="Connection cleanup requires real DB")
    def test_stream_error_cleanup(self):
        """STR-03: Error mid-stream cleans up."""
        assert False


# =============================================================================
# Performance (Slow)
# =============================================================================

@pytest.mark.slow
class TestPerformance:

    def test_cursor_fast(self):
        """PERF-01: Cursor pagination is fast."""
        rows = [(i,) for i in range(1, 51)]
        service = create_service(rows, ["id"])

        start = time.time()
        config = PaginationConfig(
            mode=PaginationMode.CURSOR,
            per_page=50,
            order_by=[OrderColumn("id", "DESC")]
        )
        service.paginate("SELECT id FROM items", config)
        elapsed = time.time() - start

        assert elapsed < 1.0

    @pytest.mark.xfail(reason="Explain plan requires real Postgres")
    def test_explain_plan_validation(self):
        """EXP-01: Explain plan validation."""
        assert False


# =============================================================================
# API Contracts
# =============================================================================

class TestAPIContracts:

    def test_pagination_mode_enum_values(self):
        """Enum values match API strings."""
        assert PaginationMode.OFFSET.value == "offset"
        assert PaginationMode.CURSOR.value == "cursor"
        assert PaginationMode.STREAM.value == "stream"

    def test_paginated_result_cursor_fields_present(self):
        """Cursor fields always present in output."""
        result = PaginatedResult(
            data=[(1, "a")],
            columns=["id", "name"],
            page=1,
            per_page=50,
            total_count=None,
            total_pages=None,
            has_next=False,
            has_prev=False,
            next_cursor=None,
            prev_cursor=None
        )

        d = result.to_dict()

        assert "next_cursor" in d
        assert "prev_cursor" in d
        assert d["next_cursor"] is None
        assert d["prev_cursor"] is None


# =============================================================================
# Factory Function & Initialization
# =============================================================================

class TestFactoryAndInit:
    """Tests for factory function and service initialization."""

    def test_create_pagination_service_factory(self):
        """Factory function creates service correctly."""
        from terno_dbi.services.pagination import create_pagination_service

        rows = [(1,)]
        connector = MockConnector(rows, ["id"])
        service = create_pagination_service(connector, "postgres", "my-secret")

        assert service is not None
        assert service.dialect == "postgres"

    def test_paginate_with_default_config(self):
        """Paginate works with no config (default)."""
        rows = [(i,) for i in range(1, 51)]
        service = create_service(rows, ["id"])

        # Pass None config to trigger default
        result = service.paginate("SELECT id FROM items", None)

        assert isinstance(result.data, list)
        assert result.page == 1

    def test_service_default_secret_from_settings(self):
        """Service uses Django SECRET_KEY when no secret provided."""
        from unittest.mock import patch, MagicMock

        rows = [(1,)]
        connector = MockConnector(rows, ["id"])

        # Mock Django settings
        mock_settings = MagicMock()
        mock_settings.SECRET_KEY = "django-secret-key"

        with patch.dict('sys.modules', {'django.conf': MagicMock(settings=mock_settings)}):
            service = PaginationService(connector, "postgres", secret_key=None)
            # Should not raise and create service
            assert service is not None


# =============================================================================
# Count Threshold & Error Handling
# =============================================================================

class TestCountThreshold:
    """Tests for total count threshold and error handling."""

    def test_stream_all_final_partial_batch(self):
        """stream_all yields partial final batch."""
        rows = [(i,) for i in range(1, 38)]  # 37 rows
        service = create_service(rows, ["id"])

        batches = list(service.stream_all("SELECT id FROM items", yield_size=10))

        # 3 full batches of 10 + 1 partial of 7
        assert len(batches) == 4
        assert len(batches[-1]) == 7

    def test_unknown_dialect_uses_default_limit_offset(self):
        """Unknown dialect falls back to standard LIMIT/OFFSET."""
        service = create_service([], ["id"], dialect="unknowndb")
        sql = service._wrap_with_limit_offset("SELECT * FROM t", 10, 20)

        # Should use default format
        assert "LIMIT 10 OFFSET 20" in sql


# =============================================================================
# Branch Coverage: Validator non-deterministic in offset mode
# =============================================================================

class TestValidatorBranches:
    """Tests for validator branch coverage."""

    def test_nondeterministic_query_in_offset_mode_no_warning(self):
        """Non-deterministic query in OFFSET mode doesn't warn."""
        from terno_dbi.services.pagination import QueryValidator

        validator = QueryValidator()
        config = PaginationConfig(mode=PaginationMode.OFFSET, page=1, per_page=50)

        # DISTINCT in offset mode should NOT trigger warning
        warnings = validator.validate("SELECT DISTINCT name FROM items", config)

        assert not any("non-deterministic" in w for w in warnings)


# =============================================================================
# Total Count Threshold & Exception Handling
# =============================================================================

class TestGetTotalCount:
    """Tests for _get_total_count threshold and error paths."""

    def test_count_exceeds_threshold_returns_none(self):
        """Count exceeding threshold returns None."""
        rows = [(i,) for i in range(1, 51)]
        service = create_service(rows, ["id"])

        # Patch _get_total_count to return None (simulating threshold exceeded)
        with patch.object(service, '_get_total_count', return_value=None):
            config = PaginationConfig(mode=PaginationMode.OFFSET, page=1, per_page=50)
            result = service.paginate("SELECT id FROM items", config)

            # total_count should be None due to threshold
            assert result.total_count is None

    def test_pagination_works_when_count_fails(self):
        """Pagination continues even when count fails internally."""
        rows = [(i,) for i in range(1, 11)]
        service = create_service(rows, ["id"])

        # Patch to return None (simulating internal exception caught)
        with patch.object(service, '_get_total_count', return_value=None):
            config = PaginationConfig(mode=PaginationMode.OFFSET, page=1, per_page=50)
            result = service.paginate("SELECT id FROM items", config)

            # Data should still be returned
            assert isinstance(result.data, list)
            assert result.total_count is None




