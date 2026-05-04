import pytest
from unittest.mock import MagicMock, patch
from typing import List
from terno_dbi.services.query import (
    _find_primary_key_order, execute_paginated_query,
    _infer_order_from_sql,
)
from terno_dbi.services.pagination import OrderColumn, PaginationMode


@pytest.mark.django_db
class TestAutoOrderInjection:
    """Tests for automatic ORDER BY injection using Primary Keys."""

    @patch('terno_dbi.services.query.models.Table')
    @patch('terno_dbi.services.query.models.TableColumn')
    def test_find_primary_key_order_success(self, mock_col_model, mock_table_model):
        """Should return the PK column when a valid table is found."""
        mock_table = MagicMock()
        mock_table.name = "users"
        mock_table_model.objects.filter.return_value.first.return_value = mock_table
        mock_col_model.objects.filter.return_value.values_list.return_value = ["id"]

        result = _find_primary_key_order("SELECT * FROM users", 1)

        assert len(result) == 1
        assert result[0].column == "id"
        assert result[0].direction == "ASC"

    @patch('terno_dbi.services.query.models.Table')
    def test_find_primary_key_order_no_table(self, mock_table_model):
        """Should return empty list if table is not in metadata."""
        mock_table_model.objects.filter.return_value.first.return_value = None
        result = _find_primary_key_order("SELECT * FROM ghost_table", 1)
        assert result == []

    def test_infer_order_from_sql_with_order_by(self):
        """Should extract ORDER BY columns from SQL."""
        result = _infer_order_from_sql("SELECT * FROM users ORDER BY id DESC")
        assert len(result) == 1
        assert result[0].column == "id"
        assert result[0].direction == "DESC"

    def test_infer_order_from_sql_no_order_by(self):
        """Should return empty list when no ORDER BY exists."""
        result = _infer_order_from_sql("SELECT * FROM users")
        assert result == []

    def test_infer_order_from_sql_handles_aliases(self):
        """Should correctly extract column names with table aliases."""
        sql = "SELECT u.name FROM users as u ORDER BY u.id ASC"
        result = _infer_order_from_sql(sql)
        assert len(result) == 1
        assert result[0].column == "id"
        assert result[0].direction == "ASC"


@pytest.mark.django_db
class TestTryAndFallback:
    """Tests for the try-and-fallback pagination pattern."""

    @patch('terno_dbi.services.query._find_primary_key_order')
    @patch('terno_dbi.services.query.PaginationService')
    @patch('terno_dbi.services.query.ConnectorFactory')
    def test_cursor_success_with_auto_injected_pk(self, mock_factory, mock_pagination, mock_find_pk):
        """When cursor pagination succeeds with injected PK, should use cursor mode."""
        mock_find_pk.return_value = [OrderColumn(column='id', direction='ASC')]

        datasource = MagicMock()
        datasource.id = 1

        mock_res = MagicMock()
        mock_res.attrs = {}
        mock_pagination.return_value.paginate.return_value = mock_res

        execute_paginated_query(
            datasource=datasource,
            native_sql="SELECT * FROM users",
            pagination_mode="cursor"
        )

        args, kwargs = mock_pagination.return_value.paginate.call_args
        config = args[1]
        assert config.order_by == [OrderColumn(column='id', direction='ASC')]
        assert config.mode == PaginationMode.CURSOR

    @patch('terno_dbi.services.query._find_primary_key_order')
    @patch('terno_dbi.services.query.PaginationService')
    @patch('terno_dbi.services.query.ConnectorFactory')
    def test_fallback_to_offset_on_cursor_failure(self, mock_factory, mock_pagination, mock_find_pk):
        """When cursor pagination crashes (e.g., SUM query), should auto-fallback to offset."""
        mock_find_pk.return_value = [OrderColumn(column='id', direction='ASC')]

        datasource = MagicMock()
        datasource.id = 1

        # First call (cursor) raises an error, second call (offset) succeeds
        mock_result = MagicMock()
        mock_result.warnings = []
        mock_result.columns = ['total']
        mock_result.data = [(42,)]
        mock_result.page = 1
        mock_result.per_page = 100
        mock_result.total_count = None
        mock_result.total_pages = None
        mock_result.has_next = False
        mock_result.has_prev = False
        mock_result.next_cursor = None
        mock_result.prev_cursor = None

        mock_pagination.return_value.paginate.side_effect = [
            Exception("column 'id' does not exist in subquery"),  # cursor fails
            mock_result,  # offset succeeds
        ]

        result = execute_paginated_query(
            datasource=datasource,
            native_sql="SELECT SUM(amount_total) FROM account_move",
            pagination_mode="cursor"
        )

        # Should have succeeded via offset fallback
        assert result['status'] == 'success'
        assert result['pagination_mode_used'] == 'offset'
        assert mock_pagination.return_value.paginate.call_count == 2

        # Verify the second call used offset mode with no order_by
        second_call_args = mock_pagination.return_value.paginate.call_args_list[1]
        fallback_config = second_call_args[0][1]
        assert fallback_config.mode == PaginationMode.OFFSET
        assert fallback_config.order_by == []

    @patch('terno_dbi.services.query._find_primary_key_order')
    @patch('terno_dbi.services.query.PaginationService')
    @patch('terno_dbi.services.query.ConnectorFactory')
    def test_no_fallback_when_user_provided_order(self, mock_factory, mock_pagination, mock_find_pk):
        """When the user explicitly provided ORDER BY and it fails, should NOT silently fallback."""
        datasource = MagicMock()
        datasource.id = 1

        mock_pagination.return_value.paginate.side_effect = Exception("bad query")

        result = execute_paginated_query(
            datasource=datasource,
            native_sql="SELECT * FROM users",
            pagination_mode="cursor",
            order_by=[{"column": "bad_col", "direction": "ASC"}]
        )

        # Should return an error, not silently fallback
        assert result['status'] == 'error'
        # _find_primary_key_order should NOT have been called (user provided order_by)
        mock_find_pk.assert_not_called()

    @patch('terno_dbi.services.query._find_primary_key_order')
    @patch('terno_dbi.services.query.PaginationService')
    @patch('terno_dbi.services.query.ConnectorFactory')
    def test_fallback_to_offset_when_no_pk_found(self, mock_factory, mock_pagination, mock_find_pk):
        """When no PK is found, should use offset mode without even trying cursor."""
        mock_find_pk.return_value = []

        datasource = MagicMock()
        datasource.id = 1

        mock_res = MagicMock()
        mock_res.attrs = {}
        mock_pagination.return_value.paginate.return_value = mock_res

        execute_paginated_query(
            datasource=datasource,
            native_sql="SELECT * FROM users",
            pagination_mode="cursor"
        )

        args, kwargs = mock_pagination.return_value.paginate.call_args
        config = args[1]
        assert config.mode == PaginationMode.OFFSET

    @patch('terno_dbi.services.query._find_primary_key_order')
    @patch('terno_dbi.services.query.PaginationService')
    @patch('terno_dbi.services.query.ConnectorFactory')
    def test_respects_existing_order_by_in_sql(self, mock_factory, mock_pagination, mock_find_pk):
        """Should NOT inject PK order if the SQL already has an ORDER BY."""
        datasource = MagicMock()
        datasource.id = 1

        mock_res = MagicMock()
        mock_res.attrs = {}
        mock_pagination.return_value.paginate.return_value = mock_res

        execute_paginated_query(
            datasource=datasource,
            native_sql="SELECT * FROM users ORDER BY created_at DESC",
            pagination_mode="cursor"
        )

        mock_find_pk.assert_not_called()

        args, kwargs = mock_pagination.return_value.paginate.call_args
        config = args[1]
        assert config.order_by[0].column == "created_at"
        assert config.order_by[0].direction == "DESC"
