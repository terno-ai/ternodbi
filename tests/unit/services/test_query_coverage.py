import pytest
from unittest.mock import MagicMock, patch
from terno_dbi.services.query import _infer_order_from_sql, execute_paginated_query

class TestQueryCoverage:

    def test_infer_order_from_sql_with_group(self):
        sql = "SELECT id, count(*) FROM users GROUP BY id"
        res = _infer_order_from_sql(sql)
        assert res == []

    def test_infer_order_from_sql_no_order(self):
        sql = "SELECT id FROM users"
        res = _infer_order_from_sql(sql)
        assert res == []

    def test_infer_order_from_sql_with_order(self):
        sql = "SELECT id FROM users ORDER BY id DESC"
        res = _infer_order_from_sql(sql)
        assert len(res) == 1
        assert res[0].column == "id"
        assert res[0].direction == "DESC"

    def test_infer_order_from_sql_with_order_asc(self):
        sql = "SELECT id FROM users ORDER BY id ASC"
        res = _infer_order_from_sql(sql)
        assert len(res) == 1
        assert res[0].column == "id"
        assert res[0].direction == "ASC"



    @patch('sqlglot.parse_one')
    def test_infer_order_from_sql_exception(self, mock_parse):
        mock_parse.side_effect = Exception("parse error")
        sql = "SELECT id FROM users ORDER BY id DESC"
        res = _infer_order_from_sql(sql)
        assert res == []

    @patch('terno_dbi.services.query.PaginationService')
    @patch('terno_dbi.services.query.ConnectorFactory')
    @patch('terno_dbi.services.query._infer_order_from_sql')
    def test_execute_paginated_query_cursor_fallback_offset(self, mock_infer, mock_factory, mock_service):
        mock_infer.return_value = []
        datasource = MagicMock()
        
        mock_result = MagicMock()
        mock_result.columns = ['id']
        mock_result.data = [[1]]
        mock_result.page = 1
        mock_result.per_page = 10
        mock_result.total_count = 1
        mock_result.total_pages = 1
        mock_result.has_next = False
        mock_result.has_prev = False
        mock_result.next_cursor = None
        mock_result.prev_cursor = None
        mock_result.warnings = []
        
        mock_service.return_value.paginate.return_value = mock_result
        
        # Test fallback: cursor mode without order_by and no inferred order -> fallback to offset
        response = execute_paginated_query(
            datasource=datasource,
            native_sql="SELECT id FROM users",
            pagination_mode="cursor"
        )
        assert response['pagination_mode_used'] == "offset"

    @patch('terno_dbi.services.query.PaginationService')
    @patch('terno_dbi.services.query.ConnectorFactory')
    @patch('terno_dbi.services.query._infer_order_from_sql')
    def test_execute_paginated_query_cursor_infer_success(self, mock_infer, mock_factory, mock_service):
        from terno_dbi.services.pagination import OrderColumn
        mock_infer.return_value = [OrderColumn(column='id', direction='DESC')]
        datasource = MagicMock()
        
        mock_result = MagicMock()
        mock_result.columns = ['id']
        mock_result.data = [[1]]
        mock_result.page = 1
        mock_result.per_page = 10
        mock_result.total_count = 1
        mock_result.total_pages = 1
        mock_result.has_next = False
        mock_result.has_prev = False
        mock_result.next_cursor = None
        mock_result.prev_cursor = None
        mock_result.warnings = []
        
        mock_service.return_value.paginate.return_value = mock_result
        
        # Test success: cursor mode auto detects order by
        response = execute_paginated_query(
            datasource=datasource,
            native_sql="SELECT id FROM users ORDER BY id DESC",
            pagination_mode="cursor"
        )
        assert response['pagination_mode_used'] == "cursor"
