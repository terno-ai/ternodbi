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


