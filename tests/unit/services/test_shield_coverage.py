import pytest
from unittest.mock import MagicMock, patch
from terno_dbi.services.shield import (
    generate_mdb,
    generate_native_sql,
    prepare_mdb,
    _keep_only_columns,
    _update_table_descriptions,
    _update_filters,
    _get_base_filters,
    _merge_grp_filters,
    get_cache_key
)

class TestShieldCoverage:
    
    @patch('terno_dbi.services.shield.models.TableColumn.objects.filter')
    @patch('terno_dbi.services.shield.models.Table.objects.filter')
    @patch('terno_dbi.services.shield.models.ForeignKey.objects.filter')
    @patch('terno_dbi.services.shield.MDatabase.from_data')
    def test_generate_mdb(self, mock_from_data, mock_fk_filter, mock_tbl_filter, mock_col_filter):
        datasource = MagicMock()
        
        mock_tbl = MagicMock()
        mock_tbl.name = "tbl1"
        mock_tbl.public_name = "PubTbl1"
        mock_tbl.description = "Desc1"
        mock_tbl_filter.return_value = [mock_tbl]
        
        mock_col = MagicMock()
        mock_col.name = "col1"
        mock_col.public_name = "PubCol1"
        mock_col.data_type = "VARCHAR"
        mock_col_filter.return_value = [mock_col]
        
        mock_fk = MagicMock()
        mock_fk.constrained_columns.name = "col1"
        mock_fk.referred_table.name = "tbl2"
        mock_fk.referred_columns.name = "id"
        mock_fk_filter.return_value = [mock_fk]
        
        mock_from_data.return_value = "MDB_OBJ"
        
        res = generate_mdb(datasource)
        
        assert res == "MDB_OBJ"
        mock_from_data.assert_called_once()
        tables, columns, fks = mock_from_data.call_args[0]
        
        assert "tbl1" in tables
        assert tables["tbl1"]["public_name"] == "PubTbl1"
        assert columns["tbl1"][0]["name"] == "col1"
        assert fks["tbl1"][0]["referred_table"] == "tbl2"
        
    @patch('terno_dbi.services.shield.Session')
    def test_generate_native_sql(self, mock_session):
        mock_sess_inst = MagicMock()
        mock_sess_inst.generateNativeSQL.return_value = "SELECT * FROM tbl1"
        mock_session.return_value = mock_sess_inst
        
        res = generate_native_sql("mDb", "sql", "postgres")
        assert res == {'status': 'success', 'native_sql': "SELECT * FROM tbl1"}
        
    @patch('terno_dbi.services.shield.cache.get')
    @patch('terno_dbi.services.shield.cache.set')
    @patch('terno_dbi.services.shield.get_admin_config_object')
    @patch('terno_dbi.services.shield.generate_mdb')
    @patch('terno_dbi.services.shield._keep_only_columns')
    @patch('terno_dbi.services.shield._update_table_descriptions')
    @patch('terno_dbi.services.shield._update_filters')
    def test_prepare_mdb(self, mock_upd_filt, mock_upd_desc, mock_keep_col, mock_gen_mdb, mock_get_admin, mock_cache_set, mock_cache_get):
        mock_cache_get.return_value = None
        
        ds = MagicMock()
        ds.id = 1
        roles = MagicMock()
        roles.values_list.return_value = [1, 2]
        
        allowed_tables = MagicMock()
        allowed_tables.values_list.return_value = ["tbl1"]
        allowed_columns = MagicMock()
        mock_get_admin.return_value = (allowed_tables, allowed_columns)
        
        mock_mdb = MagicMock()
        mock_mdb.get_table_dict.return_value = {"tbl1": MagicMock()}
        mock_gen_mdb.return_value = mock_mdb
        
        res = prepare_mdb(ds, roles)
        assert res == mock_mdb
        mock_cache_set.assert_called_once()
        mock_keep_col.assert_called_once_with(mock_mdb, allowed_tables, allowed_columns)
        mock_upd_desc.assert_called_once()
        mock_upd_filt.assert_called_once()
        
    @patch('terno_dbi.services.shield.models.TableColumn.objects.filter')
    def test_keep_only_columns_with_pub_name(self, mock_tc_filter):
        mock_mdb = MagicMock()
        class MockCol:
            def __init__(self, name):
                self.name = name
        class MockTbl:
            def __init__(self, name):
                self.name = name
                self.columns = {"col1": MockCol("col1")}
            def drop_columns(self, drop):
                pass
                
        mock_mdb.tables = {"tbl1": MockTbl("tbl1")}
        
        mock_tables = MagicMock()
        mock_tbl_obj = MagicMock()
        mock_tbl_obj.public_name = "PubTbl"
        mock_tc_filter.return_value.values_list.return_value = ["col1"]
        
        mock_tq = MagicMock()
        mock_tq.__bool__ = lambda self: True
        mock_tq.first.return_value = mock_tbl_obj
        mock_tables.filter.return_value = mock_tq
        
        mock_columns = MagicMock()
        mock_cq1 = MagicMock()
        mock_cq1.values_list.return_value = ["col1"]
        mock_columns.filter.side_effect = [mock_cq1, MagicMock(first=lambda: MagicMock(public_name="PubCol"))]
        
        _keep_only_columns(mock_mdb, mock_tables, mock_columns)
        assert mock_mdb.tables["tbl1"].columns["col1"].pub_name == "PubCol"
        
    def test_update_table_descriptions(self):
        # Build mock allowed tables with descriptions
        mock_allowed_tbl = MagicMock()
        mock_allowed_tbl.name = "tbl1"
        mock_allowed_tbl.description = "New Desc"
        
        allowed_tables = [mock_allowed_tbl]
        
        class MockTbl:
            desc = "Orig"
        tbl = MockTbl()
        tables = {"tbl1": tbl}
        _update_table_descriptions(tables, allowed_tables)
        assert tbl.desc == "New Desc"
        
    def test_update_table_descriptions_no_obj(self):
        # Empty allowed tables list
        allowed_tables = []
    
        class MockTbl:
            desc = "Orig"
        tbl = MockTbl()
        tables = {"tbl1": tbl}
        _update_table_descriptions(tables, allowed_tables)
        # Should remain unchanged because it's not in allowed_tables
        assert tbl.desc == "Orig"
        
    @patch('terno_dbi.services.shield._get_base_filters')
    @patch('terno_dbi.services.shield._get_grp_filters')
    def test_update_filters_has_filters(self, mock_get_grp, mock_get_base):
        mock_get_base.return_value = {"tbl1": ["1=1"]}
        mock_get_grp.return_value = {"tbl1": ["2=2"]}
        class MockTbl: pass
        tbl = MockTbl()
        tables = {"tbl1": tbl}
        _update_filters(tables, MagicMock(), [1])
        assert tbl.filters == "WHERE 1=1 AND  ( 2=2 ) "
        
    @patch('terno_dbi.services.shield.models.TableRowFilter.objects.filter')
    def test_get_base_filters_active(self, mock_trf_filter):
        mock_trf = MagicMock()
        mock_trf.filter_str = "col = 1"
        mock_trf.table.name = "tbl1"
        mock_trf_filter.return_value = [mock_trf]
        
        res = _get_base_filters(MagicMock())
        assert res == {"tbl1": ["(col = 1)"]}
        
    def test_merge_grp_filters(self):
        base = {"tbl1": ["a=1"]}
        grp = {"tbl1": ["b=1", "c=1"], "tbl2": ["d=1"]}
        _merge_grp_filters(base, grp)
        assert base["tbl1"][1] == " ( b=1 OR c=1 ) "
        assert base["tbl2"] == [" ( d=1 ) "]
        
    @patch('terno_dbi.services.shield.cache.get')
    def test_get_cache_key_empty(self, mock_get):
        mock_get.return_value = 0
        key = get_cache_key(1, [])
        assert key == "dbi_datasource_1_v0_roles_"


