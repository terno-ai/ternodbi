import pytest
from terno_dbi import _lazy_import_services

def test_lazy_import_services():
    """Test that _lazy_import_services returns the expected dictionary of functions."""
    result = _lazy_import_services()
    
    assert isinstance(result, dict)
    expected_keys = {
        'prepare_mdb',
        'generate_mdb',
        'generate_native_sql',
        'execute_native_sql',
        'execute_native_sql_return_df',
        'get_all_group_tables',
        'get_all_group_columns',
    }
    assert set(result.keys()) == expected_keys
    
    # Verify these are functions
    for key, func in result.items():
        assert callable(func)
