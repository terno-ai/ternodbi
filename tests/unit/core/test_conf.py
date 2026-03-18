import pytest
from unittest.mock import patch
from django.conf import settings
from terno_dbi.core import conf

class TestConf:
    def test_get_all_no_settings(self):
        with patch('terno_dbi.core.conf.getattr', return_value={}):
            result = conf.get_all()
            assert result['DEFAULT_PAGE_SIZE'] == 50

    def test_get_all_with_settings(self):
        with patch('terno_dbi.core.conf.getattr', return_value={"DEFAULT_PAGE_SIZE": 100}):
            result = conf.get_all()
            assert result['DEFAULT_PAGE_SIZE'] == 100
