"""
Tests for QueryValidator.

Covers:
- ORDER BY warnings for cursor mode
- Non-deterministic query warnings
- Deep offset warnings
"""

import os
import sys
import pytest

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dbi_server.settings')
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'src'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'server'))

import django
django.setup()

from terno_dbi.services.pagination import (
    PaginationConfig,
    PaginationMode,
    QueryValidator,
)


class TestQueryValidatorCursor:
    """Cursor mode validation tests."""

    def test_warns_cursor_without_order_by(self):
        """Cursor mode without ORDER BY triggers warning."""
        validator = QueryValidator()
        config = PaginationConfig(mode=PaginationMode.CURSOR)
        warnings = validator.validate("SELECT * FROM items", config)

        assert len(warnings) > 0
        assert any("ORDER BY" in w for w in warnings)

    def test_warns_cursor_with_distinct(self):
        """Cursor mode with DISTINCT triggers warning."""
        validator = QueryValidator()
        config = PaginationConfig(mode=PaginationMode.CURSOR)
        warnings = validator.validate(
            "SELECT DISTINCT name FROM items ORDER BY name", config
        )

        assert any("non-deterministic" in w for w in warnings)

    def test_no_warning_valid_cursor_query(self):
        """Valid cursor query with ORDER BY has no warnings."""
        validator = QueryValidator()
        config = PaginationConfig(mode=PaginationMode.CURSOR)
        warnings = validator.validate(
            "SELECT * FROM items ORDER BY id DESC", config
        )

        # Should only warn about missing ORDER BY, not other issues
        assert not any("non-deterministic" in w for w in warnings)


class TestQueryValidatorOffset:
    """Offset mode validation tests."""

    def test_warns_deep_offset(self):
        """Deep offset (>10000) triggers warning."""
        validator = QueryValidator()
        config = PaginationConfig(mode=PaginationMode.OFFSET, page=1000, per_page=50)
        warnings = validator.validate("SELECT * FROM items", config)

        assert len(warnings) > 0
        assert any("Deep offset" in w for w in warnings)

    def test_no_warning_shallow_offset(self):
        """Shallow offset has no warnings."""
        validator = QueryValidator()
        config = PaginationConfig(mode=PaginationMode.OFFSET, page=10, per_page=50)
        warnings = validator.validate("SELECT * FROM items", config)

        assert len(warnings) == 0

    def test_no_warning_first_page(self):
        """First page has no warnings."""
        validator = QueryValidator()
        config = PaginationConfig(mode=PaginationMode.OFFSET, page=1, per_page=50)
        warnings = validator.validate("SELECT * FROM items", config)

        assert len(warnings) == 0
