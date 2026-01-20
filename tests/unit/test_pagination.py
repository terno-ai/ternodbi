"""
Unit tests for the pagination service.

Tests cover:
- Cursor encoding/decoding with HMAC verification
- Query validation warnings
- Pagination config and result dataclasses
- Dialect-specific SQL generation
"""

import pytest
import json
import base64
from unittest.mock import MagicMock, patch

from terno_dbi.services.pagination import (
    PaginationMode,
    OrderColumn,
    PaginationConfig,
    PaginatedResult,
    CursorCodec,
    QueryValidator,
    PaginationTelemetry,
    PaginationMetrics,
)


class TestOrderColumn:
    """Tests for OrderColumn dataclass."""
    
    def test_default_values(self):
        col = OrderColumn("id")
        assert col.column == "id"
        assert col.direction == "DESC"
        assert col.nulls == "LAST"
    
    def test_custom_values(self):
        col = OrderColumn("created_at", "ASC", "FIRST")
        assert col.column == "created_at"
        assert col.direction == "ASC"
        assert col.nulls == "FIRST"
    
    def test_inverted_desc_to_asc(self):
        col = OrderColumn("id", "DESC")
        inverted = col.inverted()
        assert inverted.direction == "ASC"
        assert inverted.column == "id"
    
    def test_inverted_asc_to_desc(self):
        col = OrderColumn("id", "ASC")
        inverted = col.inverted()
        assert inverted.direction == "DESC"


class TestPaginationConfig:
    """Tests for PaginationConfig dataclass."""
    
    def test_default_values(self):
        config = PaginationConfig()
        assert config.mode == PaginationMode.OFFSET
        assert config.page == 1
        assert config.per_page == 50
        assert config.cursor is None
        assert config.direction == "forward"
        assert len(config.order_by) == 1
        assert config.order_by[0].column == "id"
    
    def test_cursor_mode(self):
        config = PaginationConfig(
            mode=PaginationMode.CURSOR,
            cursor="abc123",
            per_page=100
        )
        assert config.mode == PaginationMode.CURSOR
        assert config.cursor == "abc123"
        assert config.per_page == 100


class TestCursorCodec:
    """Tests for CursorCodec with HMAC signing."""
    
    def setup_method(self):
        self.codec = CursorCodec("test-secret-key")
        self.order_by = [OrderColumn("id", "DESC")]
    
    def test_encode_decode_roundtrip(self):
        values = {"id": 123, "created_at": "2024-01-20"}
        
        cursor = self.codec.encode(values, self.order_by)
        decoded = self.codec.decode(cursor)
        
        assert decoded["v"] == 1
        assert decoded["values"]["id"] == 123
        assert decoded["values"]["created_at"] == "2024-01-20"
    
    def test_cursor_contains_signature(self):
        values = {"id": 456}
        cursor = self.codec.encode(values, self.order_by)
        
        # Cursor format: base64_payload.signature
        parts = cursor.split(".")
        assert len(parts) == 2
        assert len(parts[1]) == 16  # Truncated HMAC
    
    def test_invalid_signature_rejected(self):
        values = {"id": 789}
        cursor = self.codec.encode(values, self.order_by)
        
        # Tamper with the signature
        parts = cursor.split(".")
        tampered = parts[0] + ".000000000000000a"
        
        with pytest.raises(ValueError) as exc_info:
            self.codec.decode(tampered)
        
        assert "Invalid cursor signature" in str(exc_info.value)
    
    def test_tampered_payload_rejected(self):
        values = {"id": 100}
        cursor = self.codec.encode(values, self.order_by)
        
        parts = cursor.split(".")
        # Decode, tamper, re-encode payload
        payload = json.loads(base64.urlsafe_b64decode(parts[0]))
        payload["values"]["id"] = 999
        tampered_payload = base64.urlsafe_b64encode(
            json.dumps(payload).encode()
        ).decode()
        tampered = tampered_payload + "." + parts[1]
        
        with pytest.raises(ValueError):
            self.codec.decode(tampered)
    
    def test_invalid_cursor_format(self):
        with pytest.raises(ValueError) as exc_info:
            self.codec.decode("not-a-valid-cursor")
        
        assert "Invalid cursor" in str(exc_info.value)


class TestQueryValidator:
    """Tests for QueryValidator warnings."""
    
    def setup_method(self):
        self.validator = QueryValidator()
    
    def test_no_warnings_for_valid_offset_query(self):
        config = PaginationConfig(mode=PaginationMode.OFFSET)
        warnings = self.validator.validate(
            "SELECT * FROM users ORDER BY id",
            config
        )
        assert len(warnings) == 0
    
    def test_warn_cursor_without_order_by(self):
        config = PaginationConfig(mode=PaginationMode.CURSOR)
        warnings = self.validator.validate(
            "SELECT * FROM users",
            config
        )
        assert len(warnings) == 1
        assert "ORDER BY" in warnings[0]
    
    def test_warn_cursor_with_distinct(self):
        config = PaginationConfig(mode=PaginationMode.CURSOR)
        warnings = self.validator.validate(
            "SELECT DISTINCT name FROM users ORDER BY id",
            config
        )
        assert len(warnings) == 1
        assert "non-deterministic" in warnings[0]
    
    def test_warn_deep_offset(self):
        config = PaginationConfig(
            mode=PaginationMode.OFFSET,
            page=1001,  # offset = 50000
            per_page=50
        )
        warnings = self.validator.validate(
            "SELECT * FROM users",
            config
        )
        assert len(warnings) == 1
        assert "Deep offset" in warnings[0]
    
    def test_no_warn_shallow_offset(self):
        config = PaginationConfig(
            mode=PaginationMode.OFFSET,
            page=10,  # offset = 450
            per_page=50
        )
        warnings = self.validator.validate(
            "SELECT * FROM users",
            config
        )
        assert len(warnings) == 0


class TestPaginationTelemetry:
    """Tests for PaginationTelemetry hooks."""
    
    def test_default_counters(self):
        telemetry = PaginationTelemetry()
        stats = telemetry.get_stats()
        
        assert stats["cursor_decode_failures"] == 0
        assert stats["fallback_activations"] == 0
        assert stats["deep_offset_count"] == 0
    
    def test_record_cursor_failure(self):
        telemetry = PaginationTelemetry()
        telemetry.record_cursor_decode_failure()
        telemetry.record_cursor_decode_failure()
        
        stats = telemetry.get_stats()
        assert stats["cursor_decode_failures"] == 2
    
    def test_record_fallback(self):
        telemetry = PaginationTelemetry()
        telemetry.record_fallback()
        
        stats = telemetry.get_stats()
        assert stats["fallback_activations"] == 1
    
    def test_record_deep_offset(self):
        telemetry = PaginationTelemetry()
        telemetry.record_deep_offset(50000)
        
        stats = telemetry.get_stats()
        assert stats["deep_offset_count"] == 1
    
    def test_custom_emit_function(self):
        emitted = []
        
        def capture(metrics):
            emitted.append(metrics)
        
        telemetry = PaginationTelemetry(emit_fn=capture)
        telemetry.emit(PaginationMetrics(
            mode="offset",
            duration_ms=100.5,
            rows_returned=50
        ))
        
        assert len(emitted) == 1
        assert emitted[0].mode == "offset"
        assert emitted[0].duration_ms == 100.5


class TestPaginatedResult:
    """Tests for PaginatedResult dataclass."""
    
    def test_to_dict(self):
        result = PaginatedResult(
            data=[(1, "Alice"), (2, "Bob")],
            columns=["id", "name"],
            page=1,
            per_page=10,
            total_count=100,
            total_pages=10,
            has_next=True,
            has_prev=False,
            next_cursor="abc",
            prev_cursor=None
        )
        
        d = result.to_dict()
        
        assert d["data"] == [[1, "Alice"], [2, "Bob"]]
        assert d["columns"] == ["id", "name"]
        assert d["page"] == 1
        assert d["has_next"] is True
        assert d["next_cursor"] == "abc"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
