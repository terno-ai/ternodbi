"""
Tests for CursorCodec.

Covers:
- Encoding/decoding roundtrip
- HMAC signature verification
- TTL expiration
- Version handling
- Key rotation security
"""

import os
import sys
import pytest
import time
import json
import base64
import hmac
import hashlib

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dbi_server.settings')
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'src'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'server'))

import django
django.setup()

from terno_dbi.services.pagination import CursorCodec, OrderColumn


class TestCursorCodecBasic:
    """Basic encoding/decoding tests."""

    def test_encode_decode_roundtrip(self):
        """Cursor encodes and decodes correctly."""
        codec = CursorCodec("test-key")
        order = [OrderColumn("id", "DESC")]
        values = {"id": 123, "name": "test"}

        cursor = codec.encode(values, order)
        decoded = codec.decode(cursor)

        assert decoded["v"] == 1
        assert decoded["values"]["id"] == 123
        assert decoded["values"]["name"] == "test"

    def test_cursor_contains_signature(self):
        """Cursor format is payload.signature."""
        codec = CursorCodec("test-key")
        order = [OrderColumn("id", "DESC")]
        cursor = codec.encode({"id": 1}, order)

        parts = cursor.split(".")
        assert len(parts) == 2
        assert len(parts[1]) == 16  # Truncated HMAC

    def test_composite_order_columns(self):
        """Cursor encodes multiple ORDER BY columns."""
        codec = CursorCodec("test-key")
        order = [OrderColumn("created_at", "DESC"), OrderColumn("id", "ASC")]
        values = {"created_at": "2024-01-20", "id": 123}

        cursor = codec.encode(values, order)
        decoded = codec.decode(cursor)

        assert len(decoded["order"]) == 2
        assert decoded["order"][0] == ["created_at", "DESC"]
        assert decoded["order"][1] == ["id", "ASC"]


class TestCursorCodecSecurity:
    """Security and integrity tests."""

    def test_tampered_signature_rejected(self):
        """Modifying signature causes rejection."""
        codec = CursorCodec("test-key")
        order = [OrderColumn("id", "DESC")]
        cursor = codec.encode({"id": 100}, order)

        parts = cursor.split(".")
        tampered = parts[0] + ".TAMPERED_SIG12"

        with pytest.raises(ValueError) as exc_info:
            codec.decode(tampered)

        assert "Invalid cursor signature" in str(exc_info.value)

    def test_tampered_payload_rejected(self):
        """Modifying payload invalidates signature."""
        codec = CursorCodec("test-key")
        order = [OrderColumn("id", "DESC")]
        cursor = codec.encode({"id": 100}, order)

        parts = cursor.split(".")
        payload = json.loads(base64.urlsafe_b64decode(parts[0]))
        payload["values"]["id"] = 999
        tampered_payload = base64.urlsafe_b64encode(
            json.dumps(payload).encode()
        ).decode()
        tampered = tampered_payload + "." + parts[1]

        with pytest.raises(ValueError):
            codec.decode(tampered)

    def test_invalid_format_rejected(self):
        """Garbage string is rejected."""
        codec = CursorCodec("test-key")

        with pytest.raises(ValueError):
            codec.decode("not-a-valid-cursor")

    def test_old_key_rejected_after_rotation(self):
        """Cursor signed with old key fails with new key."""
        old_codec = CursorCodec("old-key")
        order = [OrderColumn("id", "DESC")]
        cursor = old_codec.encode({"id": 100}, order)

        new_codec = CursorCodec("new-key")
        with pytest.raises(ValueError) as exc_info:
            new_codec.decode(cursor)

        assert "Invalid cursor signature" in str(exc_info.value)


class TestCursorCodecTTL:
    """TTL expiration tests."""

    def test_expired_cursor_rejected(self):
        """Cursor past TTL is rejected."""
        codec = CursorCodec("test-key", ttl_seconds=1)
        order = [OrderColumn("id", "DESC")]
        cursor = codec.encode({"id": 100}, order)

        time.sleep(1.5)

        with pytest.raises(ValueError) as exc_info:
            codec.decode(cursor)

        assert "expired" in str(exc_info.value).lower()

    def test_valid_cursor_not_expired(self):
        """Cursor within TTL is accepted."""
        codec = CursorCodec("test-key", ttl_seconds=3600)
        order = [OrderColumn("id", "DESC")]
        cursor = codec.encode({"id": 100}, order)

        decoded = codec.decode(cursor)
        assert decoded["values"]["id"] == 100


class TestCursorCodecVersioning:
    """Version handling tests (documents current permissive behavior)."""

    def test_version_zero_currently_accepted(self):
        """v=0 is currently accepted (version validation TODO)."""
        payload = {"v": 0, "values": {"id": 1}, "order": [("id", "DESC")]}
        json_bytes = json.dumps(payload).encode()
        signature = hmac.new(
            b"test-key", json_bytes, hashlib.sha256
        ).hexdigest()[:16]
        cursor = base64.urlsafe_b64encode(json_bytes).decode() + "." + signature

        codec = CursorCodec("test-key")
        decoded = codec.decode(cursor)
        assert decoded["v"] == 0

    def test_future_version_currently_accepted(self):
        """v=99 is currently accepted (version validation TODO)."""
        payload = {"v": 99, "values": {"id": 1}, "order": [("id", "DESC")]}
        json_bytes = json.dumps(payload).encode()
        signature = hmac.new(
            b"test-key", json_bytes, hashlib.sha256
        ).hexdigest()[:16]
        cursor = base64.urlsafe_b64encode(json_bytes).decode() + "." + signature

        codec = CursorCodec("test-key")
        decoded = codec.decode(cursor)
        assert decoded["v"] == 99


class TestCursorCodecNoTTL:
    """Tests for cursor encoding without TTL."""

    def test_encode_without_ttl(self):
        """Cursor without TTL (ttl_seconds=0) has no exp field."""
        codec = CursorCodec("test-key", ttl_seconds=0)
        order = [OrderColumn("id", "DESC")]
        cursor = codec.encode({"id": 100}, order)

        decoded = codec.decode(cursor)
        assert "exp" not in decoded

    def test_negative_ttl_no_expiration(self):
        """Negative TTL also skips expiration."""
        codec = CursorCodec("test-key", ttl_seconds=-1)
        order = [OrderColumn("id", "DESC")]
        cursor = codec.encode({"id": 100}, order)

        decoded = codec.decode(cursor)
        assert "exp" not in decoded


class TestCursorCodecExceptionHandling:
    """Tests for exception handling in decode."""

    def test_malformed_base64_raises_value_error(self):
        """Malformed base64 raises ValueError with generic message."""
        codec = CursorCodec("test-key")

        # Use a base64 string that decodes but isn't valid JSON
        # This triggers the non-ValueError exception path
        import base64
        non_json = base64.urlsafe_b64encode(b"{{not-valid-json}}").decode()
        with pytest.raises(ValueError) as exc_info:
            codec.decode(f"{non_json}.1234567890123456")

        # Should wrap in "Invalid cursor" message
        assert "Invalid cursor" in str(exc_info.value)

    def test_malformed_json_raises_value_error(self):
        """Valid base64 but invalid JSON raises ValueError."""
        import base64
        codec = CursorCodec("test-key")

        # Valid base64 but not JSON
        invalid_json = base64.urlsafe_b64encode(b"not json").decode()
        with pytest.raises(ValueError) as exc_info:
            codec.decode(f"{invalid_json}.1234567890123456")

        assert "Invalid cursor" in str(exc_info.value)
