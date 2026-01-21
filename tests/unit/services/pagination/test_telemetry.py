"""
Tests for PaginationTelemetry.

Covers:
- Counter increments
- Metrics emission
- Mock metrics integration
"""

import os
import sys
import pytest
from unittest.mock import MagicMock, call

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dbi_server.settings')
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'src'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'server'))

import django
django.setup()

from terno_dbi.services.pagination.telemetry import PaginationTelemetry, PaginationMetrics


class TestTelemetryCounters:
    """Test counter increment behavior."""

    def test_cursor_decode_failure_increments(self):
        """TEL-01: Counter increments on decode failure."""
        telemetry = PaginationTelemetry()
        telemetry.record_cursor_decode_failure()
        telemetry.record_cursor_decode_failure()

        stats = telemetry.get_stats()
        assert stats["cursor_decode_failures"] == 2

    def test_deep_offset_increments(self):
        """TEL-02: Counter increments on deep offset."""
        telemetry = PaginationTelemetry()
        telemetry.record_deep_offset(50000)
        telemetry.record_deep_offset(100000)

        stats = telemetry.get_stats()
        assert stats["deep_offset_count"] == 2

    def test_fallback_increments(self):
        """Counter increments on fallback activation."""
        telemetry = PaginationTelemetry()
        telemetry.record_fallback()

        stats = telemetry.get_stats()
        assert stats["fallback_activations"] == 1

    def test_clean_stats_on_init(self):
        """TEL-03: Fresh telemetry has zero counters."""
        telemetry = PaginationTelemetry()
        stats = telemetry.get_stats()

        assert stats["cursor_decode_failures"] == 0
        assert stats["fallback_activations"] == 0
        assert stats["deep_offset_count"] == 0


class TestTelemetryEmit:
    """Test metrics emission with mocked backends."""

    def test_emit_calls_custom_function(self):
        """Emit routes metrics to custom function."""
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

    def test_emit_with_mock_metrics_backend(self):
        """Integration with mocked metrics backend."""
        mock_backend = MagicMock()

        telemetry = PaginationTelemetry(emit_fn=mock_backend)
        metrics = PaginationMetrics(
            mode="cursor",
            duration_ms=50.0,
            rows_returned=100
        )
        telemetry.emit(metrics)

        mock_backend.assert_called_once()
        call_args = mock_backend.call_args[0][0]
        assert call_args.mode == "cursor"
        assert call_args.rows_returned == 100

    def test_emit_safe_with_no_function(self):
        """Emit is safe when no emit_fn is set."""
        telemetry = PaginationTelemetry()  # No emit_fn
        metrics = PaginationMetrics(mode="offset", duration_ms=10.0, rows_returned=5)

        # Should not raise
        telemetry.emit(metrics)


class TestMetricsWiring:
    """Test that telemetry can wire to production-like mocks."""

    def test_wire_to_statsd_mock(self):
        """Simulate StatSD integration."""
        statsd = MagicMock()

        def emit_to_statsd(metrics):
            statsd.timing("pagination.duration_ms", metrics.duration_ms)
            statsd.gauge("pagination.rows_returned", metrics.rows_returned)
            statsd.incr(f"pagination.mode.{metrics.mode}")

        telemetry = PaginationTelemetry(emit_fn=emit_to_statsd)
        telemetry.emit(PaginationMetrics(
            mode="cursor",
            duration_ms=42.5,
            rows_returned=100
        ))

        statsd.timing.assert_called_once_with("pagination.duration_ms", 42.5)
        statsd.gauge.assert_called_once_with("pagination.rows_returned", 100)
        statsd.incr.assert_called_once_with("pagination.mode.cursor")

    def test_wire_to_prometheus_mock(self):
        """Simulate Prometheus integration."""
        histogram = MagicMock()
        counter = MagicMock()

        def emit_to_prom(metrics):
            histogram.observe(metrics.duration_ms / 1000)  # Seconds
            counter.labels(mode=metrics.mode).inc()

        telemetry = PaginationTelemetry(emit_fn=emit_to_prom)
        telemetry.emit(PaginationMetrics(
            mode="offset",
            duration_ms=200.0,
            rows_returned=50
        ))

        histogram.observe.assert_called_once_with(0.2)
        counter.labels.assert_called_once_with(mode="offset")

    def test_counter_integration_with_mock(self):
        """Counters can be read by mock monitoring."""
        alert_fired = []

        telemetry = PaginationTelemetry()

        # Simulate 10 decode failures
        for _ in range(10):
            telemetry.record_cursor_decode_failure()

        # Mock alerting check
        stats = telemetry.get_stats()
        if stats["cursor_decode_failures"] >= 5:
            alert_fired.append("high_cursor_failures")

        assert "high_cursor_failures" in alert_fired
