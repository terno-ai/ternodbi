import logging
from dataclasses import dataclass
from typing import Callable, Dict, Optional

logger = logging.getLogger(__name__)

@dataclass
class PaginationMetrics:
    """Pagination telemetry metrics."""
    mode: str
    duration_ms: float
    rows_returned: int
    cursor_decode_failed: bool = False
    fallback_to_row_number: bool = False
    deep_offset: bool = False


class PaginationTelemetry:
    """Telemetry hooks for pagination monitoring."""
    
    def __init__(self, emit_fn: Optional[Callable[[PaginationMetrics], None]] = None):
        self.emit_fn = emit_fn or self._default_emit
        
        # Counters for aggregation
        self.cursor_decode_failures = 0
        self.fallback_activations = 0
        self.deep_offset_count = 0
    
    def _default_emit(self, metrics: PaginationMetrics):
        """Default: log metrics."""
        logger.info(
            f"PAGINATION_TELEMETRY: mode={metrics.mode} "
            f"duration={metrics.duration_ms:.2f}ms rows={metrics.rows_returned}"
        )
    
    def emit(self, metrics: PaginationMetrics):
        """Emit metrics via configured handler."""
        self.emit_fn(metrics)
    
    def record_cursor_decode_failure(self):
        self.cursor_decode_failures += 1
        logger.warning("PAGINATION_TELEMETRY: cursor_decode_failure")
    
    def record_fallback(self):
        self.fallback_activations += 1
        logger.info("PAGINATION_TELEMETRY: fallback_to_row_number")
    
    def record_deep_offset(self, offset: int):
        self.deep_offset_count += 1
        logger.info(f"PAGINATION_TELEMETRY: deep_offset={offset}")
    
    def get_stats(self) -> Dict[str, int]:
        return {
            "cursor_decode_failures": self.cursor_decode_failures,
            "fallback_activations": self.fallback_activations,
            "deep_offset_count": self.deep_offset_count
        }
