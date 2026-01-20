import logging
from typing import List
from .types import PaginationConfig, PaginationMode

logger = logging.getLogger(__name__)

class QueryValidator:
    """Validate queries before pagination and emit warnings."""
    
    def validate(self, sql: str, config: PaginationConfig) -> List[str]:
        """Validate query and return list of warnings."""
        warnings = []
        
        sql_upper = sql.upper()
        
        # Check for missing ORDER BY in cursor mode
        if config.mode == PaginationMode.CURSOR:
            if "ORDER BY" not in sql_upper:
                warnings.append(
                    "No ORDER BY in cursor mode - results may be inconsistent"
                )
        
        # Check for non-deterministic queries
        if any(kw in sql_upper for kw in ["SELECT DISTINCT", "GROUP BY", "RANDOM()"]):
            if config.mode == PaginationMode.CURSOR:
                warnings.append(
                    "Cursor pagination on non-deterministic query - "
                    "consider offset mode"
                )
        
        # Warn on deep offset
        if config.mode == PaginationMode.OFFSET:
            offset = (config.page - 1) * config.per_page
            if offset > 10000:
                warnings.append(
                    f"Deep offset ({offset}) - consider cursor pagination"
                )
        
        # Log all warnings
        for warning in warnings:
            logger.warning(f"PAGINATION_WARNING: {warning}")
        
        return warnings
