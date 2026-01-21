import logging
from typing import List
from .types import PaginationConfig, PaginationMode

logger = logging.getLogger(__name__)


class QueryValidator:
    def validate(self, sql: str, config: PaginationConfig) -> List[str]:
        warnings = []

        sql_upper = sql.upper()

        if config.mode == PaginationMode.CURSOR:
            if "ORDER BY" not in sql_upper:
                warnings.append(
                    "No ORDER BY in cursor mode - results may be inconsistent"
                )

        if any(kw in sql_upper for kw in ["SELECT DISTINCT", "GROUP BY", "RANDOM()"]):
            if config.mode == PaginationMode.CURSOR:
                warnings.append(
                    "Cursor pagination on non-deterministic query - "
                    "consider offset mode"
                )

        if config.mode == PaginationMode.OFFSET:
            offset = (config.page - 1) * config.per_page
            if offset > 10000:
                warnings.append(
                    f"Deep offset ({offset}) - consider cursor pagination"
                )

        for warning in warnings:
            logger.warning(f"PAGINATION_WARNING: {warning}")

        return warnings
