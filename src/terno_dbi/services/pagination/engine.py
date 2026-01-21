import logging
import time
import sqlalchemy
from typing import Any, Dict, Iterator, List, Optional, Tuple

from terno_dbi.core.conf import get
from .types import PaginationConfig, PaginationMode, PaginatedResult, OrderColumn
from .codecs import CursorCodec
from .validator import QueryValidator
from .telemetry import PaginationTelemetry, PaginationMetrics

logger = logging.getLogger(__name__)


class PaginationService:

    def __init__(
        self,
        connector,
        dialect: str,
        secret_key: Optional[str] = None
    ):
        self.connector = connector
        self.dialect = dialect.lower()

        if secret_key is None:
            from django.conf import settings
            secret_key = getattr(settings, 'DBI_SECRET_KEY', settings.SECRET_KEY)

        self.cursor_codec = CursorCodec(secret_key)
        self.validator = QueryValidator()
        self.telemetry = PaginationTelemetry()

    def paginate(
        self,
        sql: str,
        config: Optional[PaginationConfig] = None
    ) -> PaginatedResult:

        if config is None:
            config = PaginationConfig()

        config.per_page = min(config.per_page, get("MAX_PAGE_SIZE") or 500)

        warnings = self.validator.validate(sql, config)

        start_time = time.time()

        try:
            if config.mode == PaginationMode.CURSOR:
                if config.cursor and config.direction == "backward":
                    result = self._cursor_paginate_backward(sql, config)
                else:
                    result = self._cursor_paginate(sql, config)
            elif config.mode == PaginationMode.STREAM:
                result = self._stream_paginate(sql, config)
            else:
                result = self._offset_paginate(sql, config)

            result.warnings = warnings

            duration_ms = (time.time() - start_time) * 1000
            self.telemetry.emit(PaginationMetrics(
                mode=config.mode.value,
                duration_ms=duration_ms,
                rows_returned=len(result.data)
            ))

            return result

        except ValueError as e:
            if "cursor" in str(e).lower():
                self.telemetry.record_cursor_decode_failure()
            raise

    def _offset_paginate(
        self,
        sql: str,
        config: PaginationConfig
    ) -> PaginatedResult:
        offset = (config.page - 1) * config.per_page

        if offset > 10000:
            self.telemetry.record_deep_offset(offset)

        paginated_sql = self._wrap_with_limit_offset(
            sql, config.per_page + 1, offset
        )

        with self.connector.get_connection() as con:
            result = con.execute(sqlalchemy.text(paginated_sql))
            rows = result.fetchall()
            columns = list(result.keys())

        has_next = len(rows) > config.per_page
        data = rows[:config.per_page]

        total = self._get_total_count(sql)
        total_pages = None
        if total is not None:
            total_pages = (total + config.per_page - 1) // config.per_page

        return PaginatedResult(
            data=data,
            columns=columns,
            page=config.page,
            per_page=config.per_page,
            total_count=total,
            total_pages=total_pages,
            has_next=has_next,
            has_prev=config.page > 1,
            next_cursor=None,
            prev_cursor=None
        )

    def _cursor_paginate(
        self,
        sql: str,
        config: PaginationConfig
    ) -> PaginatedResult:
        cursor_data = None
        if config.cursor:
            cursor_data = self.cursor_codec.decode(config.cursor)

        order_clause = ", ".join([
            f"{o.column} {o.direction} NULLS {o.nulls}"
            for o in config.order_by
        ])

        cursor_where = self._build_cursor_where(cursor_data, config.order_by)

        if cursor_where:
            paginated_sql = f"""
                SELECT * FROM ({sql}) AS _q
                WHERE {cursor_where}
                ORDER BY {order_clause}
                LIMIT {config.per_page + 1}
            """
        else:
            paginated_sql = f"""
                SELECT * FROM ({sql}) AS _q
                ORDER BY {order_clause}
                LIMIT {config.per_page + 1}
            """

        with self.connector.get_connection() as con:
            params = cursor_data.get("values", {}) if cursor_data else {}
            result = con.execute(sqlalchemy.text(paginated_sql), params)
            rows = result.fetchall()
            columns = list(result.keys())

        has_next = len(rows) > config.per_page
        data = rows[:config.per_page]

        next_cursor = None
        if has_next and data:
            next_values = self._extract_cursor_values(
                data[-1], columns, config.order_by
            )
            next_cursor = self.cursor_codec.encode(next_values, config.order_by)

        prev_cursor = None
        if data and cursor_data:
            prev_values = self._extract_cursor_values(
                data[0], columns, config.order_by
            )
            prev_cursor = self.cursor_codec.encode(prev_values, config.order_by)

        return PaginatedResult(
            data=data,
            columns=columns,
            page=0,
            per_page=config.per_page,
            total_count=None,
            total_pages=None,
            has_next=has_next,
            has_prev=cursor_data is not None,
            next_cursor=next_cursor,
            prev_cursor=prev_cursor
        )

    def _cursor_paginate_backward(
        self,
        sql: str,
        config: PaginationConfig
    ) -> PaginatedResult:
        """
        Backward pagination.
        Steps:
        1. Invert ORDER BY direction
        2. Fetch rows
        3. Reverse result set
        4. Re-emit forward cursor
        """
        cursor_data = self.cursor_codec.decode(config.cursor)

        inverted_order = [o.inverted() for o in config.order_by]

        cursor_where = self._build_cursor_where(cursor_data, inverted_order)
        order_clause = ", ".join([
            f"{o.column} {o.direction} NULLS {o.nulls}"
            for o in inverted_order
        ])
        paginated_sql = f"""
            SELECT * FROM ({sql}) AS _q
            WHERE {cursor_where}
            ORDER BY {order_clause}
            LIMIT {config.per_page + 1}
        """
        with self.connector.get_connection() as con:
            params = cursor_data.get("values", {})
            result = con.execute(sqlalchemy.text(paginated_sql), params)
            rows = result.fetchall()
            columns = list(result.keys())

        has_prev = len(rows) > config.per_page
        data = list(reversed(rows[:config.per_page]))
        next_cursor = None
        if data:
            next_values = self._extract_cursor_values(
                data[-1], columns, config.order_by
            )
            next_cursor = self.cursor_codec.encode(next_values, config.order_by)
        new_prev_cursor = None
        if has_prev and data:
            prev_values = self._extract_cursor_values(
                data[0], columns, config.order_by
            )
            new_prev_cursor = self.cursor_codec.encode(prev_values, config.order_by)

        return PaginatedResult(
            data=data,
            columns=columns,
            page=0,
            per_page=config.per_page,
            total_count=None,
            total_pages=None,
            has_next=True,
            has_prev=has_prev,
            next_cursor=next_cursor,
            prev_cursor=new_prev_cursor
        )

    def _stream_paginate(
        self,
        sql: str,
        config: PaginationConfig
    ) -> PaginatedResult:
        yield_size = get("STREAM_YIELD_SIZE") or 1000

        with self.connector.get_connection() as con:
            result = con.execute(
                sqlalchemy.text(sql),
                execution_options={
                    "yield_per": yield_size,
                    "stream_results": True
                }
            )
            columns = list(result.keys())

            first_batch = []
            for i, row in enumerate(result):
                if i >= config.per_page:
                    break
                first_batch.append(row)

        return PaginatedResult(
            data=first_batch,
            columns=columns,
            page=1,
            per_page=config.per_page,
            total_count=None,
            total_pages=None,
            has_next=True,
            has_prev=False,
            next_cursor=None,
            prev_cursor=None
        )

    def stream_all(
        self,
        sql: str,
        yield_size: int = 1000
    ) -> Iterator[List[Tuple]]:
        with self.connector.get_connection() as con:
            result = con.execute(
                sqlalchemy.text(sql),
                execution_options={
                    "yield_per": yield_size,
                    "stream_results": True
                }
            )

            batch = []
            for row in result:
                batch.append(row)
                if len(batch) >= yield_size:
                    yield batch
                    batch = []

            if batch:
                yield batch

    def _build_cursor_where(
        self,
        cursor_data: Optional[Dict[str, Any]], 
        order_by: List[OrderColumn]
    ) -> str:
        """
        Build composite WHERE clause for keyset pagination.

        For ORDER BY (created_at DESC, id DESC):
          WHERE (created_at, id) < (:created_at, :id)

        Rules:
        - Use exclusive comparisons (< / >) to avoid duplicates
        - Match ORDER BY direction: DESC uses <, ASC uses >
        """
        if not cursor_data:
            return ""

        values = cursor_data.get("values", {})
        if not values:
            return ""

        columns = [o.column for o in order_by]

        # Build row comparison: (col1, col2) < (val1, val2)
        col_list = ", ".join(columns)
        val_list = ", ".join([f":{col}" for col in columns])

        # Determine comparison operator based on first column direction
        primary_dir = order_by[0].direction.upper()
        operator = "<" if primary_dir == "DESC" else ">"

        return f"({col_list}) {operator} ({val_list})"

    def _extract_cursor_values(
        self,
        row: Tuple,
        columns: List[str],
        order_by: List[OrderColumn]
    ) -> Dict[str, Any]:
        """Extract cursor values from row based on ORDER BY columns."""
        col_index = {col.lower(): i for i, col in enumerate(columns)}
        values = {}
        for order_col in order_by:
            col_lower = order_col.column.lower()
            if col_lower in col_index:
                values[order_col.column] = row[col_index[col_lower]]
        return values

    def _wrap_with_limit_offset(
        self,
        sql: str,
        limit: int,
        offset: int
    ) -> str:
        """Dialect-aware pagination wrapping."""
        if self.dialect in ("postgres", "postgresql", "snowflake", "bigquery", "databricks"):
            return f"SELECT * FROM ({sql}) AS _p LIMIT {limit} OFFSET {offset}"
        elif self.dialect == "mysql":
            return f"SELECT * FROM ({sql}) AS _p LIMIT {offset}, {limit}"
        elif self.dialect == "oracle":
            return f"""
                SELECT * FROM (
                    SELECT _p.*, ROWNUM AS _rn FROM ({sql}) _p 
                    WHERE ROWNUM <= {offset + limit}
                ) WHERE _rn > {offset}
            """
        return f"SELECT * FROM ({sql}) AS _p LIMIT {limit} OFFSET {offset}"

    def _get_total_count(
        self,
        sql: str,
        timeout_seconds: int = 10
    ) -> Optional[int]:
        """
        Get total count with guardrails.
        - Use statement timeout to prevent runaway queries
        - Return None if count exceeds threshold or times out
        """
        count_sql = f"SELECT COUNT(*) FROM ({sql}) AS _count_q"

        try:
            with self.connector.get_connection() as con:
                if self.dialect in ("postgres", "postgresql"):
                    con.execute(sqlalchemy.text(
                        f"SET statement_timeout = '{timeout_seconds}s'"
                    ))

                result = con.execute(sqlalchemy.text(count_sql))
                count = result.scalar()

                threshold = get("SKIP_TOTAL_COUNT_THRESHOLD") or 100000
                if count and count > threshold:
                    logger.info(
                        f"Total count {count} exceeds threshold {threshold}, "
                        "returning None"
                    )
                    return None

                return count
        except Exception as e:
            logger.warning(f"Failed to get total count: {e}")
            return None


def create_pagination_service(
    connector,
    dialect: str,
    secret_key: Optional[str] = None
) -> PaginationService:
    """Factory function to create a PaginationService."""
    return PaginationService(connector, dialect, secret_key)
