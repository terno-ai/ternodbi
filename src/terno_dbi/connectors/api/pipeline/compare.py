"""Compare API query results across two periods.

This logic is shared by all connectors so date arithmetic and row alignment stay
consistent instead of being reimplemented per provider.

Queries use already-resolved absolute dates, so comparisons never depend on
``now``. Rows are matched by dimension values; date dimensions are aligned by
offset within each period. Missing rows produce null values, and percentage
changes from a null or zero base remain null.

Non-additive metrics are compared per row without aggregation. Leap-year
comparisons map 29 Feb to 28 Feb, while weekday comparisons shift by whole
weeks. Fiscal periods, quarters, and YTD are handled as caller-defined custom
ranges.
"""

from __future__ import annotations
from datetime import date, timedelta
from typing import Any, Dict, List, Optional, Tuple
from terno_dbi.connectors.api.model.errors import ApiError, ErrorCode
from terno_dbi.connectors.api.model.types import Compare, DateRange


def _d(s: str) -> date:
    return date.fromisoformat(s)


def _shift_year(d: date, years: int) -> date:
    try:
        return d.replace(year=d.year - years)
    except ValueError:
        # 29 Feb in a non-leap target year -> 28 Feb.
        return d.replace(year=d.year - years, day=28)


def resolve_compare_range(base: DateRange, compare: Compare) -> DateRange:
    """The absolute date range the comparison period covers."""
    start, end = _d(base.start), _d(base.end)

    if compare.type == "custom":
        if not (compare.start and compare.end):
            raise ApiError(
                ErrorCode.INVALID_FILTER,
                "compare_type=custom requires compare start and end dates.",
            )
        return DateRange(compare.start, compare.end)

    if compare.type == "prev_range":
        span = (end - start).days
        prev_end = start - timedelta(days=1)
        prev_start = prev_end - timedelta(days=span)
        return DateRange(prev_start.isoformat(), prev_end.isoformat())

    if compare.type == "prev_year":
        return DateRange(
            _shift_year(start, 1).isoformat(),
            _shift_year(end, 1).isoformat(),
        )

    if compare.type == "prev_year_weekday":
        # 52 weeks keeps the weekday aligned (364 days).
        return DateRange(
            (start - timedelta(days=364)).isoformat(),
            (end - timedelta(days=364)).isoformat(),
        )

    raise ApiError(
        ErrorCode.INVALID_FILTER,
        f"Unknown compare_type {compare.type!r}.",
    )


def _to_number(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _delta(base: Optional[float], compare: Optional[float], show: str) -> Optional[float]:
    """One metric's change, per the requested display mode.

    A null or zero base gives a null percentage rather than infinity — a real
    number that reads as authoritative would be worse than an explicit gap.
    """
    if show == "value":
        return compare
    if base is None or compare is None:
        return None
    if show == "abs_change":
        return base - compare
    # perc_change
    if compare == 0:
        return None
    return (base - compare) / compare * 100.0


def _row_key(
    row: Dict[str, Any],
    plain_dims: List[str],
    date_dims: List[str],
    range_start: date,
) -> Tuple:
    """Alignment key: plain dimension values, plus each date as an offset."""
    parts: List[Any] = [row.get(d) for d in plain_dims]
    for dd in date_dims:
        raw = row.get(dd)
        try:
            offset = (_d(str(raw)) - range_start).days
        except (ValueError, TypeError):
            offset = raw   # non-date value; align on it verbatim
        parts.append(("__date__", dd, offset))
    return tuple(parts)


def add_comparison(
    base_rows: List[Dict[str, Any]],
    compare_rows: List[Dict[str, Any]],
    *,
    plain_dimensions: List[str],
    date_dimensions: List[str],
    metrics: List[str],
    base_start: str,
    compare_start: str,
    show: str = "perc_change",
) -> Tuple[List[Dict[str, Any]], List[str]]:
    """Align two periods and attach `<metric>__compare` / `__delta` columns.

    Returns the merged rows and any warnings. The alignment is an outer join, so
    a dimension value present in only one period still appears, with nulls on the
    missing side.
    """
    warnings: List[str] = []
    base_dt, compare_dt = _d(base_start), _d(compare_start)

    base_by = {
        _row_key(r, plain_dimensions, date_dimensions, base_dt): r
        for r in base_rows
    }
    compare_by = {
        _row_key(r, plain_dimensions, date_dimensions, compare_dt): r
        for r in compare_rows
    }

    ordered_keys = list(base_by.keys())
    for k in compare_by:
        if k not in base_by:
            ordered_keys.append(k)

    merged: List[Dict[str, Any]] = []
    for key in ordered_keys:
        b = base_by.get(key)
        c = compare_by.get(key)
        row: Dict[str, Any] = dict(b) if b is not None else {}
        if b is None and c is not None:
            # Present only in the comparison period — carry its dimensions.
            for d in plain_dimensions + date_dimensions:
                row.setdefault(d, c.get(d))
        for m in metrics:
            bv = _to_number(b.get(m)) if b is not None else None
            cv = _to_number(c.get(m)) if c is not None else None
            row[f"{m}__compare"] = cv
            row[f"{m}__delta"] = _delta(bv, cv, show)
        merged.append(row)

    if any(k not in base_by for k in compare_by):
        warnings.append(
            "Some rows exist only in the comparison period; their base values "
            "are null, not zero."
        )
    return merged, warnings


__all__ = ["add_comparison", "resolve_compare_range"]
