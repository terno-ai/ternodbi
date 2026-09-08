"""Shared value types used by API connectors and the `data_query` tool.

The types are provider-neutral, so a `QuerySpec` has the same shape across
sources such as GA4, Meta, and Google Ads. Each connector translates that
common query into the provider-specific request.

Keeping this vocabulary in one place prevents provider-specific details from
leaking into the shared query contract.
"""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Dict, List, Literal, Optional


FieldKind = Literal["dimension", "metric"]


@dataclass(frozen=True)
class Account:
    """One queryable account under a connected source.

    An ad account, a GA4 property, a YouTube channel. `id` is what the provider
    expects back in a query; `name` is for humans.
    """

    id: str
    name: str
    currency: Optional[str] = None   # per account, never per org
    timezone: Optional[str] = None
    extra: Dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> Dict[str, Any]:
        d = {"id": self.id, "name": self.name}
        if self.currency:
            d["currency"] = self.currency
        if self.timezone:
            d["timezone"] = self.timezone
        d.update(self.extra)
        return d


@dataclass(frozen=True)
class Field:
    """One dimension or metric a source can return.

    `is_non_aggregatable` is load-bearing, not decorative: summing a
    pre-aggregated metric (activeUsers, bounceRate) across rows double-counts or
    is simply invalid. The flag is surfaced in `list_fields` and repeated in the
    `notes` of any result that contains one, because an agent will otherwise
    sum it. Supermetrics ships this flag; Windsor does not, and its GA4 output
    is wrong when summed.
    """

    id: str
    name: str
    kind: FieldKind
    description: str = ""
    data_type: str = "string"
    group: Optional[str] = None
    report_types: Optional[List[str]] = None
    is_non_aggregatable: bool = False
    is_monetary: bool = False

    def as_dict(self) -> Dict[str, Any]:
        d = {
            "id": self.id,
            "name": self.name,
            "kind": self.kind,
            "data_type": self.data_type,
        }
        if self.description:
            d["description"] = self.description
        if self.group:
            d["group"] = self.group
        if self.report_types:
            d["report_types"] = self.report_types
        if self.is_non_aggregatable:
            d["is_non_aggregatable"] = True
        if self.is_monetary:
            d["is_monetary"] = True
        return d


@dataclass(frozen=True)
class DateRange:
    """A resolved, absolute date range.

    Relative ranges ("last_30_days") are resolved to absolute dates *before* a
    QuerySpec is built, in the source's own timezone — never here. Storing only
    absolute dates keeps the cache key stable and the comparison arithmetic
    honest. `inclusive_of_today` records whether the range deliberately reaches
    into the current, partial period.
    """

    start: str   # YYYY-MM-DD
    end: str     # YYYY-MM-DD
    inclusive_of_today: bool = False

    def as_dict(self) -> Dict[str, Any]:
        return {
            "start": self.start,
            "end": self.end,
            "inclusive_of_today": self.inclusive_of_today,
        }


CompareType = Literal["prev_range", "prev_year", "prev_year_weekday", "custom"]
CompareShow = Literal["perc_change", "abs_change", "value"]


@dataclass(frozen=True)
class Compare:
    """A period-over-period comparison request. See §6.9 for the semantics."""

    type: CompareType
    show: CompareShow = "perc_change"
    start: Optional[str] = None   # required when type == "custom"
    end: Optional[str] = None

    def as_dict(self) -> Dict[str, Any]:
        d = {"type": self.type, "show": self.show}
        if self.start:
            d["start"] = self.start
        if self.end:
            d["end"] = self.end
        return d


@dataclass(frozen=True)
class QuerySpec:
    """A single, provider-neutral query.

    `settings` is the deliberate, untyped escape hatch that absorbs per-source
    divergence — report-type-specific values like YouTube's `video_id`. It is
    validated against the report type's declared settings before dispatch (see
    `settings_validation`), never forwarded raw.
    """

    accounts: List[str]
    fields: List[str]
    date_range: DateRange
    report_type: Optional[str] = None
    settings: Dict[str, Any] = field(default_factory=dict)
    filters: Optional[str] = None       # operator grammar — see `filters.py`
    compare: Optional[Compare] = None
    timezone: str = "UTC"
    max_rows: int = 1000

    def as_dict(self) -> Dict[str, Any]:
        d = {
            "accounts": list(self.accounts),
            "fields": list(self.fields),
            "date_range": self.date_range.as_dict(),
            "timezone": self.timezone,
            "max_rows": self.max_rows,
        }
        if self.report_type:
            d["report_type"] = self.report_type
        if self.settings:
            d["settings"] = dict(self.settings)
        if self.filters:
            d["filters"] = self.filters
        if self.compare:
            d["compare"] = self.compare.as_dict()
        return d


@dataclass(frozen=True)
class QueryResult:
    """The result of a query, in a shape the tool can return directly.

    `rows` are objects keyed by field id. `requested_field_ids` preserves column
    order, because the display name of a metric ("Views") differs from its id
    ("screenPageViews") and a consumer that maps by label reads the wrong
    column — a trap both vendors warn about explicitly.
    """

    requested_field_ids: List[str]
    rows: List[Dict[str, Any]]
    row_count: int
    notes: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    def as_dict(self) -> Dict[str, Any]:
        return {
            "requested_field_ids": list(self.requested_field_ids),
            "rows": self.rows,
            "row_count": self.row_count,
            "notes": list(self.notes),
            "warnings": list(self.warnings),
        }


__all__ = [
    "Account",
    "Compare",
    "CompareShow",
    "CompareType",
    "DateRange",
    "Field",
    "FieldKind",
    "QueryResult",
    "QuerySpec",
]
