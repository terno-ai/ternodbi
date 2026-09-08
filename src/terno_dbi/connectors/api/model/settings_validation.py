"""Validate query settings against the report type before dispatch.

`settings` is intentionally untyped so one `QuerySpec` can support different
sources. Before dispatch, those settings must match the report type's catalog
definition so invalid or missing values fail with an actionable error such as
`MISSING_SETTING` instead of an opaque provider error.

Validation is driven entirely by catalog metadata, so individual connectors do
not need to reimplement it.
"""

from __future__ import annotations
from typing import Any, Dict, List, Optional
from terno_dbi.connectors.api.model.errors import (
    ApiError,
    ErrorCode,
    invalid_setting,
    missing_setting,
)


def validate_settings(
    catalog,
    report_type: Optional[str],
    settings: Dict[str, Any],
) -> None:
    """Raise an `ApiError` if `settings` does not satisfy `report_type`.

    `catalog` is a `ConnectorCatalog` row (or anything exposing `report_types`
    and `has_report_types`). Returns None when valid.
    """
    declared: List[Dict[str, Any]] = list(getattr(catalog, "report_types", []) or [])

    # A source with no report types accepts no settings at all.
    if not declared:
        if settings:
            raise invalid_setting(next(iter(settings)), [], report_type or "")
        return

    known = [r.get("id") for r in declared if r.get("id")]

    if not report_type:
        # Name the options in the error itself, so the agent corrects in one
        # step. list_fields (not list_datasources) is what surfaces report types.
        raise ApiError(
            ErrorCode.INVALID_REPORT_TYPE,
            "This source needs a report_type. Choose one of: "
            + ", ".join(known) + ". (list_fields also returns them.)",
            details={"available": known},
        )

    spec = next((r for r in declared if r.get("id") == report_type), None)
    if spec is None:
        raise ApiError(
            ErrorCode.INVALID_REPORT_TYPE,
            f"Unknown report type {report_type!r}. Choose one of: "
            + ", ".join(known) + ".",
            details={"report_type": report_type, "available": known},
        )

    declared_settings = spec.get("settings", []) or []
    accepted_ids = [s.get("setting_id") for s in declared_settings]

    # Every required setting must be present and non-empty.
    for s in declared_settings:
        if not s.get("required", True):
            continue
        sid = s.get("setting_id")
        value = settings.get(sid)
        if value is None or (isinstance(value, str) and not value.strip()):
            raise missing_setting(sid, s.get("label", ""), report_type)

    # No unknown setting may be forwarded to the provider.
    for supplied in settings:
        if supplied not in accepted_ids:
            raise invalid_setting(supplied, accepted_ids, report_type)


__all__ = ["validate_settings"]
