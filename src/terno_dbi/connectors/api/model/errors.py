"""Stable, machine-readable errors for API connectors.

Every failure includes a stable code for agents to branch on and a human-readable
message explaining the problem and how to fix it. Field errors should identify
the invalid field and suggest close matches so agents can recover without
re-fetching the entire catalog.

`success: false` and MCP `isError: true` must stay consistent. Failed calls
must never be reported as successful responses.
"""

from __future__ import annotations
import difflib
from typing import Any, Dict, List, Optional


class ErrorCode:
    AUTH_EXPIRED = "AUTH_EXPIRED"
    AUTH_REVOKED = "AUTH_REVOKED"
    RATE_LIMITED = "RATE_LIMITED"
    QUOTA_EXCEEDED = "QUOTA_EXCEEDED"
    INVALID_FIELD = "INVALID_FIELD"
    INVALID_REPORT_TYPE = "INVALID_REPORT_TYPE"
    MISSING_SETTING = "MISSING_SETTING"
    INVALID_SETTING = "INVALID_SETTING"
    INVALID_FILTER = "INVALID_FILTER"
    ACCOUNT_FORBIDDEN = "ACCOUNT_FORBIDDEN"
    MIXED_CURRENCY = "MIXED_CURRENCY"
    UPSTREAM_ERROR = "UPSTREAM_ERROR"
    TIMEOUT = "TIMEOUT"

    # Codes whose cause is transient — retrying later may succeed.
    _RETRIABLE = frozenset({RATE_LIMITED, UPSTREAM_ERROR, TIMEOUT})


class ApiError(Exception):
    """A connector failure with a stable code.

    Raise this from a connector; the dispatch layer turns it into the response
    envelope with `to_payload()`. Never let a raw provider exception escape — it
    has no stable code and often leaks a token or an internal URL.
    """

    def __init__(
        self,
        code: str,
        message: str,
        *,
        retriable: Optional[bool] = None,
        retry_after_seconds: Optional[int] = None,
        details: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(message)
        self.code = code
        self.message = message
        self.retriable = (
            retriable if retriable is not None
            else code in ErrorCode._RETRIABLE
        )
        self.retry_after_seconds = retry_after_seconds
        self.details = details or {}

    def to_payload(self) -> Dict[str, Any]:
        error: Dict[str, Any] = {
            "code": self.code,
            "message": self.message,
            "retriable": self.retriable,
        }
        if self.retry_after_seconds is not None:
            error["retry_after_seconds"] = self.retry_after_seconds
        if self.details:
            error["details"] = self.details
        return {"success": False, "error": error}


def invalid_field(bad: str, known: List[str]) -> ApiError:
    """`INVALID_FIELD`, with near-match suggestions.

    The whole point of the code over a bare rejection: the agent can correct the
    field id from the suggestion rather than re-listing the entire catalogue.
    """
    suggestions = difflib.get_close_matches(bad, known, n=3, cutoff=0.6)
    message = f"Unknown field {bad!r}."
    if suggestions:
        message += " Did you mean: " + ", ".join(suggestions) + "?"
    else:
        message += " Call list_fields to see the available fields."
    return ApiError(
        ErrorCode.INVALID_FIELD, message,
        details={"field": bad, "suggestions": suggestions},
    )


def missing_setting(setting_id: str, label: str, report_type: str) -> ApiError:
    human = label or setting_id
    return ApiError(
        ErrorCode.MISSING_SETTING,
        f"Report type {report_type!r} requires {human!r} "
        f"(setting id {setting_id!r}), which was not supplied.",
        details={"setting_id": setting_id, "report_type": report_type},
    )


def invalid_setting(bad: str, accepted: List[str], report_type: str) -> ApiError:
    message = f"Report type {report_type!r} does not accept setting {bad!r}."
    if accepted:
        message += " Accepted: " + ", ".join(accepted) + "."
    return ApiError(
        ErrorCode.INVALID_SETTING, message,
        details={"setting_id": bad, "report_type": report_type,
                 "accepted": accepted},
    )


__all__ = [
    "ApiError",
    "ErrorCode",
    "invalid_field",
    "invalid_setting",
    "missing_setting",
]
