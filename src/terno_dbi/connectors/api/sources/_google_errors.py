"""Reading Google's error bodies, shared by the Google connectors."""

from __future__ import annotations
from typing import Optional
from terno_dbi.connectors.api.model.errors import ApiError, ErrorCode

_SCOPE_REASONS = frozenset({
    "ACCESS_TOKEN_SCOPE_INSUFFICIENT", "insufficientPermissions",
})


def error_reason(resp) -> str:
    """Google's machine-readable reason for a failed response, or ""."""
    try:
        err = (resp.json() or {}).get("error", {})
    except ValueError:
        return ""
    if not isinstance(err, dict):
        return ""
    for detail in err.get("details") or []:
        if isinstance(detail, dict) and detail.get("reason"):
            return str(detail["reason"])
    errors = err.get("errors") or []
    if errors and isinstance(errors[0], dict) and errors[0].get("reason"):
        return str(errors[0]["reason"])
    return str(err.get("status", "") or "")


def error_message(resp) -> str:
    """Google's human-readable message for a failed response, or ""."""
    try:
        err = (resp.json() or {}).get("error", {})
    except ValueError:
        return ""
    return str(err.get("message", "") or "")[:300] if isinstance(err, dict) else ""


def scope_error(resp, product: str) -> Optional[ApiError]:
    """A reconnect error when Google refused the call for a missing scope."""
    reason = error_reason(resp) if resp.status_code == 403 else ""
    if reason not in _SCOPE_REASONS:
        return None
    return ApiError(
        ErrorCode.AUTH_EXPIRED,
        f"{product} access was not fully granted (403 {reason}). Reconnect it "
        f"and tick every box on Google's screen.",
        retriable=False,
        details={"status": 403, "reason": reason, "scope_insufficient": True},
    )
