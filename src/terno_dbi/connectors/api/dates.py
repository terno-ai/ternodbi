"""Date helpers shared across API connectors.

`get_today` is a one-line tool with an outsized effect: an agent asked for "last
month" has no reliable clock and will guess from its training cutoff, silently
querying the wrong window. Supermetrics ships this for exactly that reason.

Timezone matters here — GA4 reports in the property timezone, ad platforms in
the account timezone — so "today" differs by up to a day from UTC (§6.9). The
tool returns UTC by default and the source's timezone when one is supplied.
"""

from __future__ import annotations
from datetime import datetime, timezone as _tz
from typing import Any, Dict, Optional

try:
    from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
except ImportError:   # pragma: no cover - py<3.9
    ZoneInfo = None   # type: ignore
    ZoneInfoNotFoundError = Exception   # type: ignore


def get_today(tz_name: Optional[str] = None) -> Dict[str, Any]:
    """Current date and time, in UTC and optionally in a named timezone."""
    now_utc = datetime.now(_tz.utc)
    result: Dict[str, Any] = {
        "utc_date": now_utc.strftime("%Y-%m-%d"),
        "utc_datetime": now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    if tz_name and ZoneInfo is not None:
        try:
            local = now_utc.astimezone(ZoneInfo(tz_name))
            result["timezone"] = tz_name
            result["local_date"] = local.strftime("%Y-%m-%d")
            result["local_datetime"] = local.strftime("%Y-%m-%dT%H:%M:%S%z")
        except (ZoneInfoNotFoundError, ValueError):
            # A bad timezone name is caller input, not a server fault — report it
            # and fall back to UTC rather than swallowing every possible error.
            result["timezone_error"] = f"Unknown timezone {tz_name!r}; showing UTC."
    return result


__all__ = ["get_today"]
