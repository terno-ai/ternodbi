"""Per-account fan-out with partial success.

Every API source runs the same shape of loop: for each requested account, fetch
that account's rows and concatenate. A single account can fail for reasons that
have nothing to do with the others — a Google Ads *manager* account (which owns
no campaigns), a deactivated account, a per-account permission gap. Failing the
whole query because one account is bad is the wrong default for a multi-account
request: it throws away the data the caller *can* see.

`gather_accounts` runs the fetch per account, keeps the rows that succeed, and
records each failure as a human-readable warning. It only re-raises when *every*
account failed (there is genuinely nothing to return), so a query over a good
account and a manager account returns the good account's rows plus a warning
about the manager, rather than an error.
"""

from __future__ import annotations
from typing import Any, Callable, Dict, List, Tuple

from terno_dbi.connectors.api.model.errors import ApiError


def gather_accounts(
    accounts: List[str],
    fetch_one: Callable[[str], List[Dict[str, Any]]],
) -> Tuple[List[Dict[str, Any]], List[str]]:
    """Fetch each account's rows with partial success.

    `fetch_one(account)` returns that account's rows (already tagged/parsed).
    Returns `(rows, warnings)`. If every account raised, the first error is
    re-raised — a total failure is still a failure.
    """
    rows: List[Dict[str, Any]] = []
    warnings: List[str] = []
    errors: List[ApiError] = []
    for account in accounts:
        try:
            rows.extend(fetch_one(account))
        except ApiError as exc:
            errors.append(exc)
            warnings.append(f"Account {account} skipped: {exc.message}")
    if errors and len(errors) == len(accounts):
        raise errors[0]
    return rows, warnings


__all__ = ["gather_accounts"]
