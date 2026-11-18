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
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Dict, List, Tuple, Union

from terno_dbi.connectors.api.model.errors import ApiError

_Outcome = Union[List[Dict[str, Any]], ApiError]


def gather_accounts(
    accounts: List[str],
    fetch_one: Callable[[str], List[Dict[str, Any]]],
    *,
    max_workers: int = 1,
) -> Tuple[List[Dict[str, Any]], List[str]]:
    """Fetch each account's rows with partial success.

    `fetch_one(account)` returns that account's rows (already tagged/parsed).
    Returns `(rows, warnings)`. If every account raised, the first error is
    re-raised — a total failure is still a failure.

    With `max_workers > 1` accounts are fetched concurrently on a bounded thread
    pool. Rows and warnings keep the order of `accounts` either way, and an
    exception that is not an `ApiError` still propagates.
    """
    def attempt(account: str) -> _Outcome:
        try:
            return fetch_one(account)
        except ApiError as exc:
            return exc

    workers = max(1, min(max_workers, len(accounts)))
    if workers == 1:
        outcomes = [attempt(a) for a in accounts]
    else:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            outcomes = list(pool.map(_in_worker(attempt), accounts))

    rows: List[Dict[str, Any]] = []
    warnings: List[str] = []
    errors: List[ApiError] = []
    for account, outcome in zip(accounts, outcomes):
        if isinstance(outcome, ApiError):
            errors.append(outcome)
            warnings.append(f"Account {account} skipped: {outcome.message}")
        else:
            rows.extend(outcome)
    if errors and len(errors) == len(accounts):
        raise errors[0]
    return rows, warnings


def _in_worker(fn: Callable[[str], _Outcome]) -> Callable[[str], _Outcome]:
    """Run `fn` in a pool thread, closing any DB connection the thread opened.

    Token refreshes read and write the datasource row, and Django opens one
    connection per thread; without this, pool threads would leak them.
    """
    def run(account: str) -> _Outcome:
        try:
            return fn(account)
        finally:
            try:
                from django.db import connections
                connections.close_all()
            except Exception:   # noqa: BLE001 - Django absent or not configured
                pass
    return run


__all__ = ["gather_accounts"]
