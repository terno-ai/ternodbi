"""Resolve which accounts a caller may query for an API datasource.

The caller's groups determine the allowed accounts, and the result is passed
to the dispatch layer through `permitted_accounts`.

An empty match on a configured allowlist means access is denied. Only `None`,
when no allowlist is configured, means unrestricted access.
"""

from __future__ import annotations
from typing import Iterable, Optional, Set


def permitted_accounts(data_source, groups: Iterable) -> Optional[Set[str]]:
    """The account ids the caller's `groups` may query on `data_source`.

    - Returns `None` when the datasource has **no** allowlist rows at all —
      unrestricted, so a freshly connected source is usable without setup.
    - Returns the union of allowed accounts across the caller's groups when the
      datasource **is** configured — possibly empty, which denies everything.
    """
    from terno_dbi.core.models import GroupAccountAllowlist

    configured = GroupAccountAllowlist.objects.filter(data_source=data_source)
    if not configured.exists():
        return None   # unrestricted — not the same as an empty set

    group_ids = [getattr(g, "id", g) for g in groups]
    allowed = configured.filter(group_id__in=group_ids).values_list(
        "account_id", flat=True
    )
    return set(allowed)   # may be empty -> deny all


def filter_accounts(accounts, permitted: Optional[Set[str]]):
    """Keep only the accounts the caller may see.

    `permitted is None` means unrestricted, so everything passes. Applied to
    `list_accounts` so an agent cannot even see — let alone query — an account
    outside its allowlist.
    """
    if permitted is None:
        return list(accounts)
    return [a for a in accounts if a.id in permitted]


__all__ = ["filter_accounts", "permitted_accounts"]
