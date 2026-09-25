"""Persist and resolve which of a connected source's accounts are queryable.

One OAuth connection can see many accounts; this module keeps the user's choice
of which ones the agent may query (`ConnectorAccountSelection`) and folds that
choice into the account authorisation at query time.

The selection is always *within* the RBAC allowlist — the caller passes the
already-permitted account ids in, and the selection can only narrow that set,
never widen it. See `restrict_to_selection`.
"""

from __future__ import annotations
from typing import Iterable, List, Optional, Set


def sync_account_selections(data_source, accounts) -> List[dict]:
    from terno_dbi.core.models import ConnectorAccountSelection

    visible = {a.id: (getattr(a, "name", "") or "") for a in accounts}

    existing = {
        row.account_id: row
        for row in ConnectorAccountSelection.objects.filter(data_source=data_source)
    }

    # Prune rows for accounts the credential can no longer see.
    stale = [aid for aid in existing if aid not in visible]
    if stale:
        ConnectorAccountSelection.objects.filter(
            data_source=data_source, account_id__in=stale,
        ).delete()
        for aid in stale:
            existing.pop(aid, None)

    for account_id, name in visible.items():
        row = existing.get(account_id)
        if row is None:
            ConnectorAccountSelection.objects.create(
                data_source=data_source,
                account_id=account_id,
                account_name=name,
                enabled=True,
            )
        elif name and row.account_name != name:
            row.account_name = name
            row.save(update_fields=["account_name"])

    rows = ConnectorAccountSelection.objects.filter(data_source=data_source)
    return sorted(
        (
            {
                "account_id": r.account_id,
                "account_name": r.account_name,
                "enabled": r.enabled,
            }
            for r in rows
        ),
        key=lambda r: (r["account_name"].lower(), r["account_id"]),
    )


def set_enabled_accounts(data_source, account_ids: Iterable[str]) -> int:
    from terno_dbi.core.models import ConnectorAccountSelection

    wanted = {str(a) for a in account_ids}
    enabled_count = 0
    for row in ConnectorAccountSelection.objects.filter(data_source=data_source):
        should = row.account_id in wanted
        if row.enabled != should:
            row.enabled = should
            row.save(update_fields=["enabled", "updated_at"])
        if should:
            enabled_count += 1
    return enabled_count


def enabled_account_ids(data_source) -> Optional[Set[str]]:
    from terno_dbi.core.models import ConnectorAccountSelection

    rows = list(
        ConnectorAccountSelection.objects
        .filter(data_source=data_source)
        .values_list("account_id", "enabled")
    )
    if not rows:
        return None
    return {aid for aid, enabled in rows if enabled}


def restrict_to_selection(
    data_source, permitted: Optional[Set[str]],
) -> Optional[Set[str]]:
    selection = enabled_account_ids(data_source)
    if selection is None:
        return permitted
    if permitted is None:
        return selection
    return permitted & selection


__all__ = [
    "sync_account_selections",
    "set_enabled_accounts",
    "enabled_account_ids",
    "restrict_to_selection",
]
