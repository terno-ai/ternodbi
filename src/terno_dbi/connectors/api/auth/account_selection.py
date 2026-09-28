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
                "writes_enabled": r.writes_enabled,
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


def set_writes_enabled_accounts(data_source, account_ids: Iterable[str]) -> int:
    """Opt specific accounts into write actions. Absent = writes off.

    Parallel to `set_enabled_accounts` but for the `writes_enabled` flag. An
    account must already exist as a selection row (created on connect); this only
    flips the write flag, it does not create rows or affect read `enabled`.
    """
    from terno_dbi.core.models import ConnectorAccountSelection

    wanted = {str(a) for a in account_ids}
    writable_count = 0
    for row in ConnectorAccountSelection.objects.filter(data_source=data_source):
        should = row.account_id in wanted
        if row.writes_enabled != should:
            row.writes_enabled = should
            row.save(update_fields=["writes_enabled", "updated_at"])
        if should:
            writable_count += 1
    return writable_count


def _apply_deltas(data_source, changes: dict, field: str) -> int:
    """Flip `field` on only the named accounts, leaving every other row untouched.

    `changes` is `{account_id: bool}` — each entry is an independent "turn this
    account on/off". Because untouched rows are never written, two admins editing
    *different* accounts cannot clobber each other (unlike a whole-set replace,
    where the last full save wins everything). A same-account race degrades to a
    single boolean's last-writer-wins, which is harmless. Unknown account ids are
    ignored. Returns the resulting count of rows where `field` is True.
    """
    from terno_dbi.core.models import ConnectorAccountSelection

    wanted = {str(aid): bool(val) for aid, val in (changes or {}).items()}
    if wanted:
        rows = {
            r.account_id: r
            for r in ConnectorAccountSelection.objects.filter(
                data_source=data_source, account_id__in=list(wanted),
            )
        }
        for account_id, value in wanted.items():
            row = rows.get(account_id)
            if row is None:
                continue   # account not (or no longer) visible; skip silently
            if getattr(row, field) != value:
                setattr(row, field, value)
                row.save(update_fields=[field, "updated_at"])

    return ConnectorAccountSelection.objects.filter(
        data_source=data_source, **{field: True},
    ).count()


def apply_enabled_deltas(data_source, changes: dict) -> int:
    """Per-account read-enable changes; see `_apply_deltas`. Returns enabled count."""
    return _apply_deltas(data_source, changes, "enabled")


def apply_writes_deltas(data_source, changes: dict) -> int:
    """Per-account write-enable changes; see `_apply_deltas`. Returns writes count."""
    return _apply_deltas(data_source, changes, "writes_enabled")


def writes_enabled_account_ids(data_source) -> Set[str]:
    """The accounts opted into write actions for this connection.

    Unlike `enabled_account_ids`, this never returns None: writes are off by
    default, so "no rows" and "no account opted in" both mean the empty set —
    an empty set that denies every write, which is the safe default.
    """
    from terno_dbi.core.models import ConnectorAccountSelection

    rows = (
        ConnectorAccountSelection.objects
        .filter(data_source=data_source, writes_enabled=True)
        .values_list("account_id", flat=True)
    )
    return set(rows)


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
    "set_writes_enabled_accounts",
    "apply_enabled_deltas",
    "apply_writes_deltas",
    "enabled_account_ids",
    "writes_enabled_account_ids",
    "restrict_to_selection",
]
