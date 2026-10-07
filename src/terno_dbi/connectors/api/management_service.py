"""Framework-agnostic service layer for connector management.

Browser views and the desktop connector proxy both use these functions with the
same resolved `(user, org)`. Authorization and connector operations live here;
callers only handle authentication, request parsing, and response formatting.

Errors are raised as `ConnectorManagementError` subclasses and mapped by each
caller to its own response format.
"""

from __future__ import annotations

import logging

from django.conf import settings

from terno_dbi.services.secrets import decrypt_dict, encrypt_dict
from terno_dbi.connectors.api import registry
from terno_dbi.connectors.api.auth import account_selection, rbac

logger = logging.getLogger(__name__)


# --- typed errors ----------------------------------------------------------

class ConnectorManagementError(Exception):
    """Base class for an expected connector-management failure."""


class ConnectorPermissionDenied(ConnectorManagementError):
    """The user is not allowed to perform this management action."""


class ConnectorNotConnected(ConnectorManagementError):
    """No connected datasource exists for this connector in this org."""


class ConnectorBadRequest(ConnectorManagementError):
    """The request payload was malformed, or the provider rejected the call."""


# --- authorization ----------------------------------------------------------

def is_org_admin(user, org) -> bool:
    """Whether `user` may manage (connect/disconnect/configure) sources for `org`.

    Connecting a source exposes the connector's data to the whole organisation on
    that user's provider credentials, so it is an admin action — mirroring how a
    database source is added. The admin set is: a member of the configurable
    "Org Admin" group (the same group that grants the agent its admin scope), the
    organisation's owner, or a Django superuser. Querying a connected source
    stays open to every member (narrowed only by the account allowlist).
    """
    if getattr(user, "is_superuser", False):
        return True
    if getattr(org, "owner_id", None) == getattr(user, "id", None):
        return True
    group = getattr(settings, "TERNO_ORG_ADMIN_GROUP", "Org Admin")
    return user.groups.filter(name=group).exists()


# --- connection-state helpers -------------------------------------------------

def _connector_status(ds) -> str:
    """Map a DataSource's auth_status onto the card status the gallery renders."""
    from terno_dbi.core.models import DataSource

    if ds is None:
        return "not_connected"
    return {
        DataSource.AuthStatus.CONNECTED: "connected",
        DataSource.AuthStatus.EXPIRED: "expired",
        DataSource.AuthStatus.ERROR: "error",
        DataSource.AuthStatus.NOT_AUTHENTICATED: "not_connected",
    }.get(ds.auth_status, "not_connected")


def _connected_email(ds) -> str:
    try:
        bundle = decrypt_dict(ds.connection_json) or {}
    except Exception:   # noqa: BLE001
        return ""
    if not isinstance(bundle, dict):
        return ""
    return bundle.get("CONNECTED_EMAIL", "") or ""


def _get_login_customer_id(ds) -> str:
    """The per-connection manager (MCC) id, digits only, or ''."""
    try:
        bundle = decrypt_dict(ds.connection_json) or {}
    except Exception:   # noqa: BLE001
        return ""
    if not isinstance(bundle, dict):
        return ""
    return str(bundle.get("LOGIN_CUSTOMER_ID", "") or "").replace("-", "")


def _set_login_customer_id(ds, value: str) -> None:
    """Store (or clear, when blank) the manager id in the encrypted bundle."""
    try:
        bundle = decrypt_dict(ds.connection_json) or {}
    except Exception:   # noqa: BLE001
        bundle = {}
    if not isinstance(bundle, dict):
        return
    cid = str(value or "").strip().replace("-", "")
    if cid:
        bundle["LOGIN_CUSTOMER_ID"] = cid
    else:
        bundle.pop("LOGIN_CUSTOMER_ID", None)
    ds.connection_json = encrypt_dict(bundle)
    ds.save(update_fields=["connection_json"])


def _connected_datasource(org, connector_key):
    from terno_dbi.core.models import DataSource

    rows = list(DataSource.objects.filter(
        organisation=org, catalog__key=connector_key, catalog__family="api",
    ).select_related("catalog"))
    if not rows:
        return None
    for ds in rows:
        if ds.auth_status == DataSource.AuthStatus.CONNECTED:
            return ds
    return rows[0]


def _connector_cards(org) -> list:
    """A card per enabled API connector, carrying this org's connection state.

    The *offer* is the ConnectorCatalog (family=api, enabled); the *state* is the
    org's DataSource for each. Every connector declared and enabled in ternodbi
    appears here automatically — there is no per-connector code. A connected row
    wins over an unauthenticated leftover for the same source.
    """
    from terno_dbi.core.models import ConnectorAccountSelection, ConnectorCatalog, DataSource
    from django.db.models import Count, Q

    catalogs = list(ConnectorCatalog.objects.filter(family="api", enabled=True))
    ds_by_key = {}
    for ds in (DataSource.objects
               .filter(organisation=org, catalog__family="api")
               .select_related("catalog")):
        key = ds.catalog.key
        if key not in ds_by_key or ds.auth_status == DataSource.AuthStatus.CONNECTED:
            ds_by_key[key] = ds

    ds_ids = [ds.id for ds in ds_by_key.values() if ds]
    counts_by_ds = {
        row["data_source"]: row
        for row in (ConnectorAccountSelection.objects
                    .filter(data_source_id__in=ds_ids)
                    .values("data_source")
                    .annotate(enabled=Count("id", filter=Q(enabled=True)),
                              writable=Count("id", filter=Q(writes_enabled=True))))
    }

    cards = []
    for cat in catalogs:
        ds = ds_by_key.get(cat.key)
        connected = ds is not None and _connector_status(ds) == "connected"
        counts = counts_by_ds.get(ds.id) if ds else None
        cards.append({
            "key": cat.key,
            "name": cat.name,
            "provider": cat.provider,
            "category": cat.category,
            "description": cat.summary,
            "icon_url": cat.icon_url,
            "most_popular": cat.most_popular,
            "status": _connector_status(ds),
            "datasource_id": ds.id if ds else None,
            "last_error": (getattr(ds, "auth_error", "") if ds else "") or "",
            # Connection identity + account state, only meaningful when connected.
            "connected_email": _connected_email(ds) if connected else "",
            "enabled_account_count": (counts["enabled"] if counts else 0),
            "writes_enabled_count": (counts["writable"] if counts else 0),
        })
    return cards


# --- service functions (one per management operation) -------------------------

def list_connectors(user, org) -> dict:
    """The API connectors this org can use, with connection state.

    Open to every member; ``can_manage`` tells the UI whether to show the
    admin-only Connect/Disconnect controls (the mutating ops enforce it too).
    """
    return {
        "connectors": _connector_cards(org),
        "can_manage": is_org_admin(user, org),
    }


def disconnect_connector(user, org, connector_key) -> dict:
    """Disconnect an API source for this org (admin-only).

    Clears the stored OAuth tokens and marks the DataSource not-authenticated.
    The row is *kept* on purpose — deleting it would cascade away the source's
    memory, schema metadata and history, which a disconnect must not destroy. All
    rows for the connector are cleared (a source connected more than once before
    the reuse fix may have duplicates).
    """
    from terno_dbi.core.models import DataSource

    if not is_org_admin(user, org):
        raise ConnectorPermissionDenied(
            "Only organisation admins can disconnect a data source.")

    rows = list(DataSource.objects.filter(
        organisation=org, catalog__key=connector_key, catalog__family="api",
    ))
    if not rows:
        raise ConnectorNotConnected("Not connected.")

    for ds in rows:
        ds.connection_json = {}
        ds.auth_status = DataSource.AuthStatus.NOT_AUTHENTICATED
        fields = ["connection_json", "auth_status"]
        if hasattr(ds, "auth_error"):
            ds.auth_error = ""
            fields.append("auth_error")
        ds.save(update_fields=fields)
    logger.info("disconnected API connector %r for org %s (%d row(s))",
                connector_key, org.id, len(rows))
    return {"status": "disconnected", "key": connector_key}


def _require_managed_ds(user, org, connector_key):
    """Admin-gate account management and return the connected datasource."""
    if not is_org_admin(user, org):
        raise ConnectorPermissionDenied(
            "Only organisation admins can manage connector accounts.")
    ds = _connected_datasource(org, connector_key)
    if ds is None:
        raise ConnectorNotConnected("Not connected.")
    return ds


def list_accounts(user, org, connector_key) -> dict:
    """List every account the credential can see (within RBAC), each with its
    ``enabled`` flag, refreshing the cached set from the provider. Drives the
    account-picker modal, so it returns *all* accounts, not only enabled ones.
    """
    from terno_dbi.connectors.api.model.errors import ApiError

    ds = _require_managed_ds(user, org, connector_key)
    try:
        connector = registry.build_connector(ds)
        accounts = connector.list_accounts()
    except ApiError as exc:
        raise ConnectorBadRequest(exc.message)

    permitted = rbac.permitted_accounts(ds, user.groups.all())
    visible = rbac.filter_accounts(accounts, permitted)
    rows = account_selection.sync_account_selections(ds, visible)
    return {
        "accounts": rows,
        "count": len(rows),
        "enabled_count": sum(1 for r in rows if r["enabled"]),
        "writes_enabled_count": sum(1 for r in rows if r.get("writes_enabled")),
        "email": _connected_email(ds),
        "login_customer_id": _get_login_customer_id(ds),
    }


def save_accounts(user, org, connector_key, body: dict) -> dict:
    """Update which accounts on a connected source are queryable (admin-only).

    Supports per-account deltas, which avoid overwriting concurrent changes, and
    the legacy whole-set form, which replaces the full account set.

    `login_customer_id` can be updated with either form.
    """
    ds = _require_managed_ds(user, org, connector_key)
    body = body or {}

    result = {"status": "saved"}
    if "login_customer_id" in body:
        _set_login_customer_id(ds, str(body.get("login_customer_id") or ""))
        result["login_customer_id"] = _get_login_customer_id(ds)

    # Delta shape takes precedence when either key is present: apply only the
    # named accounts, so concurrent edits to different accounts don't clobber.
    if "enabled_deltas" in body or "writes_deltas" in body:
        enabled_deltas = body.get("enabled_deltas") or {}
        writes_deltas = body.get("writes_deltas") or {}
        if not isinstance(enabled_deltas, dict) or not isinstance(writes_deltas, dict):
            raise ConnectorBadRequest(
                "enabled_deltas and writes_deltas must be objects.")
        if "enabled_deltas" in body:
            result["enabled_count"] = account_selection.apply_enabled_deltas(
                ds, enabled_deltas)
        if "writes_deltas" in body:
            result["writes_enabled_count"] = account_selection.apply_writes_deltas(
                ds, writes_deltas)
        return result

    if "account_ids" in body:
        account_ids = body.get("account_ids") or []
        if not isinstance(account_ids, list):
            raise ConnectorBadRequest("account_ids must be a list.")
        result["enabled_count"] = account_selection.set_enabled_accounts(
            ds, account_ids)
        if "writes_account_ids" in body:
            writes_ids = body.get("writes_account_ids") or []
            if not isinstance(writes_ids, list):
                raise ConnectorBadRequest("writes_account_ids must be a list.")
            result["writes_enabled_count"] = (
                account_selection.set_writes_enabled_accounts(ds, writes_ids)
            )

    return result
