"""Memory service — persistent, scoped facts an agent can recall across sessions.
"""
import logging

import reversion
from django.contrib.auth.models import User
from django.core.exceptions import ObjectDoesNotExist
from django.db import transaction
from django.db.models import Q

from terno_dbi.core.models import Memory

logger = logging.getLogger(__name__)


class MemoryError(Exception):
    """Base for memory errors that map to caller-facing messages."""


class MemoryNotFound(MemoryError):
    pass


class MemoryConflict(MemoryError):
    """Read-before-write violated: missing/stale ``expected_hash`` on an update."""


class MemoryNotUnique(MemoryError):
    """``old_string`` matched more than once and ``replace_all`` was not set."""

    def __init__(self, count):
        self.count = count
        super().__init__(f"old_string is not unique (found {count} times)")


class MemoryNoMatch(MemoryError):
    """``old_string`` was not found in the memory body."""


class MemoryPermission(MemoryError):
    """Caller may not perform this write (e.g. missing author, or org store)."""


# --- helpers ----------------------------------------------------------------

def _visible_qs(organisation_id, user_id):
    """Rows a user may read: all org-store memories + their own user-store ones."""
    return Memory.objects.filter(organisation_id=organisation_id).filter(
        Q(store=Memory.Store.ORG) | Q(store=Memory.Store.USER, created_by_id=user_id)
    )


def _set_revision_meta(user_id, comment):
    """Attach the acting user + a comment to the current reversion revision."""
    reversion.set_comment(comment)
    if user_id:
        user = User.objects.filter(id=user_id).first()
        if user:
            reversion.set_user(user)


def serialize(mem):
    """Full memory (metadata + body + content_hash)."""
    return {
        "name": mem.name,
        "description": mem.description,
        "type": mem.memory_type,
        "scope": mem.scope,
        "datasource_id": mem.data_source_id,
        "datasource_name": mem.data_source.display_name if mem.data_source_id else None,
        "store": mem.store,
        "created_by": mem.created_by.username if mem.created_by_id else None,
        "content": mem.content,
        "content_hash": mem.content_hash,
    }


def _index_row(v):
    """One index entry (no body) from a ``.values()`` dict."""
    return {
        "name": v["name"],
        "description": v["description"],
        "type": v["memory_type"],
        "store": v["store"],
        "scope": ("datasource:%s" % v["data_source_id"]
                  if v["data_source_id"] else "global"),
        "datasource_id": v["data_source_id"],
        "datasource_name": v.get("data_source__display_name"),
    }


# --- read side --------------------------------------------------------------

def list_memories(organisation_id, user_id, data_source_id=None):
    """The memory index (name/description/type/scope only — never the body).

    Omit ``data_source_id`` to list every visible memory (all scopes). Pass it
    to restrict to global + that datasource's memories.
    """
    qs = _visible_qs(organisation_id, user_id)
    if data_source_id is not None:
        qs = qs.filter(Q(data_source__isnull=True) | Q(data_source_id=data_source_id))
    rows = qs.values(
        "name", "description", "memory_type", "store",
        "data_source_id", "data_source__display_name",
    ).order_by("store", "data_source_id", "name")
    return [_index_row(r) for r in rows]


def render_index(rows):
    """Render index rows as grouped markdown text (a convenience for clients).

    Groups under ``## Global`` and one ``## Datasource <id> — <name>`` per
    database, one line per memory: ``- [name](name) — description``.
    """
    if not rows:
        return "# Memory Index\n\n(empty)"
    globals_ = [r for r in rows if r["datasource_id"] is None]
    by_ds = {}
    for r in rows:
        if r["datasource_id"] is not None:
            by_ds.setdefault((r["datasource_id"], r["datasource_name"]), []).append(r)

    lines = ["# Memory Index", ""]
    if globals_:
        lines.append("## Global")
        for r in globals_:
            lines.append(f"- [{r['name']}]({r['name']}) — {r['description']}")
        lines.append("")
    for (ds_id, ds_name) in sorted(by_ds, key=lambda k: k[0]):
        lines.append(f"## Datasource {ds_id} — {ds_name}")
        for r in by_ds[(ds_id, ds_name)]:
            lines.append(f"- [{r['name']}]({r['name']}) — {r['description']}")
        lines.append("")
    return "\n".join(lines).strip()


def read_memory(organisation_id, user_id, name, data_source_id=None):
    qs = _visible_qs(organisation_id, user_id).filter(name=name)
    obj = None
    if data_source_id is not None:
        obj = qs.filter(data_source_id=data_source_id).first()
    if obj is None:
        obj = qs.order_by("data_source_id").first()  # global (NULL) first
    if obj is None:
        raise MemoryNotFound(name)
    return obj


def grep_memory(organisation_id, user_id, pattern, data_source_id=None):
    """Regex search over the bodies of visible memories."""
    qs = _visible_qs(organisation_id, user_id).filter(content__iregex=pattern)
    if data_source_id is not None:
        qs = qs.filter(Q(data_source__isnull=True) | Q(data_source_id=data_source_id))
    rows = qs.values(
        "name", "description", "memory_type", "store",
        "data_source_id", "data_source__display_name",
    )
    return [_index_row(r) for r in rows]


# --- write side -------------------------------------------------------------

def _identity(organisation_id, store, name, data_source_id, created_by_id):
    """Scope identity for get/update. For org store the name is shared across
    the org (created_by is NOT part of identity); for user store it is."""
    ident = dict(
        organisation_id=organisation_id, store=store,
        name=name, data_source_id=data_source_id,
    )
    if store == Memory.Store.USER:
        ident["created_by_id"] = created_by_id
    return ident


@transaction.atomic
@reversion.create_revision()
def write_memory(organisation_id, name, description, memory_type, content,
                 store, created_by_id, data_source_id=None, expected_hash=None):
    """Create a memory, or fully replace one in the same scope."""
    if created_by_id is None:
        raise MemoryPermission("created_by is required — a memory must have an author.")

    ident = _identity(organisation_id, store, name, data_source_id, created_by_id)
    existing = Memory.objects.select_for_update().filter(**ident).first()

    if existing is None:
        create_kwargs = dict(ident)
        create_kwargs.setdefault("created_by_id", created_by_id)  # org store: set author
        obj = Memory.objects.create(
            description=description, memory_type=memory_type, content=content,
            **create_kwargs,
        )
        _set_revision_meta(created_by_id, f"create memory '{name}'")
        logger.info("Memory created: org=%s store=%s name=%s ds=%s",
                    organisation_id, store, name, data_source_id)
        return obj, "create"

    if expected_hash is None:
        raise MemoryConflict(
            f"'{name}' already exists — read it first, then pass its content_hash "
            f"as expected_hash to confirm you are replacing the current content."
        )
    if expected_hash != existing.content_hash:
        raise MemoryConflict(
            f"'{name}' changed since you last read it. Re-read it and re-apply "
            f"your change (expected_hash did not match the current content)."
        )

    existing.description = description
    existing.memory_type = memory_type
    existing.content = content
    existing.save()
    _set_revision_meta(created_by_id, f"update memory '{name}'")
    logger.info("Memory updated: org=%s store=%s name=%s", organisation_id, store, name)
    return existing, "update"


@transaction.atomic
@reversion.create_revision()
def edit_memory(organisation_id, name, old_string, new_string, store,
                created_by_id, expected_hash, replace_all=False,
                data_source_id=None):
    """Exact string replacement in a memory body."""
    ident = _identity(organisation_id, store, name, data_source_id, created_by_id)
    try:
        obj = Memory.objects.select_for_update().get(**ident)
    except ObjectDoesNotExist:
        raise MemoryNotFound(name)

    if expected_hash is None or expected_hash != obj.content_hash:
        raise MemoryConflict(
            f"'{name}' must be read immediately before editing — expected_hash "
            f"missing or stale. Re-read it and re-apply your change."
        )

    count = obj.content.count(old_string)
    if count == 0:
        raise MemoryNoMatch(f"old_string not found in '{name}'.")
    if count > 1 and not replace_all:
        raise MemoryNotUnique(count)

    obj.content = obj.content.replace(
        old_string, new_string, -1 if replace_all else 1
    )
    obj.save()
    _set_revision_meta(created_by_id, f"edit memory '{name}'")
    logger.info("Memory edited: org=%s name=%s replacements=%d",
                organisation_id, name, count if replace_all else 1)
    return obj


# --- cross-server transfer ---------------------------------------------------

class _NotSet:
    def __repr__(self):
        return "NOT_SET"


NOT_SET = _NotSet()


def export_row(mem):
    """One Memory as a natural-key-addressed dict, for a portable JSON export.
    """
    return {
        "organisation_subdomain": mem.organisation.subdomain,
        "store": mem.store,
        "datasource_display_name": mem.data_source.display_name if mem.data_source_id else None,
        "name": mem.name,
        "description": mem.description,
        "memory_type": mem.memory_type,
        "content": mem.content,
        "created_at": mem.created_at.isoformat(),
        "updated_at": mem.updated_at.isoformat(),
    }


def import_row(row, target_organisation_id, importing_user_id, can_write_org_memory,
               force_datasource_id=NOT_SET):
    """``can_write_org_memory`` gates ``store='org'`` rows the same way the
    caller's own permission check does elsewhere (Django admin, agent tools)
    — rows requesting org store are skipped, not silently downgraded, when
    the importing user isn't allowed to write org memory.
    """
    from terno_dbi.core.models import DataSource

    name = row.get("name")
    store = row.get("store") or Memory.Store.ORG
    if not name:
        return "skipped", "missing 'name'"
    if store == Memory.Store.ORG and not can_write_org_memory:
        return "skipped", "you don't have permission to write organisation-wide memories"

    author = User.objects.filter(id=importing_user_id).first()
    if author is None:
        return "skipped", "importing user no longer exists"

    data_source = None
    detail_note = None
    if force_datasource_id is not NOT_SET:
        if force_datasource_id is not None:
            data_source = DataSource.objects.filter(id=force_datasource_id).first()
            if data_source is None:
                return "skipped", f"selected datasource id {force_datasource_id} no longer exists"
    elif row.get("datasource_display_name"):
        data_source = DataSource.objects.filter(
            display_name=row["datasource_display_name"]
        ).first()
        if data_source is None:
            return "skipped", f"unknown datasource '{row['datasource_display_name']}'"
    elif row.get("data_source_id"):
        detail_note = "datasource id not portable across servers; imported as global"

    ident = dict(
        organisation_id=target_organisation_id, store=store, name=name,
        data_source_id=data_source.id if data_source else None,
    )
    if store == Memory.Store.USER:
        ident["created_by_id"] = author.id
    if Memory.objects.filter(**ident).exists():
        return "skipped", "already exists"

    try:
        _, action = write_memory(
            organisation_id=target_organisation_id, name=name,
            description=row.get("description", ""), memory_type=row.get("memory_type", "reference"),
            content=row.get("content", ""), store=store,
            created_by_id=author.id, data_source_id=data_source.id if data_source else None,
        )
        return action, detail_note
    except MemoryError as e:
        return "error", str(e)


@transaction.atomic
@reversion.create_revision()
def delete_memory(organisation_id, name, store, created_by_id, data_source_id=None):
    ident = _identity(organisation_id, store, name, data_source_id, created_by_id)
    qs = Memory.objects.filter(**ident)
    exists = qs.exists()
    if exists:
        _set_revision_meta(created_by_id, f"delete memory '{name}'")
    deleted, _ = qs.delete()
    logger.info("Memory delete: org=%s store=%s name=%s removed=%d",
                organisation_id, store, name, deleted)
    return deleted
