"""Pipedrive connector.

Implements the `ApiConnector` interface against the Pipedrive API — v2 wherever
Pipedrive has it, v1 for leads and users, which have no v2 list endpoint:

- `list_accounts()` — `GET /api/v1/users/me` names the one company the OAuth
  token belongs to.
- `list_fields()`   — a curated catalogue of each report's standard fields, plus
  the company's custom fields, discovered live from
  `GET /api/v2/{deal,person,organization}Fields`. Leads and activities are
  curated only: the v2 activity-fields endpoint needs the `admin` scope, and
  v1 leads do not return custom fields the way v2 records do.
- `_run()`          — `GET /api/v2/{deals,persons,organizations,activities}` or
  `GET /api/v1/leads`, paged and filtered to the date range on the report's date
  field (`add_time` unless the `date_field` setting names another). Owner,
  stage, pipeline, person, organization and deal ids are resolved to names on
  request.
- `list_actions()` / `execute_action()` — `create_record` (`POST` to the list
  endpoint) and `update_record` (`PATCH /{id}`, after reading the record's
  current values), for every report type. There is deliberately no delete.

Pipedrive differs from the other connectors:
  * **Every company has its own API host.** The token response names it
    (`api_domain`, e.g. `https://acme.pipedrive.com`), saved as `API_DOMAIN`, and
    every call here goes there once it is checked to be a pipedrive.com host.
  * The token endpoint authenticates the client with HTTP Basic, and the app's
    scopes are set in Pipedrive's Developer Hub, not requested at consent.
  * List endpoints cannot filter on a date, only sort by one. A range on a
    sortable date is read newest first and paging stops once it is passed. A
    range on an event time such as `won_time` is narrowed with `updated_since`,
    since a deal is updated when it is won. Any other date is checked record by
    record.
  * Records carry ids, not names (`owner_id`, `stage_id`, `org_id`, …); each has
    a companion name column (`owner`, `stage`, `organization`, …).
  * Custom fields are keyed by a 40-character hash and returned under
    `custom_fields`; option fields are requested with their labels. A write
    takes an option by label and sends Pipedrive its id.
  * Updates also accept `is_deleted` and `is_archived`, so what a write may set
    is an allowlist per record type — deleting or archiving is never forwarded.
"""

from __future__ import annotations
import logging
import re
from dataclasses import dataclass
from datetime import date, datetime, timezone as _tz
from typing import Any, Callable, Dict, Iterator, List, Optional, Set, Tuple

from terno_dbi.connectors.api.model.base import ApiConnector
from terno_dbi.connectors.api.model.errors import ApiError, ErrorCode, invalid_field
from terno_dbi.connectors.api.model.types import (
    Account, Action, ActionResult, Field, QueryResult, QuerySpec,
)

logger = logging.getLogger(__name__)

_PAGE = 500          # records per call; the cap on every list endpoint used here
_MAX_SCAN = 50000    # records read per query, matched or not — bounds token spend
_MAX_CUSTOM = 15     # custom field keys one v2 list call can be narrowed to
_IDS_PER_CALL = 100  # ids one v2 list call can fetch

# The company API host saved from the token response. Checked before any call
# so a token is only ever sent to Pipedrive.
_API_DOMAIN_RE = re.compile(
    r"^https://[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.pipedrive\.com$")

# How a date range is applied to a date field, since no list endpoint filters on
# one. `_SORT`: the endpoint sorts by the field, so read newest first and stop
# once past the range. `_SINCE`: the field is an event time never later than
# `update_time`, so `updated_since` narrows the read. `_SCAN`: check each record.
_SORT, _SINCE, _SCAN = "sort", "since", "scan"
_DEFAULT_DATE_FIELD = "add_time"
_DATE_ONLY = frozenset({"expected_close_date", "due_date"})


@dataclass(frozen=True)
class _Report:
    path: str                   # the list endpoint
    scope: str                  # read with `<scope>:read` or `<scope>:full`
    date_fields: Dict[str, str]  # date field -> how a range is applied
    fields_path: str = ""       # custom-field metadata, when discoverable
    currency: Tuple[str, ...] = ()   # where a record's currency is

    @property
    def is_v1(self) -> bool:
        return self.path.startswith("/api/v1/")


_REPORTS: Dict[str, _Report] = {
    "Deals": _Report(
        "/api/v2/deals", "deals",
        {"add_time": _SORT, "update_time": _SORT, "won_time": _SINCE,
         "lost_time": _SINCE, "close_time": _SINCE, "stage_change_time": _SINCE,
         "expected_close_date": _SCAN},
        fields_path="/api/v2/dealFields", currency=("currency",)),
    "Leads": _Report(
        "/api/v1/leads", "leads",
        {"add_time": _SORT, "update_time": _SORT, "expected_close_date": _SORT},
        currency=("value", "currency")),
    "Persons": _Report(
        "/api/v2/persons", "contacts",
        {"add_time": _SORT, "update_time": _SORT},
        fields_path="/api/v2/personFields"),
    "Organizations": _Report(
        "/api/v2/organizations", "contacts",
        {"add_time": _SORT, "update_time": _SORT},
        fields_path="/api/v2/organizationFields"),
    "Activities": _Report(
        "/api/v2/activities", "activities",
        {"add_time": _SORT, "update_time": _SORT, "due_date": _SORT,
         "marked_as_done_time": _SINCE}),
}
_DEFAULT_REPORT = "Deals"

# Where each kind of reference is looked up (users come from v1 `/users`).
_REF_PATHS = {
    "person": "/api/v2/persons",
    "org": "/api/v2/organizations",
    "deal": "/api/v2/deals",
    "stage": "/api/v2/stages",
    "pipeline": "/api/v2/pipelines",
}
_REF_LABELS = {"user": "user", "person": "person", "org": "organization",
               "deal": "deal", "stage": "stage", "pipeline": "pipeline"}
# Custom field types that hold a reference, and what they reference.
_CUSTOM_REFS = {"user": "user", "org": "org", "people": "person", "deal": "deal"}


@dataclass(frozen=True)
class _Col:
    """A catalogue column and where its value is read from."""

    field: Field
    key: str = ""         # the record key holding the value (default: field id)
    sub: str = ""         # a key inside that value, e.g. address -> country
    ref: str = ""         # resolve the id to a name: user, stage, org, …
    custom: bool = False  # read from the record's `custom_fields`
    default: bool = True  # selected when a query names no fields
    # A custom option field's `(id, label)` choices, and whether it takes several.
    options: Tuple[Tuple[int, str], ...] = ()
    multi: bool = False

    @property
    def source(self) -> str:
        return self.key or self.field.id


def _dim(fid, name, description="", **kw) -> _Col:
    data_type = kw.pop("data_type", "string")
    return _Col(Field(fid, name, "dimension", description, data_type=data_type), **kw)


def _date(fid, name, description="", **kw) -> _Col:
    return _dim(fid, name, description, data_type="date", **kw)


def _id(fid, name, description) -> _Col:
    return _dim(fid, name, description, default=False)


# --- curated catalogues per report -----------------------------------------

_OWNER = [
    _dim("owner", "Owner", "Name of the Pipedrive user who owns the record.",
         key="owner_id", ref="user"),
    _id("owner_id", "Owner ID", "Id of the Pipedrive user who owns the record."),
]

_CURATED: Dict[str, List[_Col]] = {
    "Deals": [
        _dim("id", "Deal ID", "Pipedrive's id for the deal."),
        _dim("title", "Deal"),
        _dim("status", "Status", "open, won or lost."),
        _dim("pipeline", "Pipeline", key="pipeline_id", ref="pipeline"),
        _dim("stage", "Stage", "Pipeline stage (e.g. Qualified, Negotiations).",
             key="stage_id", ref="stage"),
        *_OWNER,
        _dim("organization", "Organization", key="org_id", ref="org"),
        _dim("person", "Contact person", key="person_id", ref="person"),
        _Col(Field("value", "Value", "metric",
                   "Deal value, in the deal's own currency (see currency).",
                   data_type="number", is_monetary=True)),
        _dim("currency", "Currency", "The deal's currency code (e.g. EUR)."),
        _Col(Field("probability", "Probability (%)", "metric",
                   "Win probability set on the deal, when set.",
                   data_type="number", is_non_aggregatable=True)),
        _date("expected_close_date", "Expected close date"),
        _date("add_time", "Created", "When the deal was created."),
        _date("won_time", "Won", "When the deal was marked won."),
        _date("lost_time", "Lost", "When the deal was marked lost."),
        _dim("lost_reason", "Lost reason"),
        _date("close_time", "Closed", "When the deal was won or lost.",
              default=False),
        _date("stage_change_time", "Stage changed",
              "When the deal last moved stage.", default=False),
        _date("update_time", "Last updated", default=False),
        _dim("origin", "Origin", "How the deal was created (e.g. ManuallyCreated, "
             "API, Import).", default=False),
        _id("pipeline_id", "Pipeline ID", "Id of the deal's pipeline."),
        _id("stage_id", "Stage ID", "Id of the deal's stage."),
        _id("org_id", "Organization ID", "Id of the linked organization."),
        _id("person_id", "Contact person ID", "Id of the linked person."),
    ],
    "Leads": [
        _dim("id", "Lead ID", "Pipedrive's id for the lead (a UUID)."),
        _dim("title", "Lead"),
        *_OWNER,
        _dim("organization", "Organization", key="organization_id", ref="org"),
        _dim("person", "Contact person", key="person_id", ref="person"),
        _Col(Field("value", "Value", "metric",
                   "Potential value, in the lead's own currency (see currency).",
                   data_type="number", is_monetary=True),
             key="value", sub="amount"),
        _dim("currency", "Currency", "The lead value's currency code.",
             key="value", sub="currency"),
        _date("expected_close_date", "Expected close date"),
        _dim("source_name", "Source",
             "Where the lead came from (e.g. Manually created, API)."),
        _dim("was_seen", "Seen", "Whether anyone has opened the lead.",
             data_type="boolean"),
        _date("add_time", "Created", "When the lead was created."),
        _date("update_time", "Last updated", default=False),
        _dim("origin", "Origin", "How the lead was created.", default=False),
        _id("organization_id", "Organization ID", "Id of the linked organization."),
        _id("person_id", "Contact person ID", "Id of the linked person."),
    ],
    "Persons": [
        _dim("id", "Person ID", "Pipedrive's id for the person."),
        _dim("name", "Name"),
        _dim("email", "Email", "Primary email address.", key="emails"),
        _dim("phone", "Phone", "Primary phone number.", key="phones"),
        _dim("organization", "Organization", key="org_id", ref="org"),
        *_OWNER,
        _date("add_time", "Created", "When the person was added."),
        _date("update_time", "Last updated"),
        _dim("first_name", "First name", default=False),
        _dim("last_name", "Last name", default=False),
        _id("org_id", "Organization ID", "Id of the linked organization."),
    ],
    "Organizations": [
        _dim("id", "Organization ID", "Pipedrive's id for the organization."),
        _dim("name", "Name"),
        *_OWNER,
        _dim("address", "Address", "Full address.", key="address", sub="value"),
        _dim("country", "Country", key="address", sub="country"),
        _dim("city", "City", key="address", sub="locality"),
        _dim("website", "Website"),
        _Col(Field("employee_count", "Employees", "metric",
                   "Number of employees, when set.", data_type="integer")),
        _date("add_time", "Created", "When the organization was added."),
        _date("update_time", "Last updated"),
    ],
    "Activities": [
        _dim("id", "Activity ID", "Pipedrive's id for the activity."),
        _dim("subject", "Subject"),
        _dim("type", "Type", "Activity type key (e.g. call, meeting, task)."),
        _dim("done", "Done", "Whether the activity is marked done.",
             data_type="boolean"),
        _date("due_date", "Due date"),
        _dim("due_time", "Due time", "Time of day it is due (HH:MM), when set."),
        _dim("duration", "Duration", "Planned length (HH:MM), when set."),
        *_OWNER,
        _dim("deal", "Deal", key="deal_id", ref="deal"),
        _dim("person", "Contact person", key="person_id", ref="person"),
        _dim("organization", "Organization", key="org_id", ref="org"),
        _date("marked_as_done_time", "Completed",
              "When the activity was marked done."),
        _date("add_time", "Created", "When the activity was created."),
        _date("update_time", "Last updated", default=False),
        _id("deal_id", "Deal ID", "Id of the linked deal."),
        _id("person_id", "Contact person ID", "Id of the linked person."),
        _id("org_id", "Organization ID", "Id of the linked organization."),
        _id("lead_id", "Lead ID", "Id of the linked lead."),
    ],
}


def _curated(report_type: str) -> Dict[str, _Col]:
    return {c.field.id: c for c in _CURATED[report_type]}


def _custom_col(meta: Dict[str, Any]) -> _Col:
    """A column for one custom field from Pipedrive's field metadata.

    Numeric types become metrics; everything else (text, options, dates, people)
    is a dimension. A reference field resolves to the name it points at.
    """
    code = meta["field_code"]
    label = meta.get("field_name") or code
    description = meta.get("description") or ""
    ftype = meta.get("field_type") or ""
    group = "Custom fields"
    if ftype == "monetary":
        field = Field(code, label, "metric",
                      description or "Amount only; its currency is set per record.",
                      data_type="number", group=group, is_monetary=True)
    elif ftype in ("int", "double"):
        field = Field(code, label, "metric", description, group=group,
                      data_type="integer" if ftype == "int" else "number")
    elif ftype in ("date", "boolean"):
        field = Field(code, label, "dimension", description, data_type=ftype,
                      group=group)
    else:
        field = Field(code, label, "dimension", description, group=group)
    options = tuple(
        (o["id"], str(o.get("label") or ""))
        for o in meta.get("options") or []
        if isinstance(o, dict) and isinstance(o.get("id"), int)
    ) if ftype in ("enum", "set") else ()
    return _Col(field, custom=True, ref=_CUSTOM_REFS.get(ftype, ""), default=False,
                options=options, multi=ftype == "set")


# --- write actions ----------------------------------------------------------

# The value kinds a writable field takes. Reference kinds are the ones resolved
# to names on read, and each is checked to exist before a write.
_TEXT, _NUMBER, _BOOL, _DATE, _TIME = "text", "number", "bool", "date", "time"
_REF_KINDS = frozenset(_REF_LABELS)

# What each record type accepts on a write, by the API's own field names. An
# allowlist on purpose: Pipedrive's update endpoints also take `is_deleted` and
# `is_archived`, so anything unlisted is refused rather than forwarded.
_WRITABLE: Dict[str, Dict[str, str]] = {
    "Deals": {
        "title": _TEXT, "owner_id": "user", "person_id": "person",
        "org_id": "org", "pipeline_id": "pipeline", "stage_id": "stage",
        "value": _NUMBER, "currency": "currency", "status": "status",
        "lost_reason": _TEXT, "probability": _NUMBER,
        "expected_close_date": _DATE,
    },
    "Leads": {
        "title": _TEXT, "owner_id": "user", "person_id": "person",
        "organization_id": "org", "value": "money", "expected_close_date": _DATE,
    },
    "Persons": {
        "name": _TEXT, "owner_id": "user", "org_id": "org",
        "emails": "contacts", "phones": "contacts",
    },
    "Organizations": {
        "name": _TEXT, "owner_id": "user", "website": _TEXT, "address": "address",
    },
    "Activities": {
        "subject": _TEXT, "type": _TEXT, "owner_id": "user", "deal_id": "deal",
        "lead_id": "lead", "person_id": "person", "org_id": "org",
        "due_date": _DATE, "due_time": _TIME, "duration": _TIME, "done": _BOOL,
        "note": _TEXT,
    },
}
# Fields a record cannot be without: required on create, never cleared.
_REQUIRED: Dict[str, Tuple[str, ...]] = {
    "Deals": ("title",), "Leads": ("title",), "Persons": ("name",),
    "Organizations": ("name",), "Activities": (),
}
_NEVER_WRITTEN = frozenset({"is_deleted", "is_archived", "archive_time"})
_STATUSES = ("open", "won", "lost")

_INT_ID_RE = re.compile(r"^[1-9]\d{0,17}$")
_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.IGNORECASE)
_TIME_RE = re.compile(r"^([01]\d|2[0-3]):[0-5]\d$")
_CURRENCY_RE = re.compile(r"^[A-Za-z]{3}$")
_MAX_LISTED = 30   # choices named in a "no such user/stage" error


def _fields_description() -> str:
    per_type = "; ".join(f"{rt}: {', '.join(fields)}" for rt, fields in _WRITABLE.items())
    return (
        f"Field names mapped to values. Standard fields by record type — "
        f"{per_type}. Deals, Persons and Organizations also take custom fields "
        f"by their 40-character id from list_fields. References (owner_id, "
        f"stage_id, org_id, …) take numeric ids; lead_id a lead's UUID; dates "
        f"'YYYY-MM-DD'; due_time and duration 'HH:MM'; status open, won or lost; "
        f"a lead's value {{\"amount\", \"currency\"}}; emails and phones one "
        f"address or a list of {{\"value\", \"primary\"}}, replacing the whole "
        f"list; option custom fields an option's label. null clears a field."
    )


_RECORD_TYPE_PROP = {"type": "string", "enum": list(_REPORTS),
                     "description": "The kind of record (the report type)."}
_FIELDS_PROP = {"type": "object", "minProperties": 1,
                "description": _fields_description()}

_ACTIONS: List[Action] = [
    Action(
        "create_record", "Create a record",
        "Create one Pipedrive record — a deal, lead, person, organization or "
        "activity. A deal needs a title, a person or organization a name, and a "
        "lead a title plus a person_id or organization_id.",
        schema={"type": "object",
                "properties": {"record_type": _RECORD_TYPE_PROP,
                               "fields": _FIELDS_PROP},
                "required": ["record_type", "fields"],
                "additionalProperties": False},
        destructive=False,
    ),
    Action(
        "update_record", "Update a record",
        "Change field values on one existing record — e.g. move a deal to "
        "another stage, mark it won or lost, reassign its owner, or mark an "
        "activity done. Only the fields given are changed. Records are never "
        "deleted or archived.",
        schema={"type": "object",
                "properties": {
                    "record_type": _RECORD_TYPE_PROP,
                    "record_id": {"type": "string",
                                  "description": "The record's id (the `id` "
                                                 "field from data_query): a "
                                                 "number, or a UUID for a lead."},
                    "fields": _FIELDS_PROP,
                },
                "required": ["record_type", "record_id", "fields"],
                "additionalProperties": False},
    ),
]
_ACTIONS_BY_ID: Dict[str, Action] = {a.id: a for a in _ACTIONS}


@dataclass(frozen=True)
class _Change:
    """A validated write: the caller's fields and what Pipedrive is sent."""

    given: Dict[str, Any]         # the fields as the caller named them
    standard: Dict[str, Any]      # API values for standard fields
    custom: Dict[str, Any]        # API values for custom fields, by id
    refs: Tuple[Tuple[str, str, int], ...]   # (field, kind, id) that must exist


# Pipedrive has no validate-only mode for writes, so a dry run checks what can be
# checked here and says plainly what it did not.
def _dry_run_details(checked: str, unverified: List[str]) -> Dict[str, Any]:
    details: Dict[str, Any] = {
        "dry_run": True, "applied": False,
        "validated": f"{checked}; Pipedrive applies its own rules (required "
                     f"fields, user permissions) only when the change is applied",
    }
    if unverified:
        details["unverified"] = unverified   # linked records the lookup failed for
    return details


def _bad_value(name: str, expected: str, value) -> ApiError:
    return ApiError(
        ErrorCode.INVALID_ACTION_PARAMS,
        f"{name!r} must be {expected}, got {value!r}.",
        retriable=False, details={"field": name},
    )


def _record_number(name: str, value) -> int:
    """A numeric Pipedrive id, from an int or a string of digits."""
    if not isinstance(value, bool) and _INT_ID_RE.match(str(value).strip()):
        return int(str(value).strip())
    raise _bad_value(name, "a numeric Pipedrive id", value)


def _write_value(name: str, kind: str, value):
    """`value` in the shape Pipedrive takes for a field of `kind`, or an error."""
    if value is None:
        return None
    if kind in _REF_KINDS:
        return _record_number(name, value)
    if kind == _TEXT and isinstance(value, str):
        return value
    if kind == _NUMBER and isinstance(value, (int, float)) and not isinstance(value, bool):
        return value
    if kind == _BOOL and isinstance(value, bool):
        return value
    if kind == _DATE and isinstance(value, str):
        try:
            return datetime.strptime(value, "%Y-%m-%d").date().isoformat()
        except ValueError:
            pass
    if kind == _TIME and isinstance(value, str) and _TIME_RE.match(value):
        return value
    if kind == "currency" and isinstance(value, str) and _CURRENCY_RE.match(value):
        return value.upper()
    if kind == "status" and value in _STATUSES:
        return value
    if kind == "lead" and isinstance(value, str) and _UUID_RE.match(value):
        return value.lower()
    if (kind == "money" and isinstance(value, dict)
            and set(value) == {"amount", "currency"}
            and isinstance(value["amount"], (int, float))
            and not isinstance(value["amount"], bool)
            and isinstance(value["currency"], str)
            and _CURRENCY_RE.match(value["currency"])):
        return {"amount": value["amount"], "currency": value["currency"].upper()}
    if kind == "contacts":
        return _contacts(name, value)
    if kind == "address":
        if isinstance(value, str) and value.strip():
            return {"value": value}
        if (isinstance(value, dict) and isinstance(value.get("value"), str)
                and all(isinstance(v, str) for v in value.values())):
            return dict(value)
    raise _bad_value(name, _EXPECTED[kind], value)


_EXPECTED = {
    _TEXT: "text", _NUMBER: "a number", _BOOL: "true or false",
    _DATE: "a 'YYYY-MM-DD' date", _TIME: "an 'HH:MM' time",
    "currency": "a three-letter currency code", "status": "open, won or lost",
    "lead": "a lead's UUID",
    "money": 'an object {"amount": <number>, "currency": "<code>"}',
    "address": "an address string, or an object with its 'value'",
}


def _contacts(name: str, value) -> List[Dict[str, Any]]:
    """Emails or phones as Pipedrive's list of `{value, primary, label}`.

    One address becomes a one-entry list. Exactly one entry is primary: the one
    marked so, else the first.
    """
    expected = 'an address, or a list of {"value", "primary", "label"} entries'
    entries = [value] if isinstance(value, str) else value
    if not isinstance(entries, list) or not entries:
        raise _bad_value(name, expected, value)
    out: List[Dict[str, Any]] = []
    for entry in entries:
        if isinstance(entry, str):
            entry = {"value": entry}
        if (not isinstance(entry, dict) or set(entry) - {"value", "primary", "label"}
                or not isinstance(entry.get("value"), str) or not entry["value"].strip()
                or not isinstance(entry.get("label", ""), str)):
            raise _bad_value(name, expected, value)
        item: Dict[str, Any] = {"value": entry["value"],
                                "primary": entry.get("primary") is True}
        if entry.get("label"):
            item["label"] = entry["label"]
        out.append(item)
    primaries = sum(item["primary"] for item in out)
    if primaries > 1:
        raise _bad_value(name, "a list with one primary entry", value)
    if primaries == 0:
        out[0]["primary"] = True
    return out


def _custom_write_value(col: _Col, value):
    """A custom field's value as Pipedrive takes it: an option by id, a set as ids."""
    name = col.field.id
    if col.ref:
        return None if value is None else _record_number(name, value)
    if not col.options:
        return value   # Pipedrive checks the value's shape itself
    if col.multi:
        if value is None or value == []:
            return None   # Pipedrive clears a selection with null, never []
        items = value if isinstance(value, list) else [value]
        return [_option_id(col, v) for v in items]
    return None if value is None else _option_id(col, value)


def _option_id(col: _Col, value) -> int:
    """The id of the option `value` names — by label (any case) or by id."""
    if not isinstance(value, bool):
        text = str(value).strip()
        for oid, label in col.options:
            if text == str(oid) or text.casefold() == label.casefold():
                return oid
    raise ApiError(
        ErrorCode.INVALID_ACTION_PARAMS,
        f"{col.field.name!r} ({col.field.id}) has no option {value!r}. Options: "
        f"{', '.join(label for _, label in col.options)}.",
        retriable=False, details={"field": col.field.id},
    )


def _participants(person_id: Optional[int], existing: Optional[Dict[str, Any]]):
    """An activity's participants with `person_id` as the primary one.

    Pipedrive sets an activity's person through its participant list, which a
    write replaces whole — so the other (non-primary) participants carry over.
    """
    others = [
        {"person_id": p["person_id"], "primary": False}
        for p in (existing or {}).get("participants") or []
        if isinstance(p, dict) and not p.get("primary")
        and p.get("person_id") not in (None, person_id)
    ]
    lead = [{"person_id": person_id, "primary": True}] if person_id is not None else []
    return lead + others


def _item_name(kind: str, item: Dict[str, Any]) -> str:
    if kind == "user":
        return item.get("name") or item.get("email") or str(item["id"])
    return item.get("title" if kind == "deal" else "name") or str(item["id"])


def _missing_ref(kind: str, field: str, rid: str, items: List[Dict[str, Any]]) -> ApiError:
    """No such user/stage/…; the choices are named when there are few enough kinds."""
    label = _REF_LABELS[kind]
    message = f"{field!r}: no Pipedrive {label} has id {rid}."
    if kind in ("user", "stage", "pipeline") and items:
        listed = [
            f"{i['id']} {_item_name(kind, i)}"
            + (f" (pipeline {i.get('pipeline_id')})" if kind == "stage" else "")
            for i in items[:_MAX_LISTED] if i.get("id") is not None
        ]
        more = "…" if len(items) > _MAX_LISTED else ""
        message += f" {label.capitalize()}s: {', '.join(listed)}{more}."
    else:
        message += " Check the id with data_query."
    return ApiError(ErrorCode.INVALID_ACTION_PARAMS, message, retriable=False,
                    details={"field": field, "id": rid})


# --- dates -----------------------------------------------------------------


def _date_range(start: str, end: str) -> Tuple[date, date]:
    """The inclusive range as dates, rejecting anything but real calendar dates."""
    try:
        s = datetime.strptime(start, "%Y-%m-%d").date()
        e = datetime.strptime(end, "%Y-%m-%d").date()
    except (TypeError, ValueError):
        raise ApiError(
            ErrorCode.INVALID_FILTER,
            f"date_range must be absolute 'YYYY-MM-DD' dates, got {start!r} to "
            f"{end!r}. Resolve relative ranges (e.g. 'last 30 days') with "
            f"get_today first.",
            retriable=False,
        )
    if s > e:
        raise ApiError(
            ErrorCode.INVALID_FILTER,
            f"date_range.start ({start}) is after date_range.end ({end}).",
            retriable=False,
        )
    return s, e


def _date_of(raw) -> Optional[date]:
    """The calendar date of a Pipedrive date or timestamp (UTC), or None.

    v2 timestamps are RFC 3339 (`2026-08-01T10:20:00Z`), v1 ones ISO 8601 with
    milliseconds; dates such as `due_date` are plain `YYYY-MM-DD`.
    """
    if not isinstance(raw, str) or not raw.strip():
        return None
    text = raw.strip()
    try:
        if len(text) == 10:
            return date.fromisoformat(text)
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        moment = datetime.fromisoformat(text)
    except ValueError:
        return None
    if moment.tzinfo is not None:
        moment = moment.astimezone(_tz.utc)
    return moment.date()


# --- transport -------------------------------------------------------------


def _default_http(method: str, url: str, token: str,
                  json_body: Optional[Dict] = None) -> Dict[str, Any]:
    import requests
    kwargs: Dict[str, Any] = {}
    if method == "GET":
        kwargs["params"] = json_body or None
    else:
        kwargs["json"] = json_body
    resp = requests.request(
        method, url, headers={"Authorization": f"Bearer {token}"},
        timeout=30, **kwargs)
    if resp.status_code >= 400:
        raise _pipedrive_error(resp)
    return resp.json()


def _pipedrive_error(resp) -> Exception:
    """Turn a Pipedrive error response into an actionable exception.

    Pipedrive names the problem in the body's `error`. A missing scope is a 403
    whose fix is the app's scopes plus a reconnect, not a retry. Only 429 and
    5xx are retriable.
    """
    status = resp.status_code
    message = ""
    try:
        body = resp.json()
        if isinstance(body, dict):
            message = str(body.get("error") or body.get("message") or "")
    except ValueError:
        message = (resp.text or "")[:200]   # non-JSON (e.g. an HTML error page)

    if status == 401:
        return _AuthError()
    if status == 403 and "scope" in message.lower():
        return ApiError(
            ErrorCode.AUTH_EXPIRED,
            "Pipedrive refused a permission this connection was not granted. "
            "The Pipedrive app must request it (Developer Hub → OAuth & access "
            "scopes); then reconnect the source.",
            retriable=False,
        )
    if status == 429:
        return ApiError(
            ErrorCode.RATE_LIMITED,
            f"Pipedrive's rate limit was reached: {message or 'too many requests'}.",
            retriable=True, retry_after_seconds=_retry_after(resp),
        )
    return ApiError(
        ErrorCode.UPSTREAM_ERROR,
        f"Pipedrive API error ({status}): {message or 'unknown error'}",
        retriable=status >= 500,
        details={"status": status, "error": message},
    )


def _retry_after(resp) -> Optional[int]:
    headers = getattr(resp, "headers", None) or {}
    for name in ("Retry-After", "X-RateLimit-Reset"):
        try:
            return int(headers[name])
        except (KeyError, TypeError, ValueError):
            continue
    return None


class _AuthError(Exception):
    """Internal marker for a 401 from Pipedrive, mapped to AUTH_EXPIRED."""


class PipedriveConnector(ApiConnector):
    def __init__(self, datasource, http: Optional[Callable] = None,
                 token_refresher: Optional[Callable] = None):
        super().__init__(datasource, token_refresher=token_refresher)
        self._http = http or _default_http
        # Column catalogue per report, cached for the connector's lifetime so a
        # query does not refetch field metadata.
        self._catalogue_cache: Dict[str, Dict[str, _Col]] = {}

    # -- transport ----------------------------------------------------------

    def _api_base(self) -> str:
        domain = (self._tokens().get("API_DOMAIN") or "").rstrip("/").lower()
        if not _API_DOMAIN_RE.match(domain):
            raise ApiError(
                ErrorCode.AUTH_EXPIRED,
                f"{self.key} has no recognised Pipedrive company domain; "
                f"reconnect the source.",
            )
        return domain

    def _call(self, method: str, url: str, body: Optional[Dict] = None) -> Dict[str, Any]:
        try:
            return self._http(method, url, self.access_token(), body)
        except _AuthError:
            raise ApiError(
                ErrorCode.AUTH_EXPIRED,
                f"{self.key} access was rejected; reconnect the source.",
            )
        except ApiError:
            raise
        except Exception as exc:   # noqa: BLE001
            logger.warning("Pipedrive request failed: %s", exc)
            raise ApiError(
                ErrorCode.UPSTREAM_ERROR,
                "Pipedrive returned an error. Try again.",
            )

    def _pages(self, path: str, params: Optional[Dict[str, Any]] = None,
               limit: int = _PAGE) -> Iterator[Dict[str, Any]]:
        """Every item a list endpoint returns, following its pagination.

        v2 pages with an opaque `next_cursor`; v1 with a `start` offset and a
        `more_items_in_collection` flag.
        """
        url = f"{self._api_base()}{path}"
        query: Dict[str, Any] = {**(params or {}), "limit": limit}
        while True:
            data = self._call("GET", url, query)
            batch = data.get("data") or []
            yield from batch
            extra = data.get("additional_data") or {}
            if path.startswith("/api/v1/"):
                paging = extra.get("pagination") or extra
                if not batch or not paging.get("more_items_in_collection"):
                    return
                query["start"] = int(query.get("start", 0)) + len(batch)
            else:
                cursor = extra.get("next_cursor")
                if not batch or not cursor:
                    return
                query["cursor"] = cursor

    # -- discovery ----------------------------------------------------------

    def list_accounts(self) -> List[Account]:
        data = self._call("GET", f"{self._api_base()}/api/v1/users/me")
        me = data.get("data") or {}
        company_id = me.get("company_id")
        if company_id is None:
            return []
        extra = {k: me[k] for k in ("company_domain", "default_currency") if me.get(k)}
        return [Account(
            id=str(company_id),
            name=me.get("company_name") or f"Pipedrive company {company_id}",
            extra=extra,
        )]

    def list_fields(self, report_type: Optional[str] = None) -> List[Field]:
        return [c.field for c in self._catalogue(self._report_type(report_type)).values()]

    @staticmethod
    def _report_type(report_type: Optional[str]) -> str:
        return report_type if report_type in _REPORTS else _DEFAULT_REPORT

    def _catalogue(self, report_type: str) -> Dict[str, _Col]:
        """The column catalogue for a report: curated fields plus custom ones.

        If custom-field discovery fails for any reason the curated set is used,
        so a metadata hiccup never takes the connector down.
        """
        cached = self._catalogue_cache.get(report_type)
        if cached is not None:
            return cached
        catalogue = _curated(report_type)
        path = _REPORTS[report_type].fields_path
        if path:
            try:
                for meta in self._pages(path):
                    code = meta.get("field_code")
                    if meta.get("is_custom_field") and code and code not in catalogue:
                        catalogue[code] = _custom_col(meta)
            except Exception as exc:   # noqa: BLE001
                logger.warning(
                    "Pipedrive field discovery failed for %s (%s); using the "
                    "curated catalogue.", report_type, exc)
                catalogue = _curated(report_type)
        self._catalogue_cache[report_type] = catalogue
        return catalogue

    # -- query --------------------------------------------------------------

    def _require_scope(self, report_type: str, write: bool = False) -> None:
        """Fail clearly, before any call, if the app was not granted the scope.

        Reading needs `<scope>:read` or `:full`; writing needs `:full`.
        """
        scope = _REPORTS[report_type].scope
        needed = f"{scope}:full" if write else f"{scope}:read"
        granted = self.granted_scopes()
        if granted and not granted & ({needed} if write else {needed, f"{scope}:full"}):
            what = f"Writing {report_type} records" if write else f"The {report_type} report"
            raise ApiError(
                ErrorCode.AUTH_EXPIRED,
                f"{what} needs Pipedrive's '{needed}' permission, which this "
                f"connection was not granted. The Pipedrive app must request it "
                f"(Developer Hub → OAuth & access scopes); then reconnect the "
                f"source.",
                retriable=False,
            )

    @staticmethod
    def _date_field(report_type: str, settings: Dict[str, Any]) -> str:
        allowed = _REPORTS[report_type].date_fields
        requested = str(settings.get("date_field") or "").strip()
        if not requested:
            return _DEFAULT_DATE_FIELD
        if requested not in allowed:
            raise ApiError(
                ErrorCode.INVALID_SETTING,
                f"The {report_type} report cannot filter on {requested!r}. "
                f"date_field must be one of: {', '.join(allowed)}.",
                retriable=False,
                details={"setting_id": "date_field", "accepted": list(allowed)},
            )
        return requested

    def _list_params(self, report: _Report, strategy: str, date_field: str,
                     start: date, cols: List[_Col]) -> Dict[str, Any]:
        params: Dict[str, Any] = {}
        if strategy == _SORT and report.is_v1:
            params["sort"] = f"{date_field} DESC"
        elif strategy == _SORT:
            params.update(sort_by=date_field, sort_direction="desc")
        elif strategy == _SINCE:
            params["updated_since"] = f"{start.isoformat()}T00:00:00Z"
        if report.fields_path:
            # Option values arrive as {id, label} rather than a bare option id.
            params["include_option_labels"] = "true"
            custom = [c.field.id for c in cols if c.custom]
            if 0 < len(custom) <= _MAX_CUSTOM:
                params["custom_fields"] = ",".join(custom)
        return params

    def _run(self, spec: QuerySpec) -> QueryResult:
        report_type = self._report_type(spec.report_type)
        report = _REPORTS[report_type]
        self._require_scope(report_type)
        date_field = self._date_field(report_type, spec.settings)
        start, end = _date_range(spec.date_range.start, spec.date_range.end)
        catalogue = self._catalogue(report_type)

        unknown = [f for f in spec.fields if f not in catalogue]
        if unknown:
            raise invalid_field(unknown[0], list(catalogue.keys()))
        requested = (list(dict.fromkeys(spec.fields))
                     or [f for f, c in catalogue.items() if c.default])
        cols = [catalogue[f] for f in requested]

        strategy = report.date_fields[date_field]
        params = self._list_params(report, strategy, date_field, start, cols)

        matched: List[Tuple[date, Dict[str, Any]]] = []
        scanned = 0
        truncated = capped = False
        for record in self._pages(report.path, params):
            scanned += 1
            when = _date_of(record.get(date_field))
            if when is not None and start <= when <= end:
                matched.append((when, record))
                if len(matched) >= spec.max_rows:
                    truncated = True
                    break
            elif strategy == _SORT and when is not None and when < start:
                break   # read newest first: nothing further on is in range
            if scanned >= _MAX_SCAN:
                capped = True
                break
        if strategy != _SORT:
            matched.sort(key=lambda pair: pair[0], reverse=True)   # newest first
        records = [record for _, record in matched]

        names = self._names(cols, records)
        rows = [{c.field.id: _cell(c, record, names) for c in cols}
                for record in records]

        warnings: List[str] = []
        if truncated:
            warnings.append(
                f"Stopped at max_rows ({spec.max_rows}); more records may match. "
                f"Narrow the date range or raise max_rows before counting or "
                f"totalling.")
        if capped:
            warnings.append(
                f"Stopped after reading {_MAX_SCAN} records; more in the date "
                f"range may exist. Narrow the date range before counting or "
                f"totalling.")
        mixed = _mixed_currencies(report, cols, records)
        if mixed:
            warnings.append(
                f"These rows are in {len(mixed)} currencies "
                f"({', '.join(mixed)}); do not total monetary fields across "
                f"them — group by currency.")

        return QueryResult(
            requested_field_ids=requested,
            rows=rows,
            row_count=len(rows),
            notes=[f"Records are filtered on '{date_field}' within the selected "
                   f"date range{'' if date_field in _DATE_ONLY else ' (UTC)'}; "
                   f"set the 'date_field' setting to use a different date."],
            warnings=warnings,
        )

    # -- name resolution ----------------------------------------------------

    def _names(self, cols: List[_Col], records: List[Dict[str, Any]]) -> Dict[str, Dict[str, str]]:
        """`{ref kind: {id: name}}` for the reference columns requested.

        Best-effort per kind: if a lookup fails (e.g. a scope was not granted)
        its map is empty and the column falls back to the raw id rather than
        failing the whole query over a secondary enrichment.
        """
        wanted: Dict[str, Set[str]] = {}
        for col in cols:
            if not col.ref:
                continue
            ids = wanted.setdefault(col.ref, set())
            for record in records:
                raw = _raw(col, record)
                if isinstance(raw, (int, str)) and str(raw).isdigit():
                    ids.add(str(raw))
        names: Dict[str, Dict[str, str]] = {}
        for ref, ids in wanted.items():
            if not ids:
                names[ref] = {}
                continue
            try:
                names[ref] = self._lookup(ref, ids)
            except ApiError as exc:
                logger.warning("Pipedrive %s lookup failed: %s", ref, exc.message)
                names[ref] = {}
        return names

    def _lookup(self, ref: str, ids: Set[str]) -> Dict[str, str]:
        return {str(item["id"]): _item_name(ref, item)
                for item in self._ref_items(ref, ids) if item.get("id") is not None}

    def _ref_items(self, kind: str, ids: Set[str]) -> List[Dict[str, Any]]:
        """The records `ids` refer to. Users, stages and pipelines are few per
        company, so all are read; persons, organizations and deals by id."""
        if kind == "user":
            data = self._call("GET", f"{self._api_base()}/api/v1/users")
            return [u for u in data.get("data") or [] if isinstance(u, dict)]
        path = _REF_PATHS[kind]
        if kind in ("stage", "pipeline"):
            return list(self._pages(path))
        ordered = sorted(ids, key=int)
        items: List[Dict[str, Any]] = []
        for i in range(0, len(ordered), _IDS_PER_CALL):
            chunk = ordered[i:i + _IDS_PER_CALL]
            items.extend(self._pages(path, {"ids": ",".join(chunk)},
                                     limit=_IDS_PER_CALL))
        return items

    # -- write actions ------------------------------------------------------

    def list_actions(self) -> List[Action]:
        return list(_ACTIONS)

    def execute_action(
        self, action_id: str, account: str,
        params: Optional[Dict[str, Any]] = None, dry_run: bool = False,
    ) -> ActionResult:
        """Perform one write action. Account authorisation happens upstream.

        The token is scoped to one company, so `account` is recorded, not routed
        on. Every parameter is checked before any write: the record type, each
        field against what that type accepts, each value's shape, and that every
        user, stage, person, … it links to exists. An update reads the record
        first, so `before` shows exactly what changes.
        """
        action = _ACTIONS_BY_ID.get(action_id)
        if action is None:
            raise ApiError(
                ErrorCode.UNKNOWN_ACTION,
                f"Unknown action {action_id!r}. Available: "
                f"{', '.join(_ACTIONS_BY_ID)}.",
                retriable=False,
            )
        params = params or {}
        extra = sorted(set(params) - set(action.schema["properties"]))
        if extra:
            raise ApiError(
                ErrorCode.INVALID_ACTION_PARAMS,
                f"{action_id} does not accept: {', '.join(extra)}.",
                retriable=False, details={"unexpected": extra},
            )
        record_type = params.get("record_type")
        if record_type not in _REPORTS:
            raise ApiError(
                ErrorCode.INVALID_ACTION_PARAMS,
                f"'record_type' must be one of: {', '.join(_REPORTS)}.",
                retriable=False, details={"param": "record_type"},
            )
        self._require_scope(record_type, write=True)
        record_id = (_record_id(record_type, params)
                     if action_id == "update_record" else None)
        change = self._change(record_type, params.get("fields"),
                              creating=record_id is None)
        if record_id is None:
            return self._create_record(record_type, account, change, dry_run)
        return self._update_record(record_type, account, record_id, change, dry_run)

    def _change(self, record_type: str, fields, creating: bool) -> _Change:
        """`fields`, once every name is writable and every value well-formed."""
        if not isinstance(fields, dict) or not fields:
            raise ApiError(
                ErrorCode.INVALID_ACTION_PARAMS,
                "'fields' must be a non-empty object of field names to values.",
                retriable=False, details={"param": "fields"},
            )
        writable = _WRITABLE[record_type]
        required = _REQUIRED[record_type]
        # Custom fields and read-only hints need the catalogue; plain standard
        # fields do not, so a common write costs no metadata call.
        catalogue = (self._catalogue(record_type)
                     if any(name not in writable for name in fields) else {})
        standard: Dict[str, Any] = {}
        custom: Dict[str, Any] = {}
        refs: List[Tuple[str, str, int]] = []
        for name, value in fields.items():
            kind = writable.get(name)
            if kind is not None:
                if value is None and name in required:
                    raise _bad_value(name, "set (it cannot be cleared)", value)
                standard[name] = _write_value(name, kind, value)
                if kind in _REF_KINDS and standard[name] is not None:
                    refs.append((name, kind, standard[name]))
                continue
            col = catalogue.get(name)
            if col is None or not col.custom:
                raise self._not_writable(record_type, name, col, catalogue)
            custom[name] = _custom_write_value(col, value)
            if col.ref and custom[name] is not None:
                refs.append((name, col.ref, custom[name]))
        if creating:
            missing = [f for f in required if standard.get(f) is None]
            if missing:
                raise ApiError(
                    ErrorCode.INVALID_ACTION_PARAMS,
                    f"A new {record_type} record needs: {', '.join(missing)}.",
                    retriable=False, details={"missing": missing},
                )
            if (record_type == "Leads" and standard.get("person_id") is None
                    and standard.get("organization_id") is None):
                raise ApiError(
                    ErrorCode.INVALID_ACTION_PARAMS,
                    "A new lead must be linked to a person_id or an "
                    "organization_id.",
                    retriable=False, details={"missing": ["person_id"]},
                )
        return _Change(dict(fields), standard, custom, tuple(refs))

    @staticmethod
    def _not_writable(record_type: str, name: str, col: Optional[_Col],
                      catalogue: Dict[str, _Col]) -> ApiError:
        writable = _WRITABLE[record_type]
        if name in _NEVER_WRITTEN:
            return ApiError(
                ErrorCode.INVALID_ACTION_PARAMS,
                f"{name!r} cannot be set: records are never deleted or archived "
                f"through this connector.",
                retriable=False, details={"field": name},
            )
        if col is None:
            known = list(writable) + [f for f, c in catalogue.items() if c.custom]
            return invalid_field(name, known)
        # A read column backed by a writable field (owner -> owner_id, email ->
        # emails) says which one to set.
        hint = (f" Set {col.source!r} instead."
                if col.source != name and col.source in writable else "")
        return ApiError(
            ErrorCode.INVALID_ACTION_PARAMS,
            f"{name!r} cannot be set on a {record_type} record.{hint} Writable: "
            f"{', '.join(writable)}"
            f"{', and custom fields' if _REPORTS[record_type].fields_path else ''}.",
            retriable=False, details={"field": name},
        )

    def _check_refs(self, change: _Change) -> List[str]:
        """Refuse a write that links to a user, stage, person, … that does not exist.

        Returns the fields that could not be checked because the lookup itself
        failed (e.g. `users:read` was not granted). Those are left to Pipedrive
        rather than blocking a write it may well accept.
        """
        wanted: Dict[str, Dict[str, str]] = {}   # kind -> {id: field}
        for field, kind, rid in change.refs:
            wanted.setdefault(kind, {})[str(rid)] = field
        unverified: List[str] = []
        known: Dict[str, Dict[str, Dict[str, Any]]] = {}
        for kind, ids in wanted.items():
            try:
                items = self._ref_items(kind, set(ids))
            except ApiError as exc:
                logger.warning("Pipedrive %s check skipped: %s", kind, exc.message)
                unverified.extend(ids.values())
                continue
            known[kind] = {str(i["id"]): i for i in items if i.get("id") is not None}
            for rid, field in ids.items():
                if rid not in known[kind]:
                    raise _missing_ref(kind, field, rid, items)
        stage = known.get("stage", {}).get(str(change.standard.get("stage_id")))
        pipeline_id = change.standard.get("pipeline_id")
        if stage and pipeline_id is not None and stage.get("pipeline_id") != pipeline_id:
            raise ApiError(
                ErrorCode.INVALID_ACTION_PARAMS,
                f"Stage {stage['id']} ({_item_name('stage', stage)}) belongs to "
                f"pipeline {stage.get('pipeline_id')}, not {pipeline_id}. Set "
                f"stage_id alone to move the deal to that stage's pipeline.",
                retriable=False, details={"field": "stage_id"},
            )
        return sorted(set(unverified))

    def _read_record(self, record_type: str, record_id: str) -> Dict[str, Any]:
        """The record an update targets; refused if missing or deleted."""
        report = _REPORTS[record_type]
        params = {"include_option_labels": "true"} if report.fields_path else None
        try:
            data = self._call(
                "GET", f"{self._api_base()}{report.path}/{record_id}", params)
        except ApiError as exc:
            if exc.details.get("status") != 404:
                raise
            data = {}
        record = data.get("data")
        if not isinstance(record, dict) or not record:
            raise ApiError(
                ErrorCode.INVALID_ACTION_PARAMS,
                f"No {record_type} record with id {record_id} was found.",
                retriable=False, details={"record_id": record_id},
            )
        if record.get("is_deleted"):
            raise ApiError(
                ErrorCode.INVALID_ACTION_PARAMS,
                f"{record_type} record {record_id} has been deleted in Pipedrive "
                f"and cannot be updated.",
                retriable=False, details={"record_id": record_id},
            )
        return record

    def _write(self, method: str, url: str, body: Dict[str, Any]) -> Dict[str, Any]:
        """Send a write. Pipedrive's reason for refusing one is the actionable part."""
        try:
            data = self._call(method, url, body)
        except ApiError as exc:
            if exc.details.get("status") in (400, 404, 422):
                raise ApiError(
                    ErrorCode.INVALID_ACTION_PARAMS,
                    f"Pipedrive rejected the change: "
                    f"{exc.details.get('error') or exc.message}",
                    retriable=False, details={"status": exc.details["status"]},
                )
            raise
        record = data.get("data")
        if not isinstance(record, dict):
            raise ApiError(ErrorCode.UPSTREAM_ERROR,
                           "Pipedrive returned no record for the change.")
        return record

    @staticmethod
    def _body(record_type: str, change: _Change,
              existing: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        body = dict(change.standard)
        if record_type == "Activities" and "person_id" in body:
            body["participants"] = _participants(body.pop("person_id"), existing)
        if change.custom:
            body["custom_fields"] = dict(change.custom)
        return body

    def _create_record(self, record_type: str, account: str, change: _Change,
                       dry_run: bool) -> ActionResult:
        unverified = self._check_refs(change)
        names = ", ".join(change.given)
        if dry_run:
            return ActionResult(
                action="create_record", account=account,
                summary=f"[dry-run — not applied] Would create a {record_type} "
                        f"record with: {names}.",
                after=dict(change.given),
                details=_dry_run_details(
                    "field names, value formats and linked records", unverified),
            )
        record = self._write(
            "POST", f"{self._api_base()}{_REPORTS[record_type].path}",
            self._body(record_type, change))
        record_id = str(record.get("id") or "")
        return ActionResult(
            action="create_record", account=account,
            summary=f"Created {record_type} record {record_id} with: {names}.",
            after={"id": record_id, **change.given},
            details={"record_type": record_type, "record_id": record_id},
        )

    def _update_record(self, record_type: str, account: str, record_id: str,
                       change: _Change, dry_run: bool) -> ActionResult:
        record = self._read_record(record_type, record_id)
        unverified = self._check_refs(change)
        before = {"id": record_id, **{
            f: _flatten((record.get("custom_fields") or {}).get(f))
            if f in change.custom else record.get(f)
            for f in change.given
        }}
        after = {"id": record_id, **change.given}
        names = ", ".join(change.given)
        if dry_run:
            return ActionResult(
                action="update_record", account=account,
                summary=f"[dry-run — not applied] Would update {record_type} "
                        f"record {record_id}: {names}.",
                before=before, after=after,
                details=_dry_run_details(
                    "field names, value formats, linked records and that the "
                    "record exists", unverified),
            )
        self._write(
            "PATCH", f"{self._api_base()}{_REPORTS[record_type].path}/{record_id}",
            self._body(record_type, change, existing=record))
        return ActionResult(
            action="update_record", account=account,
            summary=f"Updated {record_type} record {record_id}: {names}.",
            before=before, after=after,
            details={"record_type": record_type, "record_id": record_id},
        )


def _record_id(record_type: str, params: Dict[str, Any]) -> str:
    """The update target, checked strictly since it is placed in the request path."""
    raw = str(params.get("record_id") or "").strip()
    lead = record_type == "Leads"
    if not (_UUID_RE if lead else _INT_ID_RE).match(raw):
        raise ApiError(
            ErrorCode.INVALID_ACTION_PARAMS,
            f"'record_id' must be {'a lead UUID' if lead else 'a numeric Pipedrive id'}, "
            f"got {raw!r}. Use the `id` field from data_query.",
            retriable=False, details={"param": "record_id"},
        )
    return raw.lower() if lead else raw   # Pipedrive writes UUIDs in lower case


def _raw(col: _Col, record: Dict[str, Any]):
    """A column's value as the record holds it, before flattening."""
    holder = (record.get("custom_fields") or {}) if col.custom else record
    raw = holder.get(col.source)
    if col.sub:
        raw = raw.get(col.sub) if isinstance(raw, dict) else None
    return raw


def _cell(col: _Col, record: Dict[str, Any], names: Dict[str, Dict[str, str]]):
    """One cell: a reference resolved to its name, a structured value flattened."""
    raw = _raw(col, record)
    if raw is None:
        return None
    if col.ref:
        return names.get(col.ref, {}).get(str(raw), raw)
    return _coerce(col.field, _flatten(raw))


def _flatten(raw):
    """A structured Pipedrive value as one cell.

    Options arrive as `{id, label}` (the label is kept); a multi-option as a list
    of them (labels joined); money, addresses and times as `{value, …}` (the
    value is kept, a range as `value/until`); emails and phones as a list of
    `{value, primary}` (the primary one is kept).
    """
    if isinstance(raw, dict):
        if "label" in raw:
            return raw.get("label")
        if raw.get("until") is not None:
            return f"{raw.get('value')}/{raw['until']}"
        return raw.get("value")
    if isinstance(raw, list):
        if raw and all(isinstance(r, dict) and "primary" in r for r in raw):
            primary = next((r for r in raw if r.get("primary")), raw[0])
            return primary.get("value")
        parts = [_flatten(r) for r in raw]
        return ", ".join(str(p) for p in parts if p is not None) or None
    return raw


def _coerce(field: Field, raw):
    """Coerce numeric strings to numbers for metrics; leave text as-is."""
    if raw is None or field.kind != "metric":
        return raw
    try:
        f = float(raw)
    except (TypeError, ValueError):
        return raw
    if field.is_monetary:
        return f
    return int(f) if f.is_integer() else f


def _mixed_currencies(report: _Report, cols: List[_Col],
                      records: List[Dict[str, Any]]) -> List[str]:
    """The currencies of the rows, when a monetary column spans more than one."""
    if not report.currency or not any(c.field.is_monetary and not c.custom for c in cols):
        return []
    found: Set[str] = set()
    for record in records:
        value: Any = record
        for key in report.currency:
            value = value.get(key) if isinstance(value, dict) else None
        if value:
            found.add(str(value))
    return sorted(found) if len(found) > 1 else []


def make_pipedrive_connector(datasource) -> PipedriveConnector:
    """Build a Pipedrive connector wired to refresh its own OAuth token when due."""
    from terno_dbi.connectors.api.auth.oauth import make_ensure_token
    return PipedriveConnector(
        datasource, token_refresher=make_ensure_token(datasource))


__all__ = ["PipedriveConnector", "make_pipedrive_connector"]
