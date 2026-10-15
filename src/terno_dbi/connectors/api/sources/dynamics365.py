"""Microsoft Dynamics 365 connector.

Implements the `ApiConnector` interface against the Dataverse Web API (v9.2),
which serves every Dynamics 365 customer-engagement app — Sales, Customer
Service, Marketing — and any Power App built on the same environment:

- `list_accounts()` — the one environment the connection was made to, from its
  `organizations` row, with the ISO code of its base currency.
- `list_fields()`   — discovered live per table from the column metadata
  (`EntityDefinitions(LogicalName=…)/Attributes`), so custom columns and the
  labels an org gave its columns are queryable. A curated list per report
  describes the core columns, picks the default ones, and is the fallback if
  discovery fails.
- `_run()`          — `GET /{entity set}` with `$select`, a `$filter` on the
  report's date column and `$orderby` newest first, following
  `@odata.nextLink`. Choices and lookups arrive as their labels and names
  through Dataverse's formatted-value annotations, so no second lookup is made.
- `list_actions()` / `execute_action()` — `create_record` (`POST`) and
  `update_record` (`PATCH` with `If-Match: *`, after reading the record's
  current values). There is deliberately no delete.

Dynamics 365 differs from the other connectors:
  * **Every environment is its own OAuth resource.** The user names it at
    connect (e.g. `contoso.crm4.dynamics.com`); the token is issued for that
    host alone, which is saved as `INSTANCE` and is where every call goes.
  * There is no read-only scope. The token acts as the signed-in user, so what a
    query sees — and what a write may change — is exactly what their Dynamics
    365 security roles allow. A missing privilege is a 403 that names it.
  * A lookup column holds a GUID, read as `_<column>_value`. Here the column's
    own id (`ownerid`) reads the linked record's name and `_ownerid_value` its
    GUID. A write binds a lookup through its navigation property
    (`parentcustomerid_account@odata.bind`), whose name only the relationship
    metadata knows.
  * `PATCH` to an id that does not exist creates the record (an upsert), so
    every update sends `If-Match: *`.
  * A date-only column (an opportunity's `actualclosedate`, a contact's
    `birthdate`) is an `Edm.Date` and is filtered with date literals; every
    other date column is a UTC `DateTimeOffset`.
"""

from __future__ import annotations
import logging
import re
from dataclasses import dataclass, field as dc_field
from datetime import date, datetime, timedelta, timezone as _tz
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from urllib.parse import quote

from terno_dbi.connectors.api.model.base import ApiConnector
from terno_dbi.connectors.api.model.errors import ApiError, ErrorCode, invalid_field
from terno_dbi.connectors.api.model.types import (
    Account, Action, ActionResult, Field, QueryResult, QuerySpec,
)

logger = logging.getLogger(__name__)

_API_PATH = "/api/data/v9.2"
_PAGE = 5000        # rows per page; Dataverse's cap for odata.maxpagesize
_MAX_PAGES = 20     # follow-on pages per query, whatever max_rows asks for
_DEFAULT_DATE_FIELD = "createdon"   # every Dataverse table has it
_FORMATTED = "OData.Community.Display.V1.FormattedValue"
_NAV_PROPERTY = "Microsoft.Dynamics.CRM.associatednavigationproperty"
_LOOKUP_TABLE = "Microsoft.Dynamics.CRM.lookuplogicalname"

# The environment host saved at connect. Checked again before any call so a
# token is only ever sent to Dataverse.
_ENV_HOST_RE = re.compile(
    r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.crm\d{0,2}\.dynamics\.com$")
# Table and column logical names are lower case; entity set names (which appear
# in URL paths) keep the schema's case.
_LOGICAL_NAME_RE = re.compile(r"^[a-z][a-z0-9_]{0,127}$")
_ENTITY_SET_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,127}$")
_GUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.IGNORECASE)


@dataclass(frozen=True)
class _Report:
    table: str        # the table's logical name
    entity_set: str   # its Web API collection, if metadata cannot be read
    primary_id: str


_REPORTS: Dict[str, _Report] = {
    "Accounts": _Report("account", "accounts", "accountid"),
    "Contacts": _Report("contact", "contacts", "contactid"),
    "Leads": _Report("lead", "leads", "leadid"),
    "Opportunities": _Report("opportunity", "opportunities", "opportunityid"),
    "Cases": _Report("incident", "incidents", "incidentid"),
    "Tasks": _Report("task", "tasks", "activityid"),
    "PhoneCalls": _Report("phonecall", "phonecalls", "activityid"),
    "Appointments": _Report("appointment", "appointments", "activityid"),
}
_REPORT_BY_TABLE: Dict[str, str] = {r.table: rt for rt, r in _REPORTS.items()}
_CUSTOM_REPORT = "Custom"
_DEFAULT_REPORT = "Opportunities"


@dataclass(frozen=True)
class _Table:
    """One Dataverse table, as its metadata describes it."""

    logical_name: str
    entity_set: str
    primary_id: str
    primary_name: str = ""
    is_custom: bool = False
    label: str = ""

    @property
    def title(self) -> str:
        return self.label or self.logical_name


# --- columns -----------------------------------------------------------------

_LOOKUP_TYPES = frozenset({"Lookup", "Customer", "Owner"})
_CHOICE_TYPES = frozenset({"Picklist", "State", "Status"})
_MULTI_CHOICE = "MultiSelectPicklistType"   # an AttributeTypeName, under "Virtual"
_INTEGER_TYPES = frozenset({"Integer", "BigInt"})
_NUMBER_TYPES = frozenset({"Decimal", "Double"})
# Column types the Web API cannot return as a value: activity parties, calendar
# rules, managed properties, and images and files (the other "Virtual" columns).
_UNREADABLE_TYPES = frozenset({"PartyList", "CalendarRules", "ManagedProperty", "Virtual"})
# Bookkeeping columns Dataverse keeps on every table (numbers, but not metrics),
# and `attachmentcount`, which an appointment's metadata lists but its Web API
# entity type does not have.
_NOISE = frozenset({
    "versionnumber", "importsequencenumber", "timezoneruleversionnumber",
    "utcconversiontimezonecode", "attachmentcount",
})
# Dataverse's answer when `$select` names a column its entity type lacks.
_NO_PROPERTY_RE = re.compile(r"Could not find a property named '([^']+)'")
_NON_AGGREGATABLE = frozenset({"closeprobability", "exchangerate"})
# Out-of-box date-only columns, for when the date-behaviour metadata cannot be
# read. Every other date column is a date-time.
_KNOWN_DATE_ONLY = frozenset({
    "anniversary", "birthdate", "duedate", "estimatedclosedate", "actualclosedate",
    "finaldecisiondate", "validfromdate", "validtodate", "closedon", "expireson",
})


@dataclass(frozen=True)
class _Col:
    """A catalogue column and how it is read and written."""

    field: Field
    attr: str              # the column's logical name, which a write sets
    attr_type: str         # Dataverse's AttributeType: String, Lookup, Money, …
    prop: str              # the Web API property selected and read
    formatted: bool = False   # read the formatted value: a choice's label, a lookup's name
    multi: bool = False       # a multi-select choice
    date_only: bool = False
    creatable: bool = False
    updatable: bool = False
    default: bool = False     # selected when a query names no fields

    @property
    def is_lookup(self) -> bool:
        return self.attr_type in _LOOKUP_TYPES

    @property
    def is_choice(self) -> bool:
        return self.attr_type in _CHOICE_TYPES or self.multi

    @property
    def is_date(self) -> bool:
        return self.attr_type == "DateTime"


@dataclass(frozen=True)
class _Known:
    """A curated column: its label, type and meaning, and whether it is a default."""

    name: str
    label: str
    attr_type: str
    description: str = ""
    default: bool = True

    def as_meta(self) -> Dict[str, Any]:
        """The column as Dataverse's attribute metadata would describe it."""
        return {"LogicalName": self.name, "AttributeType": self.attr_type,
                "DisplayName": {"UserLocalizedLabel": {"Label": self.label}}}


def _k(name, label, attr_type, description="", default=True) -> _Known:
    return _Known(name, label, attr_type, description, default)


_OWNER = _k("ownerid", "Owner", "Owner", "The user or team that owns the record.")
_CREATED = _k("createdon", "Created on", "DateTime", "When the record was created.")
_MODIFIED = _k("modifiedon", "Modified on", "DateTime",
               "When the record last changed.", default=False)
_STATUS_REASON = _k("statuscode", "Status reason", "Status",
                    "The reason within the status, e.g. In Progress or On Hold.")
_CURRENCY = _k("transactioncurrencyid", "Currency", "Lookup",
               "The currency the record's money columns are in.", default=False)


def _active(description="Active or Inactive.") -> _Known:
    return _k("statecode", "Status", "State", description)


def _regarding() -> _Known:
    return _k("regardingobjectid", "Regarding", "Lookup",
              "The record the activity is about — an account, contact, "
              "opportunity, case, …")


_CURATED: Dict[str, List[_Known]] = {
    "account": [
        _k("accountid", "Account ID", "Uniqueidentifier",
           "Dataverse's id for the account (the record_id for update_record)."),
        _k("name", "Account name", "String"),
        _k("accountnumber", "Account number", "String"),
        _k("industrycode", "Industry", "Picklist"),
        _k("customertypecode", "Relationship type", "Picklist",
           "e.g. Customer, Partner, Prospect."),
        _k("revenue", "Annual revenue", "Money",
           "Reported annual revenue, in the account's currency."),
        _k("numberofemployees", "Employees", "Integer"),
        _k("address1_city", "City", "String"),
        _k("address1_country", "Country", "String"),
        _k("primarycontactid", "Primary contact", "Lookup"),
        _k("parentaccountid", "Parent account", "Lookup", default=False),
        _CURRENCY, _OWNER, _active(), _CREATED, _MODIFIED,
    ],
    "contact": [
        _k("contactid", "Contact ID", "Uniqueidentifier",
           "Dataverse's id for the contact (the record_id for update_record)."),
        _k("fullname", "Name", "String"),
        _k("emailaddress1", "Email", "String"),
        _k("telephone1", "Business phone", "String"),
        _k("jobtitle", "Job title", "String"),
        _k("parentcustomerid", "Company name", "Customer",
           "The account (or contact) this contact belongs to."),
        _k("address1_city", "City", "String"),
        _k("address1_country", "Country", "String"),
        _k("firstname", "First name", "String", default=False),
        _k("lastname", "Last name", "String", default=False),
        _OWNER, _active(), _CREATED, _MODIFIED,
    ],
    "lead": [
        _k("leadid", "Lead ID", "Uniqueidentifier",
           "Dataverse's id for the lead (the record_id for update_record)."),
        _k("fullname", "Name", "String"),
        _k("subject", "Topic", "String"),
        _k("companyname", "Company name", "String"),
        _k("emailaddress1", "Email", "String"),
        _k("leadsourcecode", "Lead source", "Picklist",
           "Where the lead came from, e.g. Advertisement, Web, Trade Show."),
        _k("leadqualitycode", "Rating", "Picklist", "Hot, Warm or Cold."),
        _k("estimatedamount", "Est. value", "Money",
           "Potential value, in the lead's currency."),
        _k("estimatedclosedate", "Est. close date", "DateTime", default=False),
        _k("firstname", "First name", "String", default=False),
        _k("lastname", "Last name", "String", default=False),
        _CURRENCY, _OWNER, _active("Open, Qualified or Disqualified."),
        _STATUS_REASON, _CREATED, _MODIFIED,
    ],
    "opportunity": [
        _k("opportunityid", "Opportunity ID", "Uniqueidentifier",
           "Dataverse's id for the opportunity (the record_id for "
           "update_record)."),
        _k("name", "Topic", "String"),
        _k("customerid", "Potential customer", "Customer",
           "The account or contact the deal is with."),
        _k("estimatedvalue", "Est. revenue", "Money",
           "Estimated revenue, in the opportunity's currency (see Currency)."),
        _k("actualvalue", "Actual revenue", "Money",
           "Revenue when won, in the opportunity's currency."),
        _k("closeprobability", "Probability (%)", "Integer",
           "Likelihood of winning, when set."),
        _k("salesstage", "Sales stage", "Picklist",
           "Qualify, Develop, Propose or Close."),
        _k("stepname", "Pipeline phase", "String",
           "The business process stage the opportunity is in."),
        _k("opportunityratingcode", "Rating", "Picklist", "Hot, Warm or Cold.",
           default=False),
        _k("estimatedclosedate", "Est. close date", "DateTime",
           "When the deal is expected to close."),
        _k("actualclosedate", "Actual close date", "DateTime",
           "When the deal was won or lost."),
        _k("parentaccountid", "Account", "Lookup", default=False),
        _k("parentcontactid", "Contact", "Lookup", default=False),
        _k("transactioncurrencyid", "Currency", "Lookup",
           "The currency the deal's money columns are in."),
        _OWNER, _active("Open, Won or Lost."), _STATUS_REASON, _CREATED, _MODIFIED,
    ],
    "incident": [
        _k("incidentid", "Case ID", "Uniqueidentifier",
           "Dataverse's id for the case (the record_id for update_record)."),
        _k("ticketnumber", "Case number", "String"),
        _k("title", "Case title", "String"),
        _k("customerid", "Customer", "Customer",
           "The account or contact the case is for."),
        _k("casetypecode", "Case type", "Picklist", "Question, Problem or Request."),
        _k("prioritycode", "Priority", "Picklist"),
        _k("caseorigincode", "Origin", "Picklist", "e.g. Phone, Email, Web."),
        _OWNER, _active("Active, Resolved or Cancelled."), _STATUS_REASON,
        _CREATED, _MODIFIED,
    ],
    "task": [
        _k("activityid", "Task ID", "Uniqueidentifier",
           "Dataverse's id for the task (the record_id for update_record)."),
        _k("subject", "Subject", "String"),
        _regarding(),
        _k("scheduledend", "Due", "DateTime"),
        _k("prioritycode", "Priority", "Picklist"),
        _k("actualend", "Completed on", "DateTime", default=False),
        _OWNER, _active("Open, Completed or Canceled."), _CREATED, _MODIFIED,
    ],
    "phonecall": [
        _k("activityid", "Phone call ID", "Uniqueidentifier",
           "Dataverse's id for the call (the record_id for update_record)."),
        _k("subject", "Subject", "String"),
        _k("phonenumber", "Phone number", "String"),
        _k("directioncode", "Direction", "Boolean",
           "True for an outgoing call, false for an incoming one."),
        _regarding(),
        _k("scheduledend", "Due", "DateTime"),
        _k("actualdurationminutes", "Duration (minutes)", "Integer"),
        _OWNER, _active("Open, Completed or Canceled."), _CREATED, _MODIFIED,
    ],
    "appointment": [
        _k("activityid", "Appointment ID", "Uniqueidentifier",
           "Dataverse's id for the appointment (the record_id for "
           "update_record)."),
        _k("subject", "Subject", "String"),
        _k("location", "Location", "String"),
        _k("scheduledstart", "Start", "DateTime"),
        _k("scheduledend", "End", "DateTime"),
        _regarding(),
        _OWNER, _active("Open, Completed, Canceled or Scheduled."),
        _CREATED, _MODIFIED,
    ],
}


def _generic_known(table: _Table) -> List[_Known]:
    """Defaults for a table with no curated list: its id, its name, and when."""
    known = [_k(table.primary_id, f"{table.title} ID", "Uniqueidentifier",
                f"Dataverse's id for the {table.title} record.")]
    if table.primary_name:
        known.append(_k(table.primary_name, "Name", "String"))
    return known + [_CREATED, _MODIFIED]


def _label(meta: Dict[str, Any], key: str) -> str:
    label = (meta.get(key) or {}).get("UserLocalizedLabel") or {}
    return str(label.get("Label") or "")


def _readable(meta: Dict[str, Any]) -> bool:
    """Whether a column from the metadata can be selected through the Web API.

    Helper columns that only extend another (`owneridname`, `createdbyyominame`)
    are left out, except a money column's `_base` twin, which is a real value.
    `IsValidODataAttribute` is not consulted: an activity table reports it false
    for every column it shares with all activities (`subject`, `ownerid`,
    `createdon`), all of which the Web API serves.
    """
    name = meta.get("LogicalName")
    atype = meta.get("AttributeType") or ""
    if not name or meta.get("IsValidForRead") is False or name in _NOISE:
        return False
    if atype == "Virtual":
        return (meta.get("AttributeTypeName") or {}).get("Value") == _MULTI_CHOICE
    if atype in _UNREADABLE_TYPES:
        return False
    return not meta.get("AttributeOf") or atype == "Money"


def _columns(meta: Dict[str, Any], *, date_only: bool,
             known: Optional[_Known]) -> List[_Col]:
    """The catalogue column(s) for one entry of Dataverse's column metadata.

    Numeric columns become metrics; everything else is a dimension. A lookup
    becomes two columns: the linked record's name and its GUID.
    """
    name = meta["LogicalName"]
    atype = meta.get("AttributeType") or ""
    label = _label(meta, "DisplayName") or (known.label if known else name)
    description = (known.description if known and known.description
                   else _label(meta, "Description"))
    group = "Custom columns" if meta.get("IsCustomAttribute") else None
    multi = (meta.get("AttributeTypeName") or {}).get("Value") == _MULTI_CHOICE
    base = dict(attr=name, attr_type=atype,
                creatable=bool(meta.get("IsValidForCreate")),
                updatable=bool(meta.get("IsValidForUpdate")),
                default=bool(known and known.default))

    if atype in _LOOKUP_TYPES:
        prop = f"_{name}_value"
        return [
            _Col(Field(name, label, "dimension",
                       description or f"Name of the linked {label} record.",
                       group=group),
                 prop=prop, formatted=True, **base),
            _Col(Field(prop, f"{label} (ID)", "dimension",
                       f"GUID of the linked {label} record.", group=group),
                 attr=name, attr_type=atype, prop=prop),
        ]
    if atype == "Money":
        if name.endswith("_base"):
            description = (f"{label}, converted to the organisation's base "
                           f"currency — safe to total across records.")
        field = Field(name, label, "metric", description, data_type="number",
                      group=group, is_monetary=True)
    elif atype in _INTEGER_TYPES or atype in _NUMBER_TYPES:
        field = Field(name, label, "metric", description, group=group,
                      data_type="integer" if atype in _INTEGER_TYPES else "number",
                      is_non_aggregatable=name in _NON_AGGREGATABLE)
    elif atype == "DateTime":
        field = Field(name, label, "dimension", description, group=group,
                      data_type="date" if date_only else "datetime")
    elif atype == "Boolean":
        field = Field(name, label, "dimension", description, data_type="boolean",
                      group=group)
    else:
        field = Field(name, label, "dimension", description, group=group)
    formatted = atype in _CHOICE_TYPES or multi
    return [_Col(field, prop=name, formatted=formatted, multi=multi,
                 date_only=date_only and atype == "DateTime", **base)]


@dataclass
class _Catalogue:
    cols: Dict[str, _Col]
    discovered: bool   # False when built from the curated fallback
    options: Dict[str, List[Tuple[int, str]]] = dc_field(default_factory=dict)

    @property
    def defaults(self) -> List[str]:
        return [fid for fid, col in self.cols.items() if col.default]


# --- write actions ----------------------------------------------------------

_TABLE_PROP = {
    "type": "string",
    "description": "The table the record belongs to: a report type (Accounts, "
                   "Contacts, Leads, Opportunities, Cases, Tasks, PhoneCalls, "
                   "Appointments) or a table's logical name — account, contact, "
                   "lead, opportunity, incident, task, phonecall, appointment, "
                   "or a custom table such as new_project.",
}
_FIELDS_PROP = {
    "type": "object", "minProperties": 1,
    "description": "Column logical names (field ids from list_fields) mapped to "
                   "values. A lookup takes the linked record's GUID, or "
                   "{\"table\": \"<logical name>\", \"id\": \"<GUID>\"} when it "
                   "can link to several tables (customerid, parentcustomerid, "
                   "ownerid, regardingobjectid); a choice takes its label or "
                   "numeric value, a multi-select choice a list of them; dates "
                   "'YYYY-MM-DD'; date-times ISO 8601 with 'Z' or an offset; "
                   "money and numbers plain numbers. null clears a column.",
}

_ACTIONS: List[Action] = [
    Action(
        "create_record", "Create a record",
        "Create one Dynamics 365 record — an account, contact, lead, "
        "opportunity, case, task, phone call, appointment, or a row of a custom "
        "table. Which columns are required depends on the table and the org's "
        "customisations (e.g. name for an account, lastname for a contact); "
        "Dynamics 365 names any that are missing.",
        schema={"type": "object",
                "properties": {"table": _TABLE_PROP, "fields": _FIELDS_PROP},
                "required": ["table", "fields"], "additionalProperties": False},
        destructive=False,
    ),
    Action(
        "update_record", "Update a record",
        "Change column values on one existing record — e.g. move an "
        "opportunity to another sales stage, change its estimated revenue, or "
        "reassign its owner. Only the columns given are changed. Records are "
        "never deleted.",
        schema={"type": "object",
                "properties": {
                    "table": _TABLE_PROP,
                    "record_id": {"type": "string",
                                  "description": "The record's GUID (its id "
                                                 "column from data_query, e.g. "
                                                 "opportunityid)."},
                    "fields": _FIELDS_PROP,
                },
                "required": ["table", "record_id", "fields"],
                "additionalProperties": False},
    ),
]
_ACTIONS_BY_ID: Dict[str, Action] = {a.id: a for a in _ACTIONS}

# The metadata type a choice column's options are read through.
_OPTION_CASTS = {"Picklist": "PicklistAttributeMetadata",
                 "State": "StateAttributeMetadata",
                 "Status": "StatusAttributeMetadata"}


@dataclass(frozen=True)
class _LookupRef:
    """A lookup value as the caller gave it: the linked record and its table."""

    id: str
    table: Optional[str] = None


# Dataverse has no validate-only mode for writes, so a dry run checks what can be
# checked here and says plainly what it did not.
def _dry_run_details(checked: str, unverified: List[str]) -> Dict[str, Any]:
    details: Dict[str, Any] = {
        "dry_run": True, "applied": False,
        "validated": f"{checked}; Dynamics 365 applies its own rules (required "
                     f"columns, business rules, plug-ins, security roles) only "
                     f"when the change is applied",
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


def _guid(name: str, value) -> str:
    if isinstance(value, str) and _GUID_RE.match(value.strip()):
        return value.strip().lower()
    raise _bad_value(name, "a GUID", value)


def _write_value(col: _Col, value):
    """`value` checked against the column's type, in the shape Dataverse takes.

    Lookups and choices are only shape-checked here; resolving them (the bind
    target, a label's value) needs metadata and happens in `_payload`.
    """
    name, atype = col.attr, col.attr_type
    if value is None:
        return None
    if col.is_lookup:
        if isinstance(value, dict):
            target = value.get("table")
            if (set(value) != {"table", "id"} or not isinstance(target, str)
                    or not _LOGICAL_NAME_RE.match(target.strip().lower())):
                raise _bad_value(name, 'a GUID or {"table": "<logical name>", '
                                       '"id": "<GUID>"}', value)
            return _LookupRef(_guid(name, value["id"]), target.strip().lower())
        return _LookupRef(_guid(name, value))
    if col.is_choice:
        if col.multi and value == []:
            return None   # an empty selection clears it
        items = value if col.multi and isinstance(value, list) else [value]
        if not items or not all(isinstance(v, (int, str)) and not isinstance(v, bool)
                                for v in items):
            raise _bad_value(name, "a choice's label or numeric value", value)
        return value
    if atype in ("String", "Memo") and isinstance(value, str):
        return value
    if atype in _INTEGER_TYPES and isinstance(value, int) and not isinstance(value, bool):
        return value
    if (atype in _NUMBER_TYPES or atype == "Money") and isinstance(
            value, (int, float)) and not isinstance(value, bool):
        return value
    if atype == "Boolean" and isinstance(value, bool):
        return value
    if atype == "Uniqueidentifier":
        return _guid(name, value)
    if atype == "DateTime" and isinstance(value, str):
        return _date_value(col, value)
    raise _bad_value(name, _EXPECTED.get(atype, "a value of its type"), value)


_EXPECTED = {
    "String": "text", "Memo": "text", "Integer": "a whole number",
    "BigInt": "a whole number", "Decimal": "a number", "Double": "a number",
    "Money": "a number", "Boolean": "true or false",
}


def _date_value(col: _Col, value: str) -> str:
    if col.date_only:
        try:
            return datetime.strptime(value, "%Y-%m-%d").date().isoformat()
        except ValueError:
            raise _bad_value(col.attr, "a 'YYYY-MM-DD' date", value)
    text = value.strip()
    try:
        moment = datetime.fromisoformat(text[:-1] + "+00:00" if text.endswith("Z") else text)
    except ValueError:
        moment = None
    if moment is None or moment.tzinfo is None:
        raise _bad_value(col.attr, "an ISO 8601 date-time with 'Z' or an offset, "
                                   "e.g. '2026-10-06T09:00:00Z'", value)
    return moment.astimezone(_tz.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# --- dates ------------------------------------------------------------------


def _date_range(start: str, end: str) -> Tuple[date, date]:
    """The inclusive range as dates, rejecting anything but real calendar dates.

    Parsing (rather than passing the strings through) also keeps the `$filter`
    literal safe: only a real date ever reaches the query.
    """
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


def _date_filter(col: _Col, start: date, end: date) -> str:
    """`$filter` for an inclusive date range on `col`.

    A date-only column is an `Edm.Date` and compares with date literals; a
    date-time compares in UTC, up to (not including) the day after `end`.
    """
    if col.date_only:
        return f"{col.prop} ge {start.isoformat()} and {col.prop} le {end.isoformat()}"
    after = end + timedelta(days=1)
    return (f"{col.prop} ge {start.isoformat()}T00:00:00Z "
            f"and {col.prop} lt {after.isoformat()}T00:00:00Z")


_QUERY_SAFE = ",'()=:$"


def _query_string(params: Dict[str, str]) -> str:
    """OData system query options, percent-encoded with `%20` for spaces.

    `requests`' own encoding writes spaces as `+`, which OData does not promise
    to read as a space inside `$filter`.
    """
    return "&".join(f"{k}={quote(v, safe=_QUERY_SAFE)}" for k, v in params.items())


# --- transport --------------------------------------------------------------


def _default_http(method: str, url: str, token: str,
                  body: Optional[Dict] = None,
                  headers: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    import requests
    request_headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "OData-MaxVersion": "4.0",
        "OData-Version": "4.0",
        **(headers or {}),
    }
    kwargs: Dict[str, Any] = {} if body is None else {"json": body}
    resp = requests.request(method, url, headers=request_headers, timeout=60, **kwargs)
    if resp.status_code >= 400:
        raise _dataverse_error(resp)
    if resp.status_code == 204 or not resp.content:
        return {}   # an update, or a create without return=representation
    return resp.json()


def _dataverse_error(resp) -> Exception:
    """Turn a Dataverse error response into an actionable exception.

    Dataverse names the problem in the body's `error.message` — a missing
    privilege, an unknown column, a business rule. A 403 is the connected user's
    security roles, which a retry or reconnect will not change. Only 429 and 5xx
    are retriable.
    """
    status = resp.status_code
    code, message = "", ""
    try:
        body = resp.json()
        error = body.get("error") if isinstance(body, dict) else None
        if isinstance(error, dict):
            code = str(error.get("code") or "")
            message = str(error.get("message") or "")
    except ValueError:
        message = (resp.text or "")[:200]   # non-JSON (e.g. an HTML error page)

    if status == 401:
        return _AuthError()
    if status == 429:
        return ApiError(
            ErrorCode.RATE_LIMITED,
            f"Dynamics 365's service protection limit was reached: "
            f"{message or 'too many requests'}.",
            retriable=True, retry_after_seconds=_retry_after(resp),
        )
    details = {"status": status, "code": code, "error": message}
    if status == 403:
        return ApiError(
            ErrorCode.UPSTREAM_ERROR,
            f"Dynamics 365 refused this for the connected user: "
            f"{message or 'access denied'}. Their security roles in the "
            f"environment must allow it.",
            retriable=False, details=details,
        )
    return ApiError(
        ErrorCode.UPSTREAM_ERROR,
        f"Dynamics 365 API error ({status}): {message or 'unknown error'}",
        retriable=status >= 500, details=details,
    )


def _retry_after(resp) -> Optional[int]:
    headers = getattr(resp, "headers", None) or {}
    try:
        return int(float(headers["Retry-After"]))
    except (KeyError, TypeError, ValueError):
        return None


class _AuthError(Exception):
    """Internal marker for a 401 from Dataverse, mapped to AUTH_EXPIRED."""


def _not_found(exc: ApiError) -> bool:
    """A missing table or record. Metadata lookups may say so with a 400."""
    status = exc.details.get("status")
    return status == 404 or (
        status == 400 and "could not find" in str(exc.details.get("error")).lower())


class Dynamics365Connector(ApiConnector):
    def __init__(self, datasource, http: Optional[Callable] = None,
                 token_refresher: Optional[Callable] = None):
        super().__init__(datasource, token_refresher=token_refresher)
        self._http = http or _default_http
        # Metadata, cached for the connector's lifetime so a query or a write
        # does not refetch it: tables, column catalogues, lookup relationships.
        self._tables: Dict[str, _Table] = {}
        self._catalogues: Dict[str, _Catalogue] = {}
        self._navigation: Dict[str, Dict[Tuple[str, str], str]] = {}

    # -- transport ----------------------------------------------------------

    def _host(self) -> str:
        host = (self._tokens().get("INSTANCE") or "").strip().lower()
        if not _ENV_HOST_RE.match(host):
            raise ApiError(
                ErrorCode.AUTH_EXPIRED,
                f"{self.key} has no recognised Dynamics 365 environment; "
                f"reconnect the source.",
            )
        return host

    def _base(self) -> str:
        return f"https://{self._host()}{_API_PATH}"

    def _call(self, method: str, url: str, body: Optional[Dict] = None,
              headers: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        try:
            return self._http(method, url, self.access_token(), body, headers)
        except _AuthError:
            raise ApiError(
                ErrorCode.AUTH_EXPIRED,
                f"{self.key} access was rejected; reconnect the source.",
            )
        except ApiError:
            raise
        except Exception as exc:   # noqa: BLE001
            logger.warning("Dynamics 365 request failed: %s", exc)
            raise ApiError(
                ErrorCode.UPSTREAM_ERROR,
                "Dynamics 365 returned an error. Try again.",
            )

    def _get(self, path: str, params: Optional[Dict[str, str]] = None,
             headers: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        url = f"{self._base()}/{path}"
        if params:
            url += "?" + _query_string(params)
        return self._call("GET", url, headers=headers)

    # -- discovery ----------------------------------------------------------

    def list_accounts(self) -> List[Account]:
        data = self._get("organizations", {"$select": "name,_basecurrencyid_value"})
        host = self._host()
        accounts: List[Account] = []
        for org in data.get("value") or []:
            oid = org.get("organizationid")
            if not oid:
                continue
            accounts.append(Account(
                id=str(oid),
                name=org.get("name") or host,
                currency=self._currency_code(org.get("_basecurrencyid_value")),
                extra={"environment_url": f"https://{host}"},
            ))
        return accounts

    def _currency_code(self, currency_id) -> Optional[str]:
        """The ISO code of a currency record. Best-effort: None if unreadable."""
        if not isinstance(currency_id, str) or not _GUID_RE.match(currency_id):
            return None
        try:
            data = self._get(f"transactioncurrencies({currency_id})",
                             {"$select": "isocurrencycode"})
        except ApiError as exc:
            logger.warning("Dynamics 365 currency lookup failed: %s", exc.message)
            return None
        return data.get("isocurrencycode") or None

    def list_fields(self, report_type: Optional[str] = None) -> List[Field]:
        """The report's columns, from the table's live metadata.

        `Custom` has no table outside a query — `list_fields` is called without
        settings — so it reports nothing rather than guessing; the columns
        arrive with the query that names the table.
        """
        report_type = self._report_type(report_type)
        if report_type == _CUSTOM_REPORT:
            return []
        table = self._table(report_type, {})
        return [c.field for c in self._catalogue(table).cols.values()]

    @staticmethod
    def _report_type(report_type: Optional[str]) -> str:
        if report_type in _REPORTS or report_type == _CUSTOM_REPORT:
            return report_type
        return _DEFAULT_REPORT

    def _table(self, report_type: str, settings: Dict[str, Any]) -> _Table:
        """The table a report reads: its own, or the `table` setting's for Custom."""
        if report_type != _CUSTOM_REPORT:
            return self._entity(_REPORTS[report_type].table)
        name = str(settings.get("table") or "").strip().lower()
        if not _LOGICAL_NAME_RE.match(name):
            raise ApiError(
                ErrorCode.INVALID_SETTING,
                f"The 'Custom' report needs a 'table' setting with a table's "
                f"logical name, e.g. 'quote' or 'new_project'; got "
                f"{settings.get('table')!r}.",
                retriable=False,
                details={"setting_id": "table", "report_type": _CUSTOM_REPORT},
            )
        return self._entity(name)

    def _entity(self, logical_name: str) -> _Table:
        """A table's metadata: its entity set (the URL path) and key columns.

        A standard report's table falls back to what is known about it if the
        metadata cannot be read; any other table must be found.
        """
        cached = self._tables.get(logical_name)
        if cached is not None:
            return cached
        report = _REPORTS.get(_REPORT_BY_TABLE.get(logical_name, ""))
        try:
            data = self._get(
                f"EntityDefinitions(LogicalName='{logical_name}')",
                {"$select": "LogicalName,EntitySetName,PrimaryIdAttribute,"
                            "PrimaryNameAttribute,IsCustomEntity,DisplayName"})
            primary_name = data.get("PrimaryNameAttribute") or ""
            table = _Table(
                logical_name=logical_name,
                entity_set=_checked(data.get("EntitySetName"), _ENTITY_SET_RE),
                primary_id=_checked(data.get("PrimaryIdAttribute"), _LOGICAL_NAME_RE),
                primary_name=primary_name if _LOGICAL_NAME_RE.match(primary_name) else "",
                is_custom=bool(data.get("IsCustomEntity")),
                label=_label(data, "DisplayName"),
            )
        except ApiError as exc:
            if _not_found(exc):
                raise self._no_such_table(logical_name)
            if report is None:
                raise
            logger.warning("Dynamics 365 metadata for %s unavailable (%s); "
                           "using the known entity set.", logical_name, exc.message)
            table = _Table(logical_name, report.entity_set, report.primary_id)
        self._tables[logical_name] = table
        return table

    @staticmethod
    def _no_such_table(logical_name: str) -> ApiError:
        report_type = _REPORT_BY_TABLE.get(logical_name)
        if report_type:
            return ApiError(
                ErrorCode.INVALID_REPORT_TYPE,
                f"This Dynamics 365 environment has no '{logical_name}' table, "
                f"so the {report_type} report is unavailable — it comes with a "
                f"Dynamics 365 app (Sales or Customer Service) that is not "
                f"installed here.",
                retriable=False, details={"table": logical_name},
            )
        return ApiError(
            ErrorCode.INVALID_SETTING,
            f"This Dynamics 365 environment has no table with the logical name "
            f"'{logical_name}'. Logical names are lower case, e.g. 'quote' or "
            f"'new_project' (Power Apps → Tables shows them).",
            retriable=False, details={"setting_id": "table", "table": logical_name},
        )

    def _catalogue(self, table: _Table) -> _Catalogue:
        """The column catalogue for a table, from its live metadata.

        A curated column keeps its description and default status. If discovery
        fails the curated set is used, so a metadata hiccup never takes reads
        down — though writes, which need the metadata, refuse.
        """
        cached = self._catalogues.get(table.logical_name)
        if cached is not None:
            return cached
        known = {k.name: k for k in
                 _CURATED.get(table.logical_name) or _generic_known(table)}
        try:
            metas = self._column_metadata(table.logical_name)
        except Exception as exc:   # noqa: BLE001
            logger.warning(
                "Dynamics 365 column discovery failed for %s (%s); using the "
                "curated catalogue.", table.logical_name, exc)
            cols: Dict[str, _Col] = {}
            for k in known.values():
                for col in _columns(k.as_meta(), known=k,
                                    date_only=k.name in _KNOWN_DATE_ONLY):
                    cols[col.field.id] = col
            catalogue = _Catalogue(cols, discovered=False)
        else:
            date_only = self._date_only_columns(table.logical_name)
            cols = {}
            for meta in sorted(metas, key=lambda m: m["LogicalName"]):
                name = meta["LogicalName"]
                for col in _columns(meta, known=known.get(name),
                                    date_only=name in date_only):
                    cols[col.field.id] = col
            # Defaults first, in their curated order, then the rest by name.
            order = [k for k in known if k in cols]
            cols = {**{k: cols[k] for k in order}, **cols}
            catalogue = _Catalogue(cols, discovered=True)
        self._catalogues[table.logical_name] = catalogue
        return catalogue

    def _column_metadata(self, logical_name: str) -> List[Dict[str, Any]]:
        data = self._get(
            f"EntityDefinitions(LogicalName='{logical_name}')/Attributes",
            {"$select": "LogicalName,AttributeType,AttributeTypeName,DisplayName,"
                        "Description,IsValidForRead,IsValidForCreate,"
                        "IsValidForUpdate,AttributeOf,IsCustomAttribute"})
        metas = [m for m in data.get("value") or []
                 if isinstance(m, dict) and _readable(m)
                 and _LOGICAL_NAME_RE.match(m["LogicalName"])]
        if not metas:
            raise ApiError(ErrorCode.UPSTREAM_ERROR,
                           f"no column metadata for {logical_name}")
        return metas

    def _date_only_columns(self, logical_name: str) -> Set[str]:
        """The table's date-only columns. Best-effort: the known ones on failure."""
        try:
            data = self._get(
                f"EntityDefinitions(LogicalName='{logical_name}')/Attributes/"
                f"Microsoft.Dynamics.CRM.DateTimeAttributeMetadata",
                {"$select": "LogicalName,DateTimeBehavior"})
        except ApiError as exc:
            logger.warning("Dynamics 365 date metadata for %s unavailable: %s",
                           logical_name, exc.message)
            return set(_KNOWN_DATE_ONLY)
        return {m.get("LogicalName") for m in data.get("value") or []
                if ((m.get("DateTimeBehavior") or {}).get("Value")) == "DateOnly"}

    # -- query --------------------------------------------------------------

    def _date_column(self, settings: Dict[str, Any],
                     catalogue: _Catalogue) -> Optional[_Col]:
        """The column the date range filters on, or None to not filter at all.

        An explicit `date_field` is enforced (a typo must not silently widen the
        query to the whole table); the `createdon` default is dropped if the
        table has none.
        """
        requested = str(settings.get("date_field") or "").strip().lower()
        date_cols = [fid for fid, c in catalogue.cols.items()
                     if c.is_date and fid == c.attr]
        if requested:
            col = catalogue.cols.get(requested)
            if col is None or not col.is_date:
                if col is not None:
                    raise ApiError(
                        ErrorCode.INVALID_SETTING,
                        f"date_field {requested!r} is not a date column. Date "
                        f"columns: {', '.join(date_cols)}.",
                        retriable=False,
                        details={"setting_id": "date_field", "accepted": date_cols},
                    )
                raise invalid_field(requested, date_cols)
            return col
        col = catalogue.cols.get(_DEFAULT_DATE_FIELD)
        return col if col is not None and col.is_date else None

    def _run(self, spec: QuerySpec) -> QueryResult:
        report_type = self._report_type(spec.report_type)
        table = self._table(report_type, spec.settings)
        catalogue = self._catalogue(table)

        unknown = [f for f in spec.fields if f not in catalogue.cols]
        if unknown:
            raise invalid_field(unknown[0], list(catalogue.cols))
        # The id column is the last resort, so `$select` is never empty.
        requested = (list(dict.fromkeys(spec.fields)) or catalogue.defaults
                     or [f for f in (table.primary_id,) if f in catalogue.cols])
        if not requested:
            raise ApiError(
                ErrorCode.INVALID_FIELD,
                f"No columns of the {table.logical_name} table are known; name "
                f"the fields to read.", retriable=False)
        cols = [catalogue.cols[f] for f in requested]
        date_col = self._date_column(spec.settings, catalogue)
        start, end = _date_range(spec.date_range.start, spec.date_range.end)

        # A money column is in each record's own currency; read that too, to
        # warn before rows in different currencies are totalled.
        currency = catalogue.cols.get("_transactioncurrencyid_value")
        check_currency = currency is not None and any(
            c.attr_type == "Money" and not c.attr.endswith("_base") for c in cols)
        props = [c.prop for c in cols] + ([currency.prop] if check_currency else [])
        params = {"$select": ",".join(dict.fromkeys(props))}
        if date_col is not None:
            params["$filter"] = _date_filter(date_col, start, end)
            params["$orderby"] = f"{date_col.prop} desc"

        try:
            records, more = self._fetch(table, params, spec.max_rows)
        except ApiError as exc:
            missing = _NO_PROPERTY_RE.search(str(exc.details.get("error") or ""))
            if not missing:
                raise
            # The metadata listed a column the Web API does not serve; drop it
            # from this connection's catalogue so list_fields stops offering it.
            prop = missing.group(1)
            gone = [fid for fid, c in catalogue.cols.items() if c.prop == prop]
            for fid in gone:
                del catalogue.cols[fid]
            raise ApiError(
                ErrorCode.INVALID_FIELD,
                f"Dynamics 365 cannot read {', '.join(map(repr, gone)) or repr(prop)} "
                f"through its Web API. Query again without it.",
                retriable=False, details={"field": gone[0] if gone else prop},
            )
        rows = [{c.field.id: _cell(c, record) for c in cols} for record in records]

        warnings: List[str] = []
        if more:
            warnings.append(
                f"Stopped at {len(rows)} records; more match. Narrow the date "
                f"range or raise max_rows before counting or totalling.")
        if check_currency:
            found = sorted({str(_cell(currency, r, formatted=True)) for r in records
                            if r.get(currency.prop)})
            if len(found) > 1:
                warnings.append(
                    f"These rows are in {len(found)} currencies "
                    f"({', '.join(found)}); do not total money columns across "
                    f"them — group by transactioncurrencyid, or use the '_base' "
                    f"columns, which are in the organisation's base currency.")

        if date_col is not None:
            note = (f"Records are filtered on '{date_col.attr}' within the "
                    f"selected date range{'' if date_col.date_only else ' (UTC)'}; "
                    f"set the 'date_field' setting to use a different date column.")
        else:
            note = (f"The {table.logical_name} table has no "
                    f"'{_DEFAULT_DATE_FIELD}' column, so the date range was not "
                    f"applied and rows are returned regardless of date.")
        return QueryResult(
            requested_field_ids=requested,
            rows=rows,
            row_count=len(rows),
            notes=[note],
            warnings=warnings,
        )

    def _fetch(self, table: _Table, params: Dict[str, str],
               max_rows: int) -> Tuple[List[Dict[str, Any]], bool]:
        """Up to `max_rows` records, following `@odata.nextLink`.

        Returns the records and whether more matched. A next link is only
        followed to this environment, since the token goes with it.
        """
        base = self._base()
        headers = {"Prefer": f'odata.include-annotations="{_FORMATTED}",'
                             f'odata.maxpagesize={max(1, min(_PAGE, max_rows))}'}
        url = f"{base}/{table.entity_set}?{_query_string(params)}"
        records: List[Dict[str, Any]] = []
        for _ in range(_MAX_PAGES):
            data = self._call("GET", url, headers=headers)
            batch = [r for r in data.get("value") or [] if isinstance(r, dict)]
            room = max_rows - len(records)
            records.extend(batch[:room])
            next_link = data.get("@odata.nextLink")
            if len(batch) > room or (len(records) >= max_rows and next_link):
                return records, True
            if not next_link or not batch:
                return records, False
            if not str(next_link).startswith(f"{base}/"):
                raise ApiError(ErrorCode.UPSTREAM_ERROR,
                               "Dynamics 365 returned an unexpected paging link.")
            url = next_link
        return records, True

    # -- write actions ------------------------------------------------------

    def list_actions(self) -> List[Action]:
        return list(_ACTIONS)

    def execute_action(
        self, action_id: str, account: str,
        params: Optional[Dict[str, Any]] = None, dry_run: bool = False,
    ) -> ActionResult:
        """Perform one write action. Account authorisation happens upstream.

        The token is scoped to one environment, so `account` is recorded, not
        routed on. Every parameter is checked before any write: the table, each
        column against the table's metadata, each value's shape, every choice
        label and every linked record. An update reads the record first, so
        `before` shows exactly what changes.
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
        table = self._write_table(params.get("table"))
        record_id = (_record_id(params.get("record_id"))
                     if action_id == "update_record" else None)
        catalogue = self._catalogue(table)
        if not catalogue.discovered:
            raise ApiError(
                ErrorCode.UPSTREAM_ERROR,
                f"Could not read the {table.logical_name} table's column "
                f"metadata, which a write needs to check its values. Try again.",
            )
        values = _values(table, catalogue, params.get("fields"),
                         creating=record_id is None)
        if record_id is None:
            return self._create_record(table, account, catalogue, values, dry_run)
        return self._update_record(table, account, record_id, catalogue, values,
                                   dry_run)

    def _write_table(self, raw) -> _Table:
        """The table a write targets: a report type or a logical name.

        Writes reach the standard tables behind the reports and custom tables;
        Dataverse's other system tables (users, roles, solutions, …) are refused.
        """
        text = str(raw or "").strip()
        logical = _REPORTS[text].table if text in _REPORTS else text.lower()
        if not _LOGICAL_NAME_RE.match(logical):
            raise ApiError(
                ErrorCode.INVALID_ACTION_PARAMS,
                f"'table' must be a report type ({', '.join(_REPORTS)}) or a "
                f"table's logical name, got {raw!r}.",
                retriable=False, details={"param": "table"},
            )
        try:
            table = self._entity(logical)
        except ApiError as exc:
            if exc.code in (ErrorCode.INVALID_SETTING, ErrorCode.INVALID_REPORT_TYPE):
                raise ApiError(ErrorCode.INVALID_ACTION_PARAMS, exc.message,
                               retriable=False, details={"param": "table"})
            raise
        if logical not in _REPORT_BY_TABLE and not table.is_custom:
            raise ApiError(
                ErrorCode.INVALID_ACTION_PARAMS,
                f"'{logical}' is a Dataverse system table, which this connector "
                f"does not write to. Writable: "
                f"{', '.join(_REPORT_BY_TABLE)}, and custom tables.",
                retriable=False, details={"param": "table"},
            )
        return table

    def _payload(self, table: _Table, catalogue: _Catalogue,
                 values: List[Tuple[_Col, Any]],
                 existing: Optional[Dict[str, Any]] = None) -> Tuple[Dict[str, Any], List[str]]:
        """The request body, plus the lookups whose target could not be checked.

        Choices are resolved to their numeric values and lookups to an
        `@odata.bind` on the column's navigation property, after checking the
        linked record exists. A lookup that is cleared names the navigation
        property it currently uses (from `existing`).
        """
        body: Dict[str, Any] = {}
        unverified: List[str] = []
        existing = existing or {}
        for col, value in values:
            if col.is_lookup:
                if value is None:
                    if existing.get(col.prop) is None:
                        continue   # already empty
                    linked = existing.get(f"{col.prop}@{_LOOKUP_TABLE}")
                    nav = (existing.get(f"{col.prop}@{_NAV_PROPERTY}")
                           or self._navigation_property(table, col, linked))
                    body[nav] = None
                    continue
                nav, bind, checked = self._bind(table, col, value)
                body[f"{nav}@odata.bind"] = bind
                if not checked:
                    unverified.append(col.attr)
            elif col.is_choice and value is not None:
                body[col.attr] = self._choice_value(table, catalogue, col, value)
            else:
                body[col.attr] = value
        return body, unverified

    def _bind(self, table: _Table, col: _Col, ref: _LookupRef) -> Tuple[str, str, bool]:
        """`(navigation property, bind path, whether the target was checked)`."""
        targets = self._lookup_targets(table, col)
        target = ref.table
        if target is None:
            if len(targets) != 1:
                raise ApiError(
                    ErrorCode.INVALID_ACTION_PARAMS,
                    f"{col.attr!r} can link to {', '.join(targets) or 'several tables'}; "
                    f"give it as {{\"table\": \"<one of those>\", \"id\": \"<GUID>\"}}.",
                    retriable=False, details={"field": col.attr, "targets": targets},
                )
            target = targets[0]
        elif target not in targets:
            raise ApiError(
                ErrorCode.INVALID_ACTION_PARAMS,
                f"{col.attr!r} cannot link to a {target!r} record; it links to: "
                f"{', '.join(targets)}.",
                retriable=False, details={"field": col.attr, "targets": targets},
            )
        nav = self._navigation_property(table, col, target)
        linked = self._entity(target)
        checked = True
        try:
            self._get(f"{linked.entity_set}({ref.id})", {"$select": linked.primary_id})
        except ApiError as exc:
            if _not_found(exc):
                raise ApiError(
                    ErrorCode.INVALID_ACTION_PARAMS,
                    f"{col.attr!r}: no {target} record has id {ref.id}. Check "
                    f"the id with data_query.",
                    retriable=False, details={"field": col.attr, "id": ref.id},
                )
            logger.warning("Dynamics 365 %s check skipped: %s", target, exc.message)
            checked = False
        return nav, f"/{linked.entity_set}({ref.id})", checked

    def _lookup_targets(self, table: _Table, col: _Col) -> List[str]:
        data = self._get(
            f"EntityDefinitions(LogicalName='{table.logical_name}')/"
            f"Attributes(LogicalName='{col.attr}')/"
            f"Microsoft.Dynamics.CRM.LookupAttributeMetadata",
            {"$select": "LogicalName,Targets"})
        return [t for t in data.get("Targets") or []
                if isinstance(t, str) and _LOGICAL_NAME_RE.match(t)]

    def _navigation_property(self, table: _Table, col: _Col,
                             target: Optional[str]) -> str:
        """The single-valued navigation property a lookup is written through.

        A polymorphic lookup has one per target (`parentcustomerid_account`,
        `parentcustomerid_contact`); an owner has one for every target
        (`ownerid`), so a column with a single relationship needs no target.
        """
        relationships = self._navigation.get(table.logical_name)
        if relationships is None:
            data = self._get(
                f"EntityDefinitions(LogicalName='{table.logical_name}')/"
                f"ManyToOneRelationships",
                {"$select": "ReferencingAttribute,ReferencedEntity,"
                            "ReferencingEntityNavigationPropertyName"})
            relationships = {
                (r.get("ReferencingAttribute"), r.get("ReferencedEntity")):
                    r["ReferencingEntityNavigationPropertyName"]
                for r in data.get("value") or []
                if isinstance(r, dict)
                and _ENTITY_SET_RE.match(r.get("ReferencingEntityNavigationPropertyName") or "")
            }
            self._navigation[table.logical_name] = relationships
        if target is not None and (col.attr, target) in relationships:
            return relationships[(col.attr, target)]
        navs = {nav for (attr, _), nav in relationships.items() if attr == col.attr}
        if len(navs) == 1:
            return navs.pop()
        raise ApiError(
            ErrorCode.INVALID_ACTION_PARAMS,
            f"{col.attr!r} cannot be set through the Web API on "
            f"{table.logical_name}" + (f" to a {target} record." if target else "."),
            retriable=False, details={"field": col.attr},
        )

    def _choice_value(self, table: _Table, catalogue: _Catalogue, col: _Col, value):
        """A choice's numeric value (a multi-select's comma-separated values),
        from a label (any case) or the value itself."""
        options = catalogue.options.get(col.attr)
        if options is None:
            options = self._options(table, col)
            catalogue.options[col.attr] = options
        items = value if isinstance(value, list) else [value]
        resolved: List[int] = []
        for item in items:
            text = str(item).strip()
            match = next((v for v, label in options
                          if text == str(v) or text.casefold() == label.casefold()),
                         None)
            if match is None:
                raise ApiError(
                    ErrorCode.INVALID_ACTION_PARAMS,
                    f"{col.field.name!r} ({col.attr}) has no option {item!r}. "
                    f"Options: {', '.join(f'{label} ({v})' for v, label in options)}.",
                    retriable=False, details={"field": col.attr},
                )
            resolved.append(match)
        if col.multi:
            return ",".join(str(v) for v in dict.fromkeys(resolved))
        return resolved[0]

    def _options(self, table: _Table, col: _Col) -> List[Tuple[int, str]]:
        cast = ("MultiSelectPicklistAttributeMetadata" if col.multi
                else _OPTION_CASTS[col.attr_type])
        data = self._get(
            f"EntityDefinitions(LogicalName='{table.logical_name}')/"
            f"Attributes(LogicalName='{col.attr}')/Microsoft.Dynamics.CRM.{cast}",
            {"$select": "LogicalName",
             "$expand": "OptionSet($select=Options),GlobalOptionSet($select=Options)"})
        option_set = data.get("OptionSet") or data.get("GlobalOptionSet") or {}
        return [(o["Value"], _label(o, "Label"))
                for o in option_set.get("Options") or []
                if isinstance(o, dict) and isinstance(o.get("Value"), int)]

    def _write(self, method: str, url: str, body: Dict[str, Any],
               headers: Dict[str, str]) -> Dict[str, Any]:
        """Send a write. Dataverse's reason for refusing one is the actionable part."""
        try:
            return self._call(method, url, body, {"Content-Type": "application/json",
                                                  **headers})
        except ApiError as exc:
            status = exc.details.get("status")
            if status == 412:
                raise ApiError(
                    ErrorCode.INVALID_ACTION_PARAMS,
                    "The record no longer exists in Dynamics 365.",
                    retriable=False, details={"status": status},
                )
            if status in (400, 404):
                raise ApiError(
                    ErrorCode.INVALID_ACTION_PARAMS,
                    f"Dynamics 365 rejected the change: "
                    f"{exc.details.get('error') or exc.message}",
                    retriable=False, details={"status": status},
                )
            raise

    def _create_record(self, table: _Table, account: str, catalogue: _Catalogue,
                       values: List[Tuple[_Col, Any]], dry_run: bool) -> ActionResult:
        given = {col.attr: _given(col, value) for col, value in values}
        body, unverified = self._payload(table, catalogue, values)
        names = ", ".join(given)
        if dry_run:
            return ActionResult(
                action="create_record", account=account,
                summary=f"[dry-run — not applied] Would create a new "
                        f"{table.logical_name} record with: {names}.",
                after=given,
                details=_dry_run_details(
                    "column names, writability, value formats, choice labels "
                    "and linked records", unverified),
            )
        data = self._write(
            "POST", f"{self._base()}/{table.entity_set}?"
                    f"{_query_string({'$select': table.primary_id})}",
            body, {"Prefer": "return=representation"})
        record_id = str(data.get(table.primary_id) or "")
        return ActionResult(
            action="create_record", account=account,
            summary=f"Created {table.logical_name} record {record_id} with: {names}.",
            after={"id": record_id, **given},
            details={"table": table.logical_name, "record_id": record_id},
        )

    def _update_record(self, table: _Table, account: str, record_id: str,
                       catalogue: _Catalogue, values: List[Tuple[_Col, Any]],
                       dry_run: bool) -> ActionResult:
        record = self._read_record(table, record_id, [col for col, _ in values])
        body, unverified = self._payload(table, catalogue, values, existing=record)
        before = {"id": record_id, **{col.attr: _cell(col, record) for col, _ in values}}
        given = {col.attr: _given(col, value) for col, value in values}
        after = {"id": record_id, **given}
        names = ", ".join(given)
        if dry_run:
            return ActionResult(
                action="update_record", account=account,
                summary=f"[dry-run — not applied] Would update {table.logical_name} "
                        f"record {record_id}: {names}.",
                before=before, after=after,
                details=_dry_run_details(
                    "column names, writability, value formats, choice labels, "
                    "linked records and that the record exists", unverified),
            )
        if body:
            self._write("PATCH", f"{self._base()}/{table.entity_set}({record_id})",
                        body, {"If-Match": "*"})   # never an upsert
        return ActionResult(
            action="update_record", account=account,
            summary=f"Updated {table.logical_name} record {record_id}: {names}.",
            before=before, after=after,
            details={"table": table.logical_name, "record_id": record_id},
        )

    def _read_record(self, table: _Table, record_id: str,
                     cols: List[_Col]) -> Dict[str, Any]:
        """The record an update targets, with its labels and lookup bindings."""
        props = ",".join(dict.fromkeys([table.primary_id] + [c.prop for c in cols]))
        try:
            return self._get(f"{table.entity_set}({record_id})", {"$select": props},
                             headers={"Prefer": 'odata.include-annotations="*"'})
        except ApiError as exc:
            if not _not_found(exc):
                raise
        raise ApiError(
            ErrorCode.INVALID_ACTION_PARAMS,
            f"No {table.logical_name} record with id {record_id} was found.",
            retriable=False, details={"record_id": record_id},
        )


def _checked(value, pattern: "re.Pattern[str]") -> str:
    """A metadata name that will be placed in a URL, refused if malformed."""
    if isinstance(value, str) and pattern.match(value):
        return value
    raise ApiError(ErrorCode.UPSTREAM_ERROR,
                   f"Dynamics 365 returned unusable table metadata ({value!r}).")


def _record_id(raw) -> str:
    """The update target, checked strictly since it is placed in the request path."""
    text = str(raw or "").strip()
    if not _GUID_RE.match(text):
        raise ApiError(
            ErrorCode.INVALID_ACTION_PARAMS,
            f"'record_id' must be the record's GUID, got {text!r}. Use the "
            f"table's id column (e.g. opportunityid) from data_query.",
            retriable=False, details={"param": "record_id"},
        )
    return text.lower()


def _values(table: _Table, catalogue: _Catalogue, fields,
            creating: bool) -> List[Tuple[_Col, Any]]:
    """`fields`, once every column is writable and every value well-formed."""
    if not isinstance(fields, dict) or not fields:
        raise ApiError(
            ErrorCode.INVALID_ACTION_PARAMS,
            "'fields' must be a non-empty object of column logical names to values.",
            retriable=False, details={"param": "fields"},
        )
    writable = [fid for fid, c in catalogue.cols.items()
                if fid == c.attr and fid != table.primary_id
                and (c.creatable if creating else c.updatable)]
    values: List[Tuple[_Col, Any]] = []
    for name, value in fields.items():
        col = catalogue.cols.get(name)
        if col is None:
            raise invalid_field(name, writable)
        if name == table.primary_id:
            raise ApiError(
                ErrorCode.INVALID_ACTION_PARAMS,
                f"{name!r} is the record's id and cannot be set.",
                retriable=False, details={"field": name},
            )
        if name != col.attr:
            raise ApiError(
                ErrorCode.INVALID_ACTION_PARAMS,
                f"{name!r} is read-only; set the lookup {col.attr!r} instead, to "
                f"the linked record's GUID.",
                retriable=False, details={"field": name},
            )
        if not (col.creatable if creating else col.updatable):
            raise ApiError(
                ErrorCode.INVALID_ACTION_PARAMS,
                f"{name!r} cannot be set when {'creating' if creating else 'updating'} "
                f"a {table.logical_name} record.",
                retriable=False, details={"field": name},
            )
        if value is None and creating and col.is_lookup:
            raise _bad_value(name, "a linked record (leave it out to create "
                                   "without one)", value)
        values.append((col, _write_value(col, value)))
    return values


def _given(col: _Col, value):
    """A checked value as the caller would recognise it, for before/after."""
    if isinstance(value, _LookupRef):
        return {"table": value.table, "id": value.id} if value.table else value.id
    return value


def _cell(col: _Col, record: Dict[str, Any], formatted: Optional[bool] = None):
    """One cell: a choice's label or a lookup's name, else the value itself."""
    raw = record.get(col.prop)
    if raw is None:
        return None
    if col.formatted if formatted is None else formatted:
        label = record.get(f"{col.prop}@{_FORMATTED}")
        return label if label is not None else raw
    return _coerce(col.field, raw)


def _coerce(field: Field, raw):
    """Coerce numeric strings to numbers for metrics; leave text as-is."""
    if field.kind != "metric" or isinstance(raw, bool):
        return raw
    try:
        f = float(raw)
    except (TypeError, ValueError):
        return raw
    if field.is_monetary:
        return f
    return int(f) if f.is_integer() else f


def make_dynamics365_connector(datasource) -> Dynamics365Connector:
    """Build a Dynamics 365 connector wired to refresh its own OAuth token when due."""
    from terno_dbi.connectors.api.auth.oauth import make_ensure_token
    return Dynamics365Connector(
        datasource, token_refresher=make_ensure_token(datasource))


__all__ = ["Dynamics365Connector", "make_dynamics365_connector"]
