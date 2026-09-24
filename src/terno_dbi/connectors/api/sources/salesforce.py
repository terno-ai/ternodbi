"""Salesforce connector.

Implements the `ApiConnector` interface against the Salesforce REST API using
SOQL directly:

- `list_accounts()` — the connected Salesforce org itself, from the
  `Organization` record. A connection is scoped to exactly one org, so there is
  one account; it still goes through the account machinery because that is what
  the allowlist (§7) and the currency guard are built on.
- `list_fields()`   — the sObject's `describe`, live. This is the one place
  Salesforce differs sharply from the ad platforms: a curated field list would
  be wrong on arrival, because most orgs carry custom fields (`Pipeline__c`)
  that exist nowhere else. Cached per object for the life of the connector.
- `_run()`          — `GET /services/data/{v}/query` with a generated SOQL
  query, following `nextRecordsUrl` until `max_rows` is satisfied.

Three things make Salesforce different from the other API sources:

  * the API host is per-tenant. The token response carries an `instance_url`
    (`https://acme.my.salesforce.com`) and every request must go there, not to
    the login host that issued the token;
  * access tokens carry no `expires_in` — they live until the org's session
    timeout — so a 401 means the session ended and the source needs
    reconnecting;
  * every object has its own idea of "when". An Opportunity is dated by
    `CloseDate` (a Date), a Lead by `CreatedDate` (a DateTime), and the two take
    different SOQL literals. The date field per report is a default, not a
    rule — the `date_field` setting overrides it.

The `Custom` report type exists because a curated list of standard objects would
miss the point of Salesforce: it takes any queryable object, including custom
ones, as its `object` setting.
"""

from __future__ import annotations
import logging
import os
import re
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

from terno_dbi.connectors.api.model.base import ApiConnector
from terno_dbi.connectors.api.model.errors import ApiError, ErrorCode, invalid_field
from terno_dbi.connectors.api.model.types import Account, Field, QueryResult, QuerySpec
from terno_dbi.connectors.api.sources._multi import gather_accounts

logger = logging.getLogger(__name__)

# Salesforce retires API versions slowly (roughly a five-year window), but an
# org on an older release may not have the newest. Deployment-tunable rather
# than a constant to be edited and released.
_VERSION_ENV = "TERNO_SALESFORCE_API_VERSION"
_DEFAULT_VERSION = "v64.0"

# `query` returns at most 2000 records per page whatever LIMIT asks for; more
# arrive through `nextRecordsUrl`. Bound the follow-on pages so a careless
# max_rows cannot walk an entire org.
_MAX_PAGES = 25


def api_version() -> str:
    return os.getenv(_VERSION_ENV, "").strip() or _DEFAULT_VERSION


# -- report types -----------------------------------------------------------

@dataclass(frozen=True)
class _Report:
    """One report: the sObject it reads and the field that dates a record."""

    sobject: str
    date_field: str


# The standard CRM objects, each with the field a user means by "when". An
# Opportunity's date is when it closes, not when it was typed in; everything
# else defaults to its creation date.
_REPORTS: Dict[str, _Report] = {
    "Opportunity": _Report("Opportunity", "CloseDate"),
    "Lead": _Report("Lead", "CreatedDate"),
    "Account": _Report("Account", "CreatedDate"),
    "Contact": _Report("Contact", "CreatedDate"),
    "Case": _Report("Case", "CreatedDate"),
    "Campaign": _Report("Campaign", "CreatedDate"),
    # The object comes from the `object` setting; see `_report_for`.
    "Custom": _Report("", "CreatedDate"),
}
_DEFAULT_REPORT = "Opportunity"
_CUSTOM_REPORT = "Custom"


def _default_date_field(report_type: Optional[str]) -> str:
    """The field a report dates its records by, before any `date_field` setting."""
    return _REPORTS.get(report_type or "", _REPORTS[_DEFAULT_REPORT]).date_field


# -- describe -> Field ------------------------------------------------------

# Numeric describe types. These are the fields it is meaningful to sum, so they
# become metrics; everything else is a dimension to group by.
_NUMERIC_TYPES = frozenset({"currency", "double", "int", "long", "percent"})

# A percent is a per-record rate (Opportunity.Probability); summing a column of
# them is meaningless, so it carries the same flag GA4's rates do.
_RATE_TYPES = frozenset({"percent"})

# Types SOQL cannot return as a scalar: compound fields come back as nested
# objects and blobs are not returned by `query` at all. Listing them would
# promise columns that arrive empty or unusable.
_UNSELECTABLE_TYPES = frozenset({"address", "location", "base64"})

_DATA_TYPES: Dict[str, str] = {
    "int": "integer",
    "long": "integer",
    "double": "number",
    "currency": "number",
    "percent": "number",
    "date": "date",
    "datetime": "datetime",
    "time": "string",
    "boolean": "boolean",
}

# Parents worth exposing a name for. Reference fields are the whole point of a
# CRM report — "opportunities by owner" needs `Owner.Name`, not an 18-character
# `OwnerId` — but only objects that actually carry a `Name` can be joined this
# way, so the set is explicit rather than guessed from `referenceTo`.
_NAMED_PARENTS = frozenset({
    "User", "Account", "Contact", "Lead", "Campaign", "Opportunity", "Case",
    "Product2", "Pricebook2", "RecordType", "Group",
})

# SOQL has no parameter binding, so any identifier that reaches a query is
# checked against the shape Salesforce itself allows for an object or field.
_IDENTIFIER = re.compile(r"^[A-Za-z][A-Za-z0-9_]*(\.[A-Za-z][A-Za-z0-9_]*)?$")


def _check_identifier(value: str, what: str) -> str:
    """Return `value` if it is a plain SOQL identifier, else raise.

    SOQL is assembled as text, so this is the boundary that keeps a setting from
    becoming part of the query's structure.
    """
    text = (value or "").strip()
    if not _IDENTIFIER.match(text):
        raise ApiError(
            ErrorCode.INVALID_SETTING,
            f"{what} {value!r} is not a valid Salesforce name. Use the API "
            f"name, e.g. 'Opportunity' or 'My_Object__c'.",
            retriable=False,
        )
    return text


def _field_from_describe(raw: Dict[str, Any]) -> Optional[Field]:
    """One describe entry as a `Field`, or None if it cannot be queried."""
    name = raw.get("name")
    sf_type = (raw.get("type") or "string").lower()
    if not name or sf_type in _UNSELECTABLE_TYPES:
        return None
    numeric = sf_type in _NUMERIC_TYPES
    return Field(
        id=name,
        name=raw.get("label") or name,
        kind="metric" if numeric else "dimension",
        description=raw.get("inlineHelpText") or "",
        data_type=_DATA_TYPES.get(sf_type, "string"),
        # Custom fields are where an org's own vocabulary lives; separating them
        # is what makes a 300-field describe readable.
        group="Custom fields" if raw.get("custom") else "Standard fields",
        is_non_aggregatable=sf_type in _RATE_TYPES,
        is_monetary=sf_type == "currency",
    )


def _parent_field(raw: Dict[str, Any]) -> Optional[Field]:
    """`Owner.Name` for a reference field, when the parent has a name."""
    relationship = raw.get("relationshipName")
    targets = raw.get("referenceTo") or []
    if not relationship or not targets:
        return None
    # A polymorphic lookup (WhoId -> Lead or Contact) has no single parent to
    # name, so it is left alone rather than resolved to the wrong one.
    if len(targets) != 1 or targets[0] not in _NAMED_PARENTS:
        return None
    # Describe labels a lookup "Owner ID"; the joined name is "Owner name".
    label = (raw.get("label") or relationship).removesuffix(" ID").strip()
    return Field(
        id=f"{relationship}.Name",
        name=f"{label} name" if label else relationship,
        kind="dimension",
        description=f"Name of the related {targets[0]} record.",
        group="Related records",
    )


def _catalogue_from_describe(describe: Dict[str, Any]) -> Dict[str, Field]:
    catalogue: Dict[str, Field] = {}
    for raw in describe.get("fields", []) or []:
        field = _field_from_describe(raw)
        if field is not None:
            catalogue[field.id] = field
        parent = _parent_field(raw)
        if parent is not None and parent.id not in catalogue:
            catalogue[parent.id] = parent
    return catalogue


# -- SOQL -------------------------------------------------------------------

def _date_literal(value: str, data_type: str, *, end: bool) -> str:
    """A SOQL literal for `value`, in the form the field's type accepts.

    A Date field takes a bare `2026-08-01`; a DateTime field takes a full
    instant, and quoting a bare date against one is a parse error. The end of a
    DateTime range reaches to the last second of the day, so "to 31 August"
    includes what happened on the 31st.
    """
    if data_type != "datetime":
        return value
    return f"{value}T23:59:59Z" if end else f"{value}T00:00:00Z"


def _build_soql(sobject: str, fields: List[str], date_field: Optional[str],
                date_type: str, start: str, end: str, limit: int) -> str:
    """Compose the SOQL for one report.

    Ordering is newest-first on the date field so a truncated result is the
    recent end of the range rather than an arbitrary slice.
    """
    select = ", ".join(fields)
    soql = f"SELECT {select} FROM {sobject}"
    if date_field:
        soql += (
            f" WHERE {date_field} >= "
            f"{_date_literal(start, date_type, end=False)}"
            f" AND {date_field} <= {_date_literal(end, date_type, end=True)}"
        )
        soql += f" ORDER BY {date_field} DESC"
    soql += f" LIMIT {int(limit)}"
    return soql


def _dig(record: Dict[str, Any], field_id: str) -> Any:
    """Read a field id out of a record, following `Owner.Name` into the parent.

    A null lookup comes back as `"Owner": null`, not a missing key, so the walk
    has to tolerate a None midway rather than assume a dict.
    """
    current: Any = record
    for part in field_id.split("."):
        if not isinstance(current, dict):
            return None
        current = current.get(part)
    return current


# -- transport --------------------------------------------------------------

def _default_http(method: str, url: str, token: str,
                  params: Optional[Dict] = None) -> Dict[str, Any]:
    import requests
    headers = {"Authorization": f"Bearer {token}",
               "Accept": "application/json"}
    resp = requests.request(method, url, headers=headers,
                            params=params or {}, timeout=60)
    if resp.status_code == 401:
        raise _AuthError()
    if resp.status_code >= 400:
        raise _salesforce_error(resp)
    return resp.json()


def _salesforce_error(resp) -> ApiError:
    """Surface Salesforce's own `errorCode` and message.

    Salesforce returns errors as a JSON *array* of `{message, errorCode}`, and
    the errorCode is the actionable half: INVALID_FIELD names the field,
    REQUEST_LIMIT_EXCEEDED means the org's daily API allocation is spent (which
    no retry will fix today), INVALID_TYPE means the object does not exist or
    the user cannot see it.
    """
    status = resp.status_code
    message, error_code = "", ""
    try:
        body = resp.json()
        first = body[0] if isinstance(body, list) and body else body
        if isinstance(first, dict):
            message = first.get("message", "")
            error_code = first.get("errorCode", "")
    except ValueError:
        message = (resp.text or "")[:200]

    if error_code == "REQUEST_LIMIT_EXCEEDED":
        return ApiError(
            ErrorCode.QUOTA_EXCEEDED,
            "Salesforce daily API request limit reached for this org: "
            f"{message or 'no further requests are allowed today'}.",
            retriable=False,
        )
    if status == 404 and not error_code:
        message = (message or "Not found") + (
            f" (is API version {api_version()} available in this org? "
            f"Set {_VERSION_ENV} to one it supports.)")

    label = f"{status} {error_code}".strip()
    return ApiError(
        ErrorCode.UPSTREAM_ERROR,
        f"Salesforce API error ({label}): {message or 'unknown error'}",
        retriable=status == 429 or status >= 500,
    )


class _AuthError(Exception):
    """Internal marker for a 401 from Salesforce, mapped to AUTH_EXPIRED."""


class SalesforceConnector(ApiConnector):
    def __init__(self, datasource, http: Optional[Callable] = None,
                 token_refresher: Optional[Callable] = None):
        super().__init__(datasource, token_refresher=token_refresher)
        self._http = http or _default_http
        self._describe_cache: Dict[str, Dict[str, Field]] = {}
        self._org: Optional[Account] = None

    # -- transport ----------------------------------------------------------

    def _instance_url(self) -> str:
        """The per-tenant API host recorded when the source was connected."""
        instance = (self._tokens().get("INSTANCE_URL")
                    or self._tokens().get("instance_url") or "").rstrip("/")
        if not instance:
            raise ApiError(
                ErrorCode.AUTH_EXPIRED,
                f"{self.key} has no recorded Salesforce instance URL; "
                f"reconnect the source.",
                retriable=False,
            )
        return instance

    def _base(self) -> str:
        return f"{self._instance_url()}/services/data/{api_version()}"

    def _call(self, method: str, url: str,
              params: Optional[Dict] = None) -> Dict[str, Any]:
        try:
            return self._http(method, url, self.access_token(), params)
        except _AuthError:
            raise ApiError(
                ErrorCode.AUTH_EXPIRED,
                f"{self.key} access was rejected; reconnect the source.",
            )
        except ApiError:
            raise
        except Exception as exc:   # noqa: BLE001
            logger.warning("Salesforce request failed: %s", exc)
            raise ApiError(
                ErrorCode.UPSTREAM_ERROR,
                "Salesforce returned an error. Try again.",
            )

    # -- discovery ----------------------------------------------------------

    def list_accounts(self) -> List[Account]:
        """The connected org, as the single queryable account.

        A Salesforce connection reaches exactly one org, so this is a one-row
        query rather than a listing. It still returns a list: the account is
        what the allowlist restricts and what a query names.
        """
        if self._org is not None:
            return [self._org]
        data = self._call("GET", f"{self._base()}/query", {
            "q": "SELECT Id, Name, OrganizationType, DefaultCurrencyIsoCode, "
                 "TimeZoneSidKey FROM Organization",
        })
        records = data.get("records") or []
        if not records:
            return []
        record = records[0]
        org_id = record.get("Id")
        if not org_id:
            return []
        extra = {}
        if record.get("OrganizationType"):
            extra["edition"] = record["OrganizationType"]
        if record.get("Name"):
            extra["instance_url"] = self._instance_url()
        self._org = Account(
            id=str(org_id),
            name=record.get("Name") or str(org_id),
            currency=record.get("DefaultCurrencyIsoCode"),
            timezone=record.get("TimeZoneSidKey"),
            extra=extra,
        )
        return [self._org]

    def _sobject(self, report_type: Optional[str],
                 settings: Optional[Dict[str, Any]] = None) -> str:
        """The API name of the object a report reads.

        `Custom` takes it from the `object` setting; every other report type
        names its own. An unknown report type falls back to the default rather
        than failing, matching the other connectors — the catalog's settings
        validation has already rejected a genuinely bogus one.
        """
        settings = settings or {}
        if report_type == _CUSTOM_REPORT:
            requested = settings.get("object")
            if not requested:
                raise ApiError(
                    ErrorCode.MISSING_SETTING,
                    "The 'Custom' report needs an 'object' setting naming the "
                    "Salesforce object to read, e.g. 'Quote' or "
                    "'Project__c'.",
                    details={"setting_id": "object",
                             "report_type": _CUSTOM_REPORT},
                    retriable=False,
                )
            return _check_identifier(str(requested), "Object")
        report = _REPORTS.get(report_type or "", _REPORTS[_DEFAULT_REPORT])
        return report.sobject or _REPORTS[_DEFAULT_REPORT].sobject

    def _describe(self, sobject: str) -> Dict[str, Field]:
        """`{field_id: Field}` for one object, from a cached describe.

        Cached per connector instance because a describe is a large response and
        a query asks for it at least twice — once for the dispatch layer's field
        metadata, once to validate the requested fields.
        """
        cached = self._describe_cache.get(sobject)
        if cached is not None:
            return cached
        data = self._call(
            "GET", f"{self._base()}/sobjects/{sobject}/describe")
        catalogue = _catalogue_from_describe(data)
        self._describe_cache[sobject] = catalogue
        return catalogue

    def list_fields(self, report_type: Optional[str] = None) -> List[Field]:
        """Live describe for the report's object.

        `Custom` has no object outside a query — `list_fields` is called without
        settings — so it reports nothing rather than guessing; the fields arrive
        with the query that names the object.
        """
        if report_type == _CUSTOM_REPORT:
            return []
        return list(self._describe(self._sobject(report_type)).values())

    # -- query --------------------------------------------------------------

    def _date_field(self, report_type: Optional[str],
                    settings: Dict[str, Any],
                    catalogue: Dict[str, Field]) -> Optional[str]:
        """Which field the date range filters on, or None to not filter at all.

        The report's default is a guess about intent, not a fact about the
        object: a custom object may have neither `CloseDate` nor `CreatedDate`
        exposed to this user. An explicit `date_field` setting is therefore
        enforced (a typo must not silently widen the query to the whole object),
        while a default that does not exist is dropped with a warning.
        """
        requested = (settings.get("date_field") or "").strip()
        if requested:
            field = _check_identifier(requested, "Date field")
            if field not in catalogue:
                raise invalid_field(field, list(catalogue.keys()))
            return field
        default = _default_date_field(report_type)
        return default if default in catalogue else None

    def _run(self, spec: QuerySpec) -> QueryResult:
        sobject = self._sobject(spec.report_type, spec.settings)
        catalogue = self._describe(sobject)

        unknown = [f for f in spec.fields if f not in catalogue]
        if unknown:
            raise invalid_field(unknown[0], list(catalogue.keys()))

        fields = list(spec.fields)
        if not fields:
            # An empty selection is a dead end; fall back to whatever identifies
            # a record, which every standard object has and most custom ones do.
            fields = [f for f in ("Id", "Name") if f in catalogue] or ["Id"]

        notes: List[str] = []
        date_field = self._date_field(spec.report_type, spec.settings, catalogue)
        if date_field is None:
            notes.append(
                f"{sobject} has no queryable "
                f"{_default_date_field(spec.report_type)} field, so the date "
                f"range was not applied and these rows are the whole object. "
                f"Set the 'date_field' setting to filter on a date this "
                f"object does have."
            )
        else:
            notes.append(
                f"Rows are filtered on {catalogue[date_field].name} "
                f"({date_field}); set the 'date_field' setting to use a "
                f"different date."
            )

        date_type = catalogue[date_field].data_type if date_field else "date"
        soql = _build_soql(sobject, fields, date_field, date_type,
                           spec.date_range.start, spec.date_range.end,
                           spec.max_rows)

        multi = len(spec.accounts) > 1

        def fetch(account):
            return self._fetch(soql, fields, spec.max_rows, account, multi=multi)

        # A Salesforce connection has one org, so this loop runs once in
        # practice. It goes through `gather_accounts` anyway so the failure
        # shape matches every other source.
        rows, warnings = gather_accounts(spec.accounts or [""], fetch)

        return QueryResult(
            requested_field_ids=fields,
            rows=rows,
            row_count=len(rows),
            notes=notes,
            warnings=warnings,
        )

    def _fetch(self, soql: str, fields: List[str], max_rows: int,
               account: str, *, multi: bool = False) -> List[Dict[str, Any]]:
        """Run the query and follow `nextRecordsUrl` until `max_rows`.

        `query` caps a page at 2000 records regardless of LIMIT, so anything
        larger arrives in pages. The page cap stops a runaway request from
        walking an entire object.
        """
        rows: List[Dict[str, Any]] = []
        data = self._call("GET", f"{self._base()}/query", {"q": soql})
        pages = 0
        while True:
            for record in data.get("records") or []:
                if len(rows) >= max_rows:
                    return rows
                rows.append(_parse_record(record, fields, account, multi=multi))
            next_url = data.get("nextRecordsUrl")
            pages += 1
            if (not next_url or data.get("done") or len(rows) >= max_rows
                    or pages >= _MAX_PAGES):
                return rows
            data = self._call("GET", f"{self._instance_url()}{next_url}")


def _parse_record(record: Dict[str, Any], fields: List[str], account: str, *,
                  multi: bool = False) -> Dict[str, Any]:
    """Map one SOQL record to field ids.

    Scalars sit at the top level; a `Owner.Name` selection arrives nested under
    its relationship. The `attributes` envelope Salesforce adds to every record
    (and to every nested one) is dropped — it is response plumbing, not data.
    """
    row: Dict[str, Any] = {}
    if multi:
        row["_account"] = account
    for field_id in fields:
        row[field_id] = _dig(record, field_id)
    return row


def make_salesforce_connector(datasource) -> SalesforceConnector:
    """Build a Salesforce connector wired to refresh its own OAuth token."""
    from terno_dbi.connectors.api.auth.oauth import make_ensure_token
    return SalesforceConnector(
        datasource, token_refresher=make_ensure_token(datasource))


__all__ = ["SalesforceConnector", "api_version", "make_salesforce_connector"]
