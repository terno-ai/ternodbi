"""Zoho CRM connector.

Implements the `ApiConnector` interface against the Zoho CRM REST API v8:

- `list_accounts()` — `GET /crm/v8/org` returns the one organisation the OAuth
  token belongs to, with its home currency and time zone.
- `list_fields()`   — discovered live per module from `GET /crm/v8/settings/fields`,
  so an org's custom fields (and renamed labels) are queryable. A curated
  catalogue per module labels the core fields, picks the default columns, and is
  the fallback if discovery fails.
- `_run()`          — a generated COQL query (`POST /crm/v8/coql`) over the report's
  module, filtered to the date range on `Created_Time` and paged with
  `LIMIT offset, count`. User lookups (Owner, Created_By, …) are resolved to
  names via `GET /crm/v8/users`.

Zoho CRM differs from the other connectors:
  * **Data is regional.** An org lives in one data centre (US, EU, IN, AU, JP, CN,
    CA, SA, UK). The OAuth callback names its accounts server, which the token
    exchange and refreshes use (`INSTANCE`), and the token response names the API
    host (`API_DOMAIN`) every call here goes to.
  * Auth is `Authorization: Zoho-oauthtoken <token>`, not a bearer.
  * Like HubSpot, reports are CRM *records* (a module's rows), not pre-aggregated
    metrics — the report type selects the module and fields are its columns.
  * COQL requires a WHERE clause, returns lookups as `{"id", "name"}` objects, and
    answers an empty result with `204 No Content`.
"""

from __future__ import annotations
import logging
import re
from dataclasses import replace
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Set

from terno_dbi.connectors.api.model.base import ApiConnector
from terno_dbi.connectors.api.model.errors import ApiError, ErrorCode, invalid_field
from terno_dbi.connectors.api.model.types import Account, Field, QueryResult, QuerySpec

logger = logging.getLogger(__name__)

_API_VERSION = "v8"
_PAGE = 200          # COQL rows per call; the per-call cap in every API version.
_MAX_TOTAL = 100000  # COQL will not page beyond 100,000 records per criteria.
_MAX_SELECT = 50     # COQL columns per SELECT, at the most conservative limit.
_DATE_FIELD = "Created_Time"   # exists on every module; the range filters on it.

# The API host saved from the token response. Checked before any call so a
# token is only ever sent to a Zoho API domain.
_API_DOMAIN_RE = re.compile(
    r"^https://www\.(zohoapis\.(com|eu|in|com\.au|jp|com\.cn|ca|sa|uk)"
    r"|zohocloud\.ca)$"
)

# Report type -> module. Report ids are the modules' API names.
_MODULES = ("Leads", "Contacts", "Accounts", "Deals", "Cases",
            "Tasks", "Calls", "Events")
_DEFAULT_REPORT = "Leads"

# Field types COQL cannot select; they are left out of the catalogue.
_UNSELECTABLE_TYPES = frozenset({
    "fileupload", "imageupload", "profileimage", "multiselectlookup",
    "multiuserlookup", "subform",
})
_UNSELECTABLE_FIELDS = frozenset({("Events", "Participants")})

# Lookups that point at CRM users; their ids are resolved to user names.
_USER_TYPES = frozenset({"ownerlookup", "userlookup"})
_INTEGER_TYPES = frozenset({"integer", "bigint"})
_NUMBER_TYPES = frozenset({"double", "decimal"})
_DATE_TYPES = frozenset({"date", "datetime"})

# --- curated catalogues per module -----------------------------------------

_ID = Field("id", "Record ID", "dimension", "Zoho's unique id for the record.")

_COMMON: List[Field] = [
    Field("Owner", "Owner", "dimension", "Name of the CRM user who owns the record."),
    Field("Created_Time", "Created", "dimension", "When the record was created.",
          data_type="date"),
    Field("Modified_Time", "Modified", "dimension",
          "When the record was last changed.", data_type="date"),
]
_COMMON_USER_FIELDS = frozenset({"Owner"})

_CURATED: Dict[str, List[Field]] = {
    "Leads": [
        Field("Full_Name", "Name", "dimension", "The lead's full name."),
        Field("Email", "Email", "dimension"),
        Field("Company", "Company", "dimension"),
        Field("Lead_Source", "Lead source", "dimension",
              "Where the lead came from (e.g. Web Download, Trade Show)."),
        Field("Lead_Status", "Lead status", "dimension",
              "e.g. Attempted to Contact, Contacted, Junk Lead."),
        Field("Industry", "Industry", "dimension"),
        Field("Rating", "Rating", "dimension", "e.g. Acquired, Active, Shutdown."),
        Field("Country", "Country", "dimension"),
        Field("City", "City", "dimension"),
        Field("No_of_Employees", "Employees", "metric",
              "Employees at the lead's company.", data_type="integer"),
        Field("Annual_Revenue", "Annual revenue", "metric",
              "Reported annual revenue of the lead's company.",
              data_type="number", is_monetary=True, is_non_aggregatable=True),
    ],
    "Contacts": [
        Field("Full_Name", "Name", "dimension", "The contact's full name."),
        Field("Email", "Email", "dimension"),
        Field("Account_Name", "Account", "dimension",
              "Name of the account the contact belongs to."),
        Field("Title", "Title", "dimension", "Job title."),
        Field("Department", "Department", "dimension"),
        Field("Lead_Source", "Lead source", "dimension"),
        Field("Mailing_Country", "Country", "dimension"),
        Field("Mailing_City", "City", "dimension"),
    ],
    "Accounts": [
        Field("Account_Name", "Account", "dimension", "Company name."),
        Field("Account_Type", "Type", "dimension",
              "e.g. Customer, Partner, Prospect."),
        Field("Industry", "Industry", "dimension"),
        Field("Rating", "Rating", "dimension"),
        Field("Billing_Country", "Country", "dimension"),
        Field("Billing_City", "City", "dimension"),
        Field("Website", "Website", "dimension"),
        Field("Employees", "Employees", "metric", "Number of employees.",
              data_type="integer"),
        Field("Annual_Revenue", "Annual revenue", "metric",
              "Reported annual revenue.", data_type="number", is_monetary=True,
              is_non_aggregatable=True),
    ],
    "Deals": [
        Field("Deal_Name", "Deal", "dimension"),
        Field("Stage", "Stage", "dimension",
              "Pipeline stage (e.g. Qualification, Closed Won)."),
        Field("Pipeline", "Pipeline", "dimension"),
        Field("Type", "Type", "dimension", "e.g. New Business, Existing Business."),
        Field("Lead_Source", "Lead source", "dimension"),
        Field("Account_Name", "Account", "dimension"),
        Field("Contact_Name", "Contact", "dimension"),
        Field("Closing_Date", "Closing date", "dimension",
              "When the deal is expected to close, or closed.", data_type="date"),
        Field("Amount", "Amount", "metric",
              "Deal value, in the org's currency.", data_type="number",
              is_monetary=True),
        Field("Expected_Revenue", "Expected revenue", "metric",
              "Amount weighted by the stage's probability.", data_type="number",
              is_monetary=True),
        Field("Probability", "Probability (%)", "metric",
              "Likelihood of closing, from the stage.", data_type="number",
              is_non_aggregatable=True),
    ],
    "Cases": [
        Field("Subject", "Subject", "dimension"),
        Field("Status", "Status", "dimension", "e.g. New, Escalated, Closed."),
        Field("Priority", "Priority", "dimension"),
        Field("Case_Origin", "Origin", "dimension", "e.g. Email, Phone, Web."),
        Field("Type", "Type", "dimension"),
        Field("Case_Reason", "Reason", "dimension"),
        Field("Account_Name", "Account", "dimension"),
    ],
    "Tasks": [
        Field("Subject", "Subject", "dimension"),
        Field("Status", "Status", "dimension", "e.g. Not Started, Completed."),
        Field("Priority", "Priority", "dimension"),
        Field("Due_Date", "Due date", "dimension", data_type="date"),
        Field("Closed_Time", "Closed", "dimension", "When the task was completed.",
              data_type="date"),
    ],
    "Calls": [
        Field("Subject", "Subject", "dimension"),
        Field("Call_Type", "Call type", "dimension", "Inbound, Outbound or Missed."),
        Field("Call_Purpose", "Purpose", "dimension"),
        Field("Call_Result", "Result", "dimension"),
        Field("Call_Start_Time", "Start time", "dimension", data_type="date"),
        Field("Call_Duration_in_seconds", "Duration (s)", "metric",
              "Call length in seconds.", data_type="integer"),
    ],
    "Events": [
        Field("Event_Title", "Title", "dimension"),
        Field("Start_DateTime", "Starts", "dimension", data_type="date"),
        Field("End_DateTime", "Ends", "dimension", data_type="date"),
        Field("Venue", "Venue", "dimension"),
    ],
}


def _curated(module: str) -> Dict[str, Field]:
    """The curated catalogue for a module — the fallback and the default columns."""
    fields = [_ID, *_CURATED[module], *_COMMON]
    return {f.id: f for f in fields}


def _discovered_field(meta: Dict[str, Any]) -> Field:
    """A `Field` for one entry of Zoho's field metadata.

    Numeric types become metrics; everything else (text, picklists, lookups,
    dates) is a dimension.
    """
    api = meta["api_name"]
    label = meta.get("display_label") or meta.get("field_label") or api
    group = "Custom fields" if meta.get("custom_field") else None
    ztype = meta.get("data_type") or ""
    if ztype in ("formula", "rollup_summary"):
        # A computed field reports what it evaluates to separately.
        ztype = ((meta.get(ztype) or {}).get("return_type")
                 or meta.get("json_type") or "")
    if ztype == "currency":
        return Field(api, label, "metric", data_type="number", group=group,
                     is_monetary=True)
    if ztype == "percent":
        return Field(api, label, "metric", data_type="number", group=group,
                     is_non_aggregatable=True)
    if ztype in _INTEGER_TYPES:
        return Field(api, label, "metric", data_type="integer", group=group)
    if ztype in _NUMBER_TYPES:
        return Field(api, label, "metric", data_type="number", group=group)
    if ztype in _DATE_TYPES:
        return Field(api, label, "dimension", data_type="date", group=group)
    if ztype == "boolean":
        return Field(api, label, "dimension", data_type="boolean", group=group)
    return Field(api, label, "dimension", group=group)


def _day_bounds(start: str, end: str) -> tuple:
    """The inclusive [start 00:00:00, end 23:59:59] window as COQL datetimes (UTC).

    Parsing the dates, rather than passing them through, also keeps the COQL
    literal safe: only a real calendar date ever reaches the query string.
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
    return f"{s.isoformat()}T00:00:00+00:00", f"{e.isoformat()}T23:59:59+00:00"


def _default_http(method: str, url: str, token: str,
                  json_body: Optional[Dict] = None) -> Dict[str, Any]:
    import requests
    kwargs: Dict[str, Any] = {}
    if method == "GET":
        kwargs["params"] = json_body or None
    else:
        kwargs["json"] = json_body
    resp = requests.request(
        method, url, headers={"Authorization": f"Zoho-oauthtoken {token}"},
        timeout=30, **kwargs)
    if resp.status_code == 204:
        return {}   # Zoho answers "no matching records" with an empty 204.
    if resp.status_code >= 400:
        raise _zoho_error(resp)
    return resp.json()


def _zoho_error(resp) -> Exception:
    """Turn a Zoho error response into an actionable exception.

    Zoho puts a stable `code` (INVALID_QUERY, LIMIT_EXCEEDED, …) and a message in
    the body; surfacing them beats a generic "try again". A missing scope is a
    401 too, but reconnecting (and granting it) is the fix, not a token refresh.
    Only 429/5xx are retriable.
    """
    status = resp.status_code
    code, message = "", ""
    try:
        body = resp.json()
        if isinstance(body, dict) and isinstance(body.get("data"), list) and body["data"]:
            body = body["data"][0]   # record APIs wrap errors per record
        if isinstance(body, dict):
            code = str(body.get("code") or "")
            message = str(body.get("message") or "")
    except ValueError:
        message = (resp.text or "")[:200]   # non-JSON (e.g. an HTML error page)

    if code == "OAUTH_SCOPE_MISMATCH":
        return ApiError(
            ErrorCode.AUTH_EXPIRED,
            "Zoho CRM refused a permission this connection was not granted. "
            "Reconnect the source and allow all requested access.",
            retriable=False,
        )
    if status == 401:
        return _AuthError()
    label = f"{status} {code}".strip()
    return ApiError(
        ErrorCode.UPSTREAM_ERROR,
        f"Zoho CRM API error ({label}): {message or 'unknown error'}",
        retriable=status == 429 or status >= 500,
    )


class _AuthError(Exception):
    """Internal marker for a 401 from Zoho, mapped to AUTH_EXPIRED."""


class ZohoCRMConnector(ApiConnector):
    def __init__(self, datasource, http: Optional[Callable] = None,
                 token_refresher: Optional[Callable] = None):
        super().__init__(datasource, token_refresher=token_refresher)
        self._http = http or _default_http
        # Field catalogue and user-lookup field ids per module, cached for the
        # connector's lifetime so a query does not refetch metadata.
        self._catalogue_cache: Dict[str, Dict[str, Field]] = {}
        self._user_fields: Dict[str, Set[str]] = {}

    # -- transport ----------------------------------------------------------

    def _api_base(self) -> str:
        domain = (self._tokens().get("API_DOMAIN") or "").rstrip("/")
        if not _API_DOMAIN_RE.match(domain):
            raise ApiError(
                ErrorCode.AUTH_EXPIRED,
                f"{self.key} has no recognised Zoho API domain; reconnect the "
                f"source.",
            )
        return f"{domain}/crm/{_API_VERSION}"

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
            logger.warning("Zoho CRM request failed: %s", exc)
            raise ApiError(
                ErrorCode.UPSTREAM_ERROR,
                "Zoho CRM returned an error. Try again.",
            )

    # -- discovery ----------------------------------------------------------

    def list_accounts(self) -> List[Account]:
        data = self._call("GET", f"{self._api_base()}/org")
        accounts: List[Account] = []
        for org in data.get("org") or []:
            oid = org.get("zgid") or org.get("id")
            if not oid:
                continue
            domain = org.get("domain_name") or ""
            accounts.append(Account(
                id=str(oid),
                name=org.get("company_name") or f"Zoho CRM org {oid}",
                currency=org.get("iso_code") or None,
                timezone=org.get("time_zone") or None,
                extra={"domain_name": domain} if domain else {},
            ))
        return accounts

    def list_fields(self, report_type: Optional[str] = None) -> List[Field]:
        return list(self._module_catalogue(self._module(report_type)).values())

    @staticmethod
    def _module(report_type: Optional[str]) -> str:
        return report_type if report_type in _MODULES else _DEFAULT_REPORT

    def _module_catalogue(self, module: str) -> Dict[str, Field]:
        """The field catalogue for a module.

        Fields come from Zoho's metadata, so custom fields are included and each
        keeps the label the org gave it. A curated field keeps its description
        and flags (monetary, non-aggregatable). If discovery fails for any
        reason the curated set is used, so a metadata hiccup never takes the
        connector down.
        """
        cached = self._catalogue_cache.get(module)
        if cached is not None:
            return cached
        curated = _curated(module)
        try:
            metas = self._discover_fields(module)
        except Exception as exc:   # noqa: BLE001
            logger.warning(
                "Zoho CRM field discovery failed for %s (%s); using the curated "
                "catalogue.", module, exc)
            self._catalogue_cache[module] = curated
            self._user_fields[module] = set(_COMMON_USER_FIELDS)
            return curated

        catalogue: Dict[str, Field] = {_ID.id: _ID}
        user_fields: Set[str] = set()
        for meta in metas:
            api = meta.get("api_name")
            ztype = meta.get("data_type") or ""
            if (not api or meta.get("visible") is False
                    or ztype in _UNSELECTABLE_TYPES
                    or (module, api) in _UNSELECTABLE_FIELDS):
                continue
            discovered = _discovered_field(meta)
            known = curated.get(api)
            catalogue[api] = replace(known, name=discovered.name) if known else discovered
            if ztype in _USER_TYPES:
                user_fields.add(api)
        self._catalogue_cache[module] = catalogue
        self._user_fields[module] = user_fields
        return catalogue

    def _discover_fields(self, module: str) -> List[Dict[str, Any]]:
        data = self._call("GET", f"{self._api_base()}/settings/fields",
                          {"module": module})
        fields = data.get("fields") or []
        if not fields:
            raise ApiError(ErrorCode.UPSTREAM_ERROR,
                           f"no field metadata for {module}")
        return fields

    # -- query --------------------------------------------------------------

    def _user_names(self) -> Dict[str, str]:
        """`{user_id: "Full Name"}` for the org, for resolving user lookups.

        Best-effort: if users cannot be read the map is empty and the connector
        falls back to the raw user id rather than failing the whole query over a
        secondary enrichment.
        """
        mapping: Dict[str, str] = {}
        url = f"{self._api_base()}/users"
        page = 1
        try:
            while True:
                data = self._call("GET", url, {
                    "type": "AllUsers", "page": page, "per_page": 200})
                users = data.get("users") or []
                for user in users:
                    uid = user.get("id")
                    if uid is None:
                        continue
                    name = user.get("full_name") or " ".join(
                        p for p in (user.get("first_name"), user.get("last_name"))
                        if p
                    ).strip()
                    mapping[str(uid)] = name or user.get("email") or str(uid)
                if not users or not (data.get("info") or {}).get("more_records"):
                    break
                page += 1
        except ApiError as exc:
            logger.warning("Zoho CRM user lookup failed: %s", exc.message)
        return mapping

    def _run(self, spec: QuerySpec) -> QueryResult:
        module = self._module(spec.report_type)
        catalogue = self._module_catalogue(module)

        unknown = [f for f in spec.fields if f not in catalogue]
        if unknown:
            raise invalid_field(unknown[0], list(catalogue.keys()))

        # Each column once (COQL rejects a repeated one), or the curated defaults
        # this org actually has.
        requested = (list(dict.fromkeys(spec.fields))
                     or [f for f in _curated(module) if f in catalogue])
        if len(requested) > _MAX_SELECT:
            raise ApiError(
                ErrorCode.INVALID_FIELD,
                f"Zoho CRM returns at most {_MAX_SELECT} fields per query; "
                f"{len(requested)} were requested. Split them across queries.",
                retriable=False,
            )

        start, end = _day_bounds(spec.date_range.start, spec.date_range.end)
        # Every identifier here is either a fixed module name or a field id
        # validated against the catalogue, and the dates are re-formatted from
        # parsed values — nothing from the request reaches the query verbatim.
        query = (f"select {', '.join(requested)} from {module} "
                 f"where {_DATE_FIELD} between '{start}' and '{end}' "
                 f"order by {_DATE_FIELD} desc")

        user_fields = self._user_fields.get(module, set()) & set(requested)
        users = self._user_names() if user_fields else {}

        url = f"{self._api_base()}/coql"
        max_total = min(spec.max_rows, _MAX_TOTAL)
        rows: List[Dict[str, Any]] = []
        more = False
        while len(rows) < max_total:
            count = min(_PAGE, max_total - len(rows))
            data = self._call("POST", url, {
                "select_query": f"{query} limit {len(rows)}, {count}"})
            batch = data.get("data") or []
            for record in batch:
                rows.append({
                    f: _value(f, record.get(f), catalogue,
                              users if f in user_fields else None)
                    for f in requested
                })
            more = bool((data.get("info") or {}).get("more_records"))
            if not more or not batch:
                break

        warnings: List[str] = []
        if more and len(rows) >= max_total:
            warnings.append(
                f"Returned the first {len(rows)} records; more match. Narrow the "
                f"date range or raise max_rows before counting or totalling.")

        return QueryResult(
            requested_field_ids=requested,
            rows=rows,
            row_count=len(rows),
            notes=[f"Records are filtered by '{_DATE_FIELD}' within the selected "
                   f"date range (UTC)."],
            warnings=warnings,
        )


def _value(field_id, raw, catalogue, users: Optional[Dict[str, str]]):
    """One cell: a lookup flattened to its name, a user lookup to the user's."""
    if isinstance(raw, dict):   # a lookup: {"id": ..., "name": ...}
        rid = raw.get("id")
        if users is not None and rid is not None:
            return users.get(str(rid)) or raw.get("name") or rid
        return raw.get("name") or rid
    return _coerce(field_id, raw, catalogue)


def _coerce(field_id, raw, catalogue):
    """Coerce numeric strings (e.g. from a formula) to numbers; leave text as-is."""
    if raw is None:
        return None
    field = catalogue.get(field_id)
    if field and field.kind == "metric":
        try:
            f = float(raw)
            if field.is_monetary:
                return f
            return int(f) if f.is_integer() else f
        except (TypeError, ValueError):
            return raw
    return raw


def make_zoho_crm_connector(datasource) -> ZohoCRMConnector:
    """Build a Zoho CRM connector wired to refresh its own OAuth token when due."""
    from terno_dbi.connectors.api.auth.oauth import make_ensure_token
    return ZohoCRMConnector(
        datasource, token_refresher=make_ensure_token(datasource))


__all__ = ["ZohoCRMConnector", "make_zoho_crm_connector"]
