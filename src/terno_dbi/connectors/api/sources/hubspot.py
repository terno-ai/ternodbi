"""HubSpot CRM connector.

Implements the `ApiConnector` interface against the HubSpot CRM API v3:

- `list_accounts()` — token introspection (`GET /oauth/v1/access-tokens/{token}`)
  returns the one portal (hub) the OAuth token belongs to. HubSpot is single-
  tenant per token, so there is exactly one "account".
- `list_fields()`   — a *curated* catalogue of properties per report type
  (contacts/companies/deals/tickets/leads), since a portal can have hundreds of
  custom properties.
- `_run()`          — `POST /crm/v3/objects/{type}/search`, filtered to the
  requested date range on the object's date property, paged to `max_rows`.
  A record's owner id is resolved to the owner's name via `GET /crm/v3/owners`.

HubSpot differs from the ad connectors:
  * there is one portal per token, not a list of ad accounts;
  * data is CRM *objects* (records with properties), not pre-aggregated metrics —
    the report type selects the object type, fields are its properties;
  * the date range filters records by their date property (createdate) using the
    CRM Search API, whose filters take epoch-millisecond bounds;
  * search returns at most 100 records per page and 10,000 in total.
"""

from __future__ import annotations
import logging
from datetime import datetime, timezone as _tz
from typing import Any, Callable, Dict, List, Optional

from terno_dbi.connectors.api.model.base import ApiConnector
from terno_dbi.connectors.api.model.errors import ApiError, ErrorCode, invalid_field
from terno_dbi.connectors.api.model.types import Account, Field, QueryResult, QuerySpec

logger = logging.getLogger(__name__)

_API_BASE = "https://api.hubapi.com"
_SEARCH_PAGE = 100        # HubSpot search returns at most 100 records per page.
_SEARCH_MAX_TOTAL = 10000  # HubSpot search will not page beyond 10,000 records.

# "hubspot_owner" is a synthetic name column backed by the real HubSpot property
# "hubspot_owner_id"; the connector resolves the id to the owner's name.
_OWNER_NAME = "hubspot_owner"
_OWNER_ID = "hubspot_owner_id"

# --- report -> CRM object + its date property ------------------------------

_OBJECT_FOR: Dict[str, str] = {
    "Contacts": "contacts",
    "Companies": "companies",
    "Deals": "deals",
    "Tickets": "tickets",
    "Leads": "leads",
}
# The property each report is filtered and sorted by within the date range.
# createdate exists on every CRM object.
_DATE_PROPERTY: Dict[str, str] = {
    "Contacts": "createdate",
    "Companies": "createdate",
    "Deals": "createdate",
    "Tickets": "createdate",
    "Leads": "createdate",
}
_DEFAULT_REPORT = "Contacts"

# Owner columns shared by every object report (records carry hubspot_owner_id).
_OWNER_FIELDS: List[Field] = [
    Field(_OWNER_ID, "Owner ID", "dimension",
          "Internal id of the record's HubSpot owner."),
    Field(_OWNER_NAME, "Owner", "dimension",
          "Name (or email) of the record's HubSpot owner."),
]

# --- curated property catalogues per report --------------------------------

_CONTACT_FIELDS: List[Field] = [
    Field("email", "Email", "dimension", "Primary email address."),
    Field("firstname", "First name", "dimension"),
    Field("lastname", "Last name", "dimension"),
    Field("lifecyclestage", "Lifecycle stage", "dimension",
          "Marketing/sales lifecycle stage (e.g. lead, customer)."),
    Field("hs_lead_status", "Lead status", "dimension"),
    Field("jobtitle", "Job title", "dimension"),
    Field("company", "Company", "dimension", "Company name on the contact."),
    Field("city", "City", "dimension"),
    Field("country", "Country", "dimension"),
    Field("createdate", "Created", "dimension", "When the contact was created.",
          data_type="date"),
    Field("lastmodifieddate", "Last modified", "dimension",
          "When the contact was last changed.", data_type="date"),
    *_OWNER_FIELDS,
]

_COMPANY_FIELDS: List[Field] = [
    Field("name", "Name", "dimension", "Company name."),
    Field("domain", "Domain", "dimension", "Primary web domain."),
    Field("industry", "Industry", "dimension"),
    Field("lifecyclestage", "Lifecycle stage", "dimension"),
    Field("city", "City", "dimension"),
    Field("country", "Country", "dimension"),
    Field("createdate", "Created", "dimension", "When the company was created.",
          data_type="date"),
    Field("numberofemployees", "Employees", "metric",
          "Number of employees.", data_type="integer"),
    Field("annualrevenue", "Annual revenue", "metric",
          "Reported annual revenue.", data_type="number", is_monetary=True,
          is_non_aggregatable=True),
    *_OWNER_FIELDS,
]

_DEAL_FIELDS: List[Field] = [
    Field("dealname", "Deal name", "dimension"),
    Field("dealstage", "Deal stage", "dimension",
          "Stage id within the pipeline (an internal id, not a label)."),
    Field("pipeline", "Pipeline", "dimension",
          "Pipeline id the deal belongs to (an internal id)."),
    Field("dealtype", "Deal type", "dimension", "e.g. newbusiness, existingbusiness."),
    Field("createdate", "Created", "dimension", "When the deal was created.",
          data_type="date"),
    Field("closedate", "Close date", "dimension",
          "When the deal is expected to close, or closed.", data_type="date"),
    Field("amount", "Amount", "metric",
          "Deal value, in the portal's currency.", data_type="number",
          is_monetary=True),
    *_OWNER_FIELDS,
]

_TICKET_FIELDS: List[Field] = [
    Field("subject", "Subject", "dimension", "Ticket subject line."),
    Field("hs_pipeline", "Pipeline", "dimension",
          "Support pipeline id (an internal id)."),
    Field("hs_pipeline_stage", "Stage", "dimension",
          "Ticket status/stage id within the pipeline (an internal id)."),
    Field("hs_ticket_priority", "Priority", "dimension",
          "LOW, MEDIUM, or HIGH."),
    Field("hs_ticket_category", "Category", "dimension",
          "Ticket category, when set."),
    Field("createdate", "Created", "dimension", "When the ticket was created.",
          data_type="date"),
    Field("hs_lastmodifieddate", "Last modified", "dimension",
          "When the ticket was last changed.", data_type="date"),
    *_OWNER_FIELDS,
]

_LEAD_FIELDS: List[Field] = [
    Field("hs_lead_name", "Lead name", "dimension", "The full name of the lead."),
    Field("hs_lead_type", "Lead type", "dimension",
          "Lead type category (e.g. NEW BUSINESS)."),
    Field("hs_lead_label", "Lead label", "dimension",
          "Current lead status/label (e.g. WARM)."),
    Field("createdate", "Created", "dimension", "When the lead was created.",
          data_type="date"),
    *_OWNER_FIELDS,
]

_FIELDS_FOR: Dict[str, List[Field]] = {
    "Contacts": _CONTACT_FIELDS,
    "Companies": _COMPANY_FIELDS,
    "Deals": _DEAL_FIELDS,
    "Tickets": _TICKET_FIELDS,
    "Leads": _LEAD_FIELDS,
}


def _catalogue(report_type: Optional[str]) -> Dict[str, Field]:
    fields = _FIELDS_FOR.get(report_type or _DEFAULT_REPORT, _CONTACT_FIELDS)
    return {f.id: f for f in fields}


def _day_bounds_ms(start: str, end: str) -> tuple:
    """The [start 00:00:00, end 23:59:59.999] window as epoch milliseconds (UTC).

    HubSpot stores datetime properties as epoch milliseconds and its search
    filters compare against the same unit, so the inclusive day range must be
    expressed in ms — a plain date string is rejected.
    """
    s = datetime.strptime(start, "%Y-%m-%d").replace(tzinfo=_tz.utc)
    e = datetime.strptime(end, "%Y-%m-%d").replace(tzinfo=_tz.utc)
    start_ms = int(s.timestamp() * 1000)
    end_ms = int((e.timestamp() + 86400) * 1000) - 1   # end of the last day
    return start_ms, end_ms


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
    if resp.status_code == 401:
        raise _AuthError()
    resp.raise_for_status()
    return resp.json()


class _AuthError(Exception):
    """Internal marker for a 401 from HubSpot, mapped to AUTH_EXPIRED."""


class HubSpotConnector(ApiConnector):
    def __init__(self, datasource, http: Optional[Callable] = None,
                 token_refresher: Optional[Callable] = None):
        super().__init__(datasource, token_refresher=token_refresher)
        self._http = http or _default_http

    # -- transport ----------------------------------------------------------

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
            logger.warning("HubSpot request failed: %s", exc)
            raise ApiError(
                ErrorCode.UPSTREAM_ERROR,
                "HubSpot returned an error. Try again.",
            )

    # -- discovery ----------------------------------------------------------

    def list_accounts(self) -> List[Account]:
        token = self.access_token()
        data = self._call(
            "GET", f"{_API_BASE}/oauth/v1/access-tokens/{token}")
        hub_id = data.get("hub_id")
        if hub_id is None:
            return []
        domain = data.get("hub_domain") or ""
        name = domain or f"HubSpot portal {hub_id}"
        return [Account(id=str(hub_id), name=name,
                        extra={"hub_domain": domain} if domain else {})]

    def list_fields(self, report_type: Optional[str] = None) -> List[Field]:
        return list(_catalogue(report_type).values())

    # -- query --------------------------------------------------------------

    def _owner_map(self) -> Dict[str, str]:
        """`{owner_id: "First Last"}` for the portal, for resolving owner names.

        Best-effort: if owners cannot be read (e.g. the scope was declined) the
        map is empty and the connector falls back to the raw owner id rather
        than failing the whole query over a secondary enrichment.
        """
        mapping: Dict[str, str] = {}
        url = f"{_API_BASE}/crm/v3/owners/"
        after: Optional[str] = None
        try:
            while True:
                params: Dict[str, Any] = {"limit": 500}
                if after:
                    params["after"] = after
                data = self._call("GET", url, params)
                for owner in data.get("results", []):
                    oid = owner.get("id")
                    if oid is None:
                        continue
                    name = " ".join(
                        p for p in (owner.get("firstName"), owner.get("lastName"))
                        if p
                    ).strip()
                    mapping[str(oid)] = name or owner.get("email") or str(oid)
                after = (((data.get("paging") or {}).get("next") or {}).get("after"))
                if not after:
                    break
        except ApiError as exc:
            logger.warning("HubSpot owner lookup failed: %s", exc.message)
        return mapping

    def _run(self, spec: QuerySpec) -> QueryResult:
        report_type = spec.report_type if spec.report_type in _OBJECT_FOR else _DEFAULT_REPORT
        object_type = _OBJECT_FOR[report_type]
        catalogue = _catalogue(report_type)

        unknown = [f for f in spec.fields if f not in catalogue]
        if unknown:
            raise invalid_field(unknown[0], list(catalogue.keys()))

        # The fields the caller asked for (or the full curated set).
        requested = list(spec.fields) or list(catalogue.keys())
        # Real HubSpot property names to request: the synthetic owner-name column
        # is backed by the hubspot_owner_id property.
        api_props: List[str] = []
        for f in requested:
            prop = _OWNER_ID if f == _OWNER_NAME else f
            if prop not in api_props:
                api_props.append(prop)

        date_prop = _DATE_PROPERTY[report_type]
        start_ms, end_ms = _day_bounds_ms(spec.date_range.start, spec.date_range.end)

        url = f"{_API_BASE}/crm/v3/objects/{object_type}/search"
        body: Dict[str, Any] = {
            "filterGroups": [{"filters": [{
                "propertyName": date_prop,
                "operator": "BETWEEN",
                "value": start_ms,
                "highValue": end_ms,
            }]}],
            "properties": api_props,
            "sorts": [{"propertyName": date_prop, "direction": "DESCENDING"}],
        }

        owner_map = self._owner_map() if _OWNER_NAME in requested else {}

        max_total = min(spec.max_rows, _SEARCH_MAX_TOTAL)
        rows: List[Dict[str, Any]] = []
        after: Optional[str] = None
        while len(rows) < max_total:
            body["limit"] = min(max_total - len(rows), _SEARCH_PAGE)
            if after:
                body["after"] = after
            else:
                body.pop("after", None)
            data = self._call("POST", url, body)
            results = data.get("results", [])
            for obj in results:
                props = obj.get("properties") or {}
                record: Dict[str, Any] = {}
                for f in requested:
                    if f == _OWNER_NAME:
                        oid = props.get(_OWNER_ID)
                        record[f] = (owner_map.get(str(oid), oid)
                                     if oid is not None else None)
                    else:
                        record[f] = _coerce(f, props.get(f), catalogue)
                rows.append(record)
            after = (((data.get("paging") or {}).get("next") or {}).get("after"))
            if not after or not results:
                break

        return QueryResult(
            requested_field_ids=requested,
            rows=rows[:max_total],
            row_count=len(rows[:max_total]),
            notes=[f"Records are filtered by '{date_prop}' within the selected "
                   f"date range."],
        )


def _coerce(field_id, raw, catalogue):
    """Coerce numeric HubSpot property strings to numbers; leave text as-is."""
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


def make_hubspot_connector(datasource) -> HubSpotConnector:
    """Build a HubSpot connector wired to refresh its own OAuth token when due."""
    from terno_dbi.connectors.api.auth.oauth import make_ensure_token
    return HubSpotConnector(
        datasource, token_refresher=make_ensure_token(datasource))


__all__ = ["HubSpotConnector", "make_hubspot_connector"]
