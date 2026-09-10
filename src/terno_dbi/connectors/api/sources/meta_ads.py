"""Meta Ads connector.

Implements the `ApiConnector` interface against the Meta Marketing (Graph) API
directly:

- `list_accounts()` — `GET /me/adaccounts`
- `list_fields()`   — a *curated* catalogue per report type.
- `_run()`          — `GET /{account}/insights` for the metric report, or
                      `GET /{account}/{campaigns|adsets|ads}` for entity reports.

Meta differs from the Google sources:
  * requests are GET with query params, not POST bodies;
  * insights takes `level`, `breakdowns` and `time_increment` params — demographic
    and delivery splits (age, gender, country, platform) are `breakdowns`, while
    entity names (campaign_name, …) are ordinary `fields` that also set `level`;
  * the daily date split comes from `time_increment=1`, surfaced as `date_start`;
  * money (spend, cpc, cpm) is already in the account currency — no micros.
"""

from __future__ import annotations
import json
import logging
from typing import Any, Callable, Dict, List, Optional

from terno_dbi.connectors.api.model.base import ApiConnector
from terno_dbi.connectors.api.model.errors import ApiError, ErrorCode, invalid_field
from terno_dbi.connectors.api.model.types import Account, Field, QueryResult, QuerySpec
from terno_dbi.connectors.api.sources._multi import gather_accounts

logger = logging.getLogger(__name__)

_API_VERSION = "v25.0"
_BASE = f"https://graph.facebook.com/{_API_VERSION}"

# --- insights (the metric report) ------------------------------------------

_INSIGHTS_METRICS: List[Field] = [
    Field("impressions", "Impressions", "metric", "Times ads were shown.",
          data_type="integer"),
    Field("clicks", "Clicks", "metric", "All clicks.", data_type="integer"),
    Field("reach", "Reach", "metric", "People who saw the ads.",
          data_type="integer"),
    Field("spend", "Spend", "metric", "Amount spent, in account currency.",
          data_type="number", is_monetary=True),
    Field("cpc", "CPC", "metric", "Average cost per click.", data_type="number",
          is_monetary=True, is_non_aggregatable=True),
    Field("cpm", "CPM", "metric", "Cost per 1,000 impressions.",
          data_type="number", is_monetary=True, is_non_aggregatable=True),
    Field("ctr", "CTR", "metric", "Click-through rate.", data_type="number",
          is_non_aggregatable=True),
    Field("frequency", "Frequency", "metric", "Average impressions per person.",
          data_type="number", is_non_aggregatable=True),
]

# Entity-name fields select the insights `level`; each also comes back as a
# column. Ordered coarse->fine so the finest requested one wins.
_LEVEL_FIELDS: List[tuple] = [
    ("campaign_name", "campaign"),
    ("adset_name", "adset"),
    ("ad_name", "ad"),
]
_LEVEL_RANK = {name: i for i, (name, _lvl) in enumerate(_LEVEL_FIELDS)}
_LEVEL_OF = dict(_LEVEL_FIELDS)

# Demographic / delivery splits — passed as `breakdowns`, not `fields`.
_BREAKDOWNS: List[Field] = [
    Field("age", "Age", "dimension", "Age bracket of the audience."),
    Field("gender", "Gender", "dimension", "Gender of the audience."),
    Field("country", "Country", "dimension", "Country of the audience."),
    Field("publisher_platform", "Platform", "dimension",
          "Facebook, Instagram, Audience Network, Messenger."),
]

_INSIGHTS_DIMENSIONS: List[Field] = [
    Field("date", "Date", "dimension", "Day the stat occurred.",
          data_type="date"),
    Field("campaign_name", "Campaign", "dimension"),
    Field("adset_name", "Ad set", "dimension"),
    Field("ad_name", "Ad", "dimension"),
    *_BREAKDOWNS,
]
_BREAKDOWN_IDS = frozenset(f.id for f in _BREAKDOWNS)

# --- entity reports --------------------------------------------------------

_ENTITY_ENDPOINT: Dict[str, str] = {
    "Campaigns": "campaigns",
    "AdSets": "adsets",
    "Ads": "ads",
}
_ENTITY_FIELDS: Dict[str, List[Field]] = {
    "Campaigns": [
        Field("id", "Campaign ID", "dimension", data_type="string"),
        Field("name", "Campaign", "dimension"),
        Field("status", "Status", "dimension"),
        Field("objective", "Objective", "dimension"),
    ],
    "AdSets": [
        Field("id", "Ad set ID", "dimension", data_type="string"),
        Field("name", "Ad set", "dimension"),
        Field("status", "Status", "dimension"),
        Field("campaign_id", "Campaign ID", "dimension", data_type="string"),
        Field("optimization_goal", "Optimization goal", "dimension"),
    ],
    "Ads": [
        Field("id", "Ad ID", "dimension", data_type="string"),
        Field("name", "Ad", "dimension"),
        Field("status", "Status", "dimension"),
        Field("adset_id", "Ad set ID", "dimension", data_type="string"),
        Field("campaign_id", "Campaign ID", "dimension", data_type="string"),
    ],
}

_MONETARY = frozenset({"spend", "cpc", "cpm"})
_DEFAULT_REPORT = "Insights"


def _fields_for(report_type: Optional[str]) -> Dict[str, Field]:
    if report_type in _ENTITY_ENDPOINT:
        return {f.id: f for f in _ENTITY_FIELDS[report_type]}
    return {f.id: f for f in (*_INSIGHTS_DIMENSIONS, *_INSIGHTS_METRICS)}


def _default_http(method: str, url: str, token: str,
                  json_body: Optional[Dict] = None) -> Dict[str, Any]:
    import requests
    kwargs: Dict[str, Any] = {}
    # Meta calls are GET with query params; the connector passes those as the
    # "body" slot, and they go on the query string rather than a JSON body.
    if method == "GET":
        kwargs["params"] = json_body or {}
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
    """Internal marker for a 401 from Meta, mapped to AUTH_EXPIRED."""


class MetaAdsConnector(ApiConnector):
    def __init__(self, datasource, http: Optional[Callable] = None,
                 token_refresher: Optional[Callable] = None):
        super().__init__(datasource, token_refresher=token_refresher)
        self._http = http or _default_http

    # -- transport ----------------------------------------------------------

    def _call(self, method: str, url: str, params: Optional[Dict] = None) -> Dict[str, Any]:
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
            logger.warning("Meta Ads request failed: %s", exc)
            raise ApiError(
                ErrorCode.UPSTREAM_ERROR,
                "Meta returned an error. Try again.",
            )

    @staticmethod
    def _account_path(account_id: str) -> str:
        """Meta ad-account ids are 'act_<digits>'; accept a bare id too."""
        aid = str(account_id)
        return aid if aid.startswith("act_") else f"act_{aid}"

    # -- discovery ----------------------------------------------------------

    def list_accounts(self) -> List[Account]:
        data = self._call(
            "GET", f"{_BASE}/me/adaccounts",
            {"fields": "id,name,account_id,currency,timezone_name"})
        accounts: List[Account] = []
        for acc in data.get("data", []):
            acc_id = acc.get("id") or acc.get("account_id")
            if not acc_id:
                continue
            accounts.append(Account(
                id=acc_id,
                name=acc.get("name", acc_id),
                currency=acc.get("currency"),
                timezone=acc.get("timezone_name"),
            ))
        return accounts

    def list_fields(self, report_type: Optional[str] = None) -> List[Field]:
        return list(_fields_for(report_type).values())

    # -- query --------------------------------------------------------------

    def _run(self, spec: QuerySpec) -> QueryResult:
        report_type = spec.report_type or _DEFAULT_REPORT
        catalogue = _fields_for(report_type)

        unknown = [f for f in spec.fields if f not in catalogue]
        if unknown:
            raise invalid_field(unknown[0], list(catalogue.keys()))

        if report_type in _ENTITY_ENDPOINT:
            return self._run_entities(spec, report_type, catalogue)
        return self._run_insights(spec, catalogue)

    def _run_insights(self, spec: QuerySpec, catalogue) -> QueryResult:
        metrics = [f for f in spec.fields if catalogue[f].kind == "metric"]
        dims = [f for f in spec.fields if catalogue[f].kind == "dimension"]
        if not (metrics or dims):
            metrics = [m.id for m in _INSIGHTS_METRICS[:3]]

        breakdowns = [d for d in dims if d in _BREAKDOWN_IDS]
        level_fields = [d for d in dims if d in _LEVEL_OF]
        want_date = "date" in dims
        # Everything sent in `fields`: metrics plus entity-name columns. The date
        # split and breakdowns are separate params, not fields.
        api_fields = [*metrics, *level_fields]

        params: Dict[str, Any] = {
            "time_range": json.dumps(
                {"since": spec.date_range.start, "until": spec.date_range.end}),
            "level": self._insights_level(level_fields),
            "limit": spec.max_rows,
        }
        if api_fields:
            params["fields"] = ",".join(api_fields)
        if breakdowns:
            params["breakdowns"] = ",".join(breakdowns)
        if want_date:
            params["time_increment"] = 1

        multi = len(spec.accounts) > 1

        def fetch(account):
            url = f"{_BASE}/{self._account_path(account)}/insights"
            data = self._call("GET", url, params)
            return _parse_insights(data, spec.fields, catalogue, account,
                                   multi=multi)

        rows, warnings = gather_accounts(spec.accounts, fetch)

        return QueryResult(
            requested_field_ids=list(spec.fields) or api_fields,
            rows=rows,
            row_count=len(rows),
            warnings=warnings,
        )

    @staticmethod
    def _insights_level(level_fields) -> str:
        if not level_fields:
            return "account"
        # The finest requested entity wins (ad > adset > campaign).
        finest = max(level_fields, key=lambda f: _LEVEL_RANK[f])
        return _LEVEL_OF[finest]

    def _run_entities(self, spec: QuerySpec, report_type, catalogue) -> QueryResult:
        fields = spec.fields or [f.id for f in _ENTITY_FIELDS[report_type][:2]]
        endpoint = _ENTITY_ENDPOINT[report_type]
        params = {"fields": ",".join(fields), "limit": spec.max_rows}

        multi = len(spec.accounts) > 1

        def fetch(account):
            url = f"{_BASE}/{self._account_path(account)}/{endpoint}"
            data = self._call("GET", url, params)
            out = []
            for obj in data.get("data", []):
                record: Dict[str, Any] = {}
                if multi:
                    record["_account"] = account
                for name in fields:
                    record[name] = obj.get(name)
                out.append(record)
            return out

        rows, warnings = gather_accounts(spec.accounts, fetch)

        return QueryResult(
            requested_field_ids=list(fields),
            rows=rows,
            row_count=len(rows),
            warnings=warnings,
        )


def _parse_insights(data, requested_fields, catalogue, account, *,
                    multi=False) -> List[Dict[str, Any]]:
    """Map an insights response back to the requested field ids.

    Meta returns a flat object per row; the only remapping is the date field,
    which arrives as `date_start` under `time_increment=1`. Metric values come
    as strings and are coerced to numbers.
    """
    result: List[Dict[str, Any]] = []
    for row in data.get("data", []):
        record: Dict[str, Any] = {}
        if multi:
            record["_account"] = account
        for name in requested_fields:
            key = "date_start" if name == "date" else name
            record[name] = _coerce(name, row.get(key), catalogue)
        result.append(record)
    return result


def _coerce(field_id, raw, catalogue):
    if raw is None:
        return None
    field = catalogue.get(field_id)
    if field and field.kind == "metric":
        try:
            f = float(raw)
            # Money keeps decimals; counts collapse to int when whole.
            if field_id in _MONETARY:
                return f
            return int(f) if f.is_integer() else f
        except (TypeError, ValueError):
            return raw
    return raw


def make_meta_ads_connector(datasource) -> MetaAdsConnector:
    """Build a Meta Ads connector wired to refresh its own OAuth token when due."""
    from terno_dbi.connectors.api.auth.oauth import make_ensure_token
    return MetaAdsConnector(
        datasource, token_refresher=make_ensure_token(datasource))


__all__ = ["MetaAdsConnector", "make_meta_ads_connector"]
