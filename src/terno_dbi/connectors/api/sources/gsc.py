"""Google Search Console connector.

Implements the `ApiConnector` interface against the Search Console API v3
directly:

- `list_accounts()` — `GET /sites` (the verified sites the credential can see)
- `list_fields()` — a *static* catalogue; unlike GA4, Search Console exposes a
  fixed set of dimensions and metrics with no metadata endpoint to discover.
- `_run()` — `POST /sites/{siteUrl}/searchAnalytics/query`
"""

from __future__ import annotations
import logging
from typing import Any, Callable, Dict, List, Optional
from urllib.parse import quote
from terno_dbi.connectors.api.model.base import ApiConnector
from terno_dbi.connectors.api.model.errors import ApiError, ErrorCode, invalid_field
from terno_dbi.connectors.api.model.types import Account, Field, QueryResult, QuerySpec
from terno_dbi.connectors.api.sources._multi import gather_accounts

logger = logging.getLogger(__name__)

_BASE = "https://searchconsole.googleapis.com/webmasters/v3"


_DIMENSIONS: List[Field] = [
    Field("date", "Date", "dimension", "The day the search occurred.",
          data_type="date"),
    Field("query", "Query", "dimension",
          "The search query users typed to reach the site."),
    Field("page", "Page", "dimension", "The landing-page URL from search."),
    Field("country", "Country", "dimension",
          "Country of the searcher (ISO-3166-1 alpha-3)."),
    Field("device", "Device", "dimension",
          "Device class: DESKTOP, MOBILE or TABLET."),
    Field("searchAppearance", "Search appearance", "dimension",
          "Rich-result feature the page appeared as (e.g. AMP, review snippet)."),
]

_METRICS: List[Field] = [
    Field("clicks", "Clicks", "metric",
          "Number of clicks from search results.", data_type="integer"),
    Field("impressions", "Impressions", "metric",
          "Number of times a result was shown.", data_type="integer"),
    Field("ctr", "CTR", "metric",
          "Click-through rate (clicks / impressions).", data_type="number",
          is_non_aggregatable=True),
    Field("position", "Average position", "metric",
          "Average ranking position (1 is best).", data_type="number",
          is_non_aggregatable=True),
]

_DIMENSION_IDS = frozenset(f.id for f in _DIMENSIONS)
_METRIC_IDS = frozenset(f.id for f in _METRICS)
_ALL_FIELDS: Dict[str, Field] = {f.id: f for f in (*_DIMENSIONS, *_METRICS)}


def _default_http(method: str, url: str, token: str,
                  json_body: Optional[Dict] = None) -> Dict[str, Any]:
    import requests
    resp = requests.request(
        method, url,
        headers={"Authorization": f"Bearer {token}"},
        json=json_body, timeout=30,
    )
    if resp.status_code == 401:
        raise _AuthError()
    resp.raise_for_status()
    return resp.json()


class _AuthError(Exception):
    """Internal marker for a 401 from Google, mapped to AUTH_EXPIRED."""


class GSCConnector(ApiConnector):
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
            logger.warning("GSC request failed: %s", exc)
            raise ApiError(
                ErrorCode.UPSTREAM_ERROR,
                "Google Search Console returned an error. Try again.",
            )

    # -- discovery ----------------------------------------------------------

    def list_accounts(self) -> List[Account]:
        data = self._call("GET", f"{_BASE}/sites")
        accounts: List[Account] = []
        for site in data.get("siteEntry", []):
            site_url = site.get("siteUrl")
            if not site_url:
                continue
            # Filter out sites the credential can see but not query. Search
            # Console lists these with permissionLevel 'siteUnverifiedUser'.
            level = site.get("permissionLevel", "")
            if level == "siteUnverifiedUser":
                continue
            accounts.append(Account(
                id=site_url,
                name=site_url,
                extra={"permission_level": level} if level else {},
            ))
        return accounts

    def list_fields(self, report_type: Optional[str] = None) -> List[Field]:
        return list(_ALL_FIELDS.values())

    # -- query --------------------------------------------------------------

    @staticmethod
    def _site_path(site_url: str) -> str:
        """URL-encode a siteUrl for the request path.

        Both URL-prefix properties ('https://example.com/') and domain
        properties ('sc-domain:example.com') must be fully percent-encoded,
        including the slashes and the colon, or the API 404s.
        """
        return quote(str(site_url), safe="")

    def _run(self, spec: QuerySpec) -> QueryResult:
        unknown = [f for f in spec.fields if f not in _ALL_FIELDS]
        if unknown:
            raise invalid_field(unknown[0], list(_ALL_FIELDS.keys()))

        dimensions = [f for f in spec.fields if f in _DIMENSION_IDS]
        metrics = [f for f in spec.fields if f in _METRIC_IDS]
        # The API always returns all four metrics; when the caller named none,
        # surface them all rather than an empty result.
        if not metrics:
            metrics = [f.id for f in _METRICS]

        body: Dict[str, Any] = {
            "startDate": spec.date_range.start,
            "endDate": spec.date_range.end,
            "dimensions": dimensions,
            "rowLimit": spec.max_rows,
        }

        multi = len(spec.accounts) > 1

        def fetch(account):
            url = f"{_BASE}/sites/{self._site_path(account)}/searchAnalytics/query"
            data = self._call("POST", url, body)
            return _parse_rows(data, dimensions, metrics, account, multi=multi)

        # Partial success: one site the credential cannot query must not sink a
        # multi-site query.
        rows, warnings = gather_accounts(spec.accounts, fetch)

        return QueryResult(
            requested_field_ids=list(spec.fields) or [*dimensions, *metrics],
            rows=rows,
            row_count=len(rows),
            warnings=warnings,
        )


def _parse_rows(data, dimensions, metrics, account, *, multi=False) -> List[Dict[str, Any]]:
    """Map a Search Analytics response back to field ids.

    Each row carries `keys` (dimension values, in the order requested) plus the
    four metrics as top-level numbers. Alignment of keys is by index, matching
    the request — the same positional contract GA4 uses.
    """
    result: List[Dict[str, Any]] = []
    for row in data.get("rows", []):
        record: Dict[str, Any] = {}
        if multi:
            record["_account"] = account
        keys = row.get("keys", [])
        for i, name in enumerate(dimensions):
            record[name] = keys[i] if i < len(keys) else None
        for name in metrics:
            record[name] = _coerce_number(row.get(name))
        result.append(record)
    return result


def _coerce_number(raw):
    if raw is None:
        return None
    try:
        f = float(raw)
        return int(f) if f.is_integer() else f
    except (TypeError, ValueError):
        return raw


def make_gsc_connector(datasource) -> GSCConnector:
    """Build a GSC connector wired to refresh its own OAuth token when due.

    The refresher is bound here (not in the model layer) so `access_token()`
    keeps the token fresh on every provider call, whatever the caller.
    """
    from terno_dbi.connectors.api.auth.oauth import make_ensure_token
    return GSCConnector(datasource, token_refresher=make_ensure_token(datasource))


__all__ = ["GSCConnector", "make_gsc_connector"]
