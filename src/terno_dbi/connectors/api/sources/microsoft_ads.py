"""Microsoft Advertising (Bing Ads) connector.

Implements the `ApiConnector` interface against the Microsoft Advertising
**REST** API v13 — not SOAP. Microsoft froze SOAP feature work on 1 Oct 2026 and
schedules full deprecation for 31 Jan 2027, so a new integration targets REST
only. The REST services keep the SOAP operation names, as URL paths:

- `list_accounts()` — `POST /CustomerManagement/v13/AccountsInfo/Query`
- `list_fields()`   — a *curated* catalogue per report type. Each Microsoft
  report exposes 100+ columns; a hand-picked subset is what an agent actually
  needs, and keeps discovery legible.
- `_run()`          — the three-step reporting dance:
  `POST /Reporting/v13/GenerateReport/Submit` -> `ReportRequestId`,
  `POST /Reporting/v13/GenerateReport/Poll` until `Success`,
  then download the `ReportDownloadUrl`, which is a **ZIP of one CSV**.

Microsoft differs from the other ad connectors in ways that shape this module:

  * **Reporting is asynchronous.** There is no synchronous query endpoint, so
    `_run` submits and polls. `data_query` is already job-based, so the blocking
    poll lives inside the job — but the default executor is synchronous, so the
    budget is deliberately modest (see `_poll_timeout`).
  * **One report covers every account.** `Scope.AccountIds` takes a list, so all
    requested accounts go in a single submit/poll cycle rather than the
    per-account `gather_accounts` fan-out the other sources use. With report
    generation dominating latency, N sequential cycles would be far slower than
    one. The trade-off is that partial success is lost: an inaccessible account
    fails the whole report, where Google Ads would return the rest plus a
    warning.
  * **No LIMIT and no ORDER BY.** The provider returns the full result set in
    its own order, so `max_rows` is applied here — after sorting, so truncation
    keeps the top rows rather than an arbitrary slice.
  * **Percentages and money are formatted strings.** `Ctr` arrives as "5.26%",
    and unavailable cells as "--"; both are normalised on parse.
  * **Account currency is not discoverable.** `AccountsInfo/Query` returns no
    currency, so `Account.currency` stays `None` and the dispatch layer's
    mixed-currency guard cannot fire. A note on the result covers the gap.
  * Requires a developer token header in addition to the OAuth bearer.
"""

from __future__ import annotations
import csv
import io
import logging
import os
import time
import zipfile
from typing import Any, Callable, Dict, List, Optional

from terno_dbi.connectors.api.model.base import ApiConnector
from terno_dbi.connectors.api.model.errors import ApiError, ErrorCode, invalid_field
from terno_dbi.connectors.api.model.types import Account, Field, QueryResult, QuerySpec

logger = logging.getLogger(__name__)

_CM_BASE = "https://clientcenter.api.bingads.microsoft.com/CustomerManagement/v13"
_REPORTING_BASE = "https://reporting.api.bingads.microsoft.com/Reporting/v13"
_DEVELOPER_TOKEN_ENV = "TERNO_MICROSOFT_ADS_DEVELOPER_TOKEN"

# report type -> the polymorphic `Type` discriminator Microsoft expects. REST
# uses "Type"; "__type" is the older WCF/SOAP-JSON spelling and is rejected.
_REQUEST_TYPE: Dict[str, str] = {
    "Campaign": "CampaignPerformanceReportRequest",
    "AdGroup": "AdGroupPerformanceReportRequest",
    "Keyword": "KeywordPerformanceReportRequest",
    "SearchTerm": "SearchQueryPerformanceReportRequest",
}
_DEFAULT_REPORT = "Campaign"

_TIME_PERIOD = "TimePeriod"
_ACCOUNT_ID = "AccountId"
_CURRENCY_CODE = "CurrencyCode"

# Poll budget. Kept modest because `SynchronousExecutor` runs the job inline in
# the request; a deployment with a background executor can raise it.
_POLL_TIMEOUT_SETTING = "TERNO_MICROSOFT_ADS_POLL_TIMEOUT"
_DEFAULT_POLL_TIMEOUT = 60.0
_POLL_INITIAL_DELAY = 2.0
_POLL_MAX_DELAY = 10.0

# Cells Microsoft uses for "not applicable"; normalised to None.
_NULL_CELLS = frozenset({"", "--", "N/A"})


_SHARED_METRICS: List[Field] = [
    Field("Impressions", "Impressions", "metric",
          "Times an ad was shown.", data_type="integer"),
    Field("Clicks", "Clicks", "metric", "Ad clicks.", data_type="integer"),
    Field("Spend", "Spend", "metric",
          "Cost for the row, in the account's currency.", data_type="number",
          is_monetary=True),
    Field("Conversions", "Conversions", "metric",
          "Attributed conversions.", data_type="number"),
    Field("Revenue", "Revenue", "metric",
          "Revenue attributed to conversions.", data_type="number",
          is_monetary=True),
    Field("Ctr", "CTR", "metric",
          "Click-through rate. Microsoft delivers this as a percentage, so "
          "5.26 means 5.26%.", data_type="number", is_non_aggregatable=True),
    Field("AverageCpc", "Avg. CPC", "metric",
          "Average cost per click.", data_type="number",
          is_monetary=True, is_non_aggregatable=True),
]

_SHARED_SEGMENTS: List[Field] = [
    Field(_TIME_PERIOD, "Date", "dimension",
          "Day the stat occurred. Requesting it switches the report to daily "
          "aggregation; omit it for one summary row per group.",
          data_type="date"),
    Field("DeviceType", "Device", "dimension",
          "Device class: Computer, Smartphone, Tablet."),
]

_ACCOUNT_FIELDS: List[Field] = [
    Field(_ACCOUNT_ID, "Account ID", "dimension",
          "Microsoft Advertising account id.", data_type="string"),
    Field("AccountName", "Account", "dimension",
          "Microsoft Advertising account name."),
]

# Not offered by the search-term report; added to the others only.
_CURRENCY_FIELD = Field(
    _CURRENCY_CODE, "Currency", "dimension",
    "ISO code of the currency the row's monetary values are in.")

_REPORT_DIMENSIONS: Dict[str, List[Field]] = {
    "Campaign": [
        Field("CampaignId", "Campaign ID", "dimension", data_type="string"),
        Field("CampaignName", "Campaign", "dimension"),
        Field("CampaignStatus", "Campaign status", "dimension"),
        Field("CampaignType", "Campaign type", "dimension"),
    ],
    "AdGroup": [
        Field("AdGroupId", "Ad group ID", "dimension", data_type="string"),
        Field("AdGroupName", "Ad group", "dimension"),
        # Microsoft names this plain "Status" on the ad group report only.
        Field("Status", "Ad group status", "dimension"),
        Field("CampaignName", "Campaign", "dimension"),
    ],
    "Keyword": [
        Field("Keyword", "Keyword", "dimension"),
        Field("KeywordId", "Keyword ID", "dimension", data_type="string"),
        Field("KeywordStatus", "Keyword status", "dimension"),
        Field("BidMatchType", "Match type", "dimension"),
        Field("AdGroupName", "Ad group", "dimension"),
        Field("CampaignName", "Campaign", "dimension"),
    ],
    "SearchTerm": [
        Field("SearchQuery", "Search term", "dimension"),
        Field("Keyword", "Keyword", "dimension"),
        Field("AdGroupName", "Ad group", "dimension"),
        Field("CampaignName", "Campaign", "dimension"),
    ],
}


def _fields_for(report_type: Optional[str]) -> Dict[str, Field]:
    rt = report_type if report_type in _REQUEST_TYPE else _DEFAULT_REPORT
    fields = [*_REPORT_DIMENSIONS[rt], *_SHARED_SEGMENTS, *_ACCOUNT_FIELDS]
    if rt != "SearchTerm":
        fields.append(_CURRENCY_FIELD)
    fields.extend(_SHARED_METRICS)
    return {f.id: f for f in fields}


def _poll_timeout() -> float:
    """The poll budget, overridable per deployment."""
    try:
        from django.conf import settings
        value = getattr(settings, _POLL_TIMEOUT_SETTING, None)
    except Exception:   # noqa: BLE001 - usable outside Django too
        value = None
    if value is None:
        value = os.environ.get(_POLL_TIMEOUT_SETTING)
    try:
        return float(value) if value is not None else _DEFAULT_POLL_TIMEOUT
    except (TypeError, ValueError):
        return _DEFAULT_POLL_TIMEOUT


def _developer_token() -> str:
    token = os.getenv(_DEVELOPER_TOKEN_ENV, "").strip()
    if not token:
        raise ApiError(
            ErrorCode.UPSTREAM_ERROR,
            f"Microsoft Advertising is not configured on the server: "
            f"{_DEVELOPER_TOKEN_ENV} is unset. Set the developer token and "
            f"restart.",
            retriable=False,
        )
    return token


def _default_http(method: str, url: str, token: str,
                  json_body: Optional[Dict] = None,
                  headers: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    import requests

    all_headers = {
        "Authorization": f"Bearer {token}",
        "DeveloperToken": _developer_token(),
        "Content-Type": "application/json",
    }
    all_headers.update(headers or {})
    resp = requests.request(method, url, headers=all_headers,
                            json=json_body, timeout=30)
    if resp.status_code == 401:
        raise _AuthError()
    if resp.status_code >= 400:
        raise _ms_error(resp)
    if not resp.content:
        return {}
    try:
        return resp.json()
    except ValueError:
        raise ApiError(
            ErrorCode.UPSTREAM_ERROR,
            "Microsoft Advertising returned a non-JSON response.",
        )


def _default_download(url: str) -> bytes:
    import requests

    resp = requests.get(url, timeout=120)
    if resp.status_code >= 400:
        raise ApiError(
            ErrorCode.UPSTREAM_ERROR,
            f"Could not download the Microsoft Advertising report "
            f"({resp.status_code}).",
            retriable=True,
        )
    return resp.content


def _ms_error(resp) -> ApiError:
    """Turn a Microsoft Advertising REST error into an actionable `ApiError`.

    Microsoft returns `{"Errors": [{Code, ErrorCode, Message, Details}]}` (or
    `OperationErrors` for batch operations). Surfacing the named `ErrorCode` is
    what lets an operator see e.g. `InvalidCredentials` or
    `UserIsNotAuthorized` rather than a generic failure. Only 429/5xx are
    retriable; a 4xx is a config or permission problem retrying will not fix.
    """
    status = resp.status_code
    code_name, message = "", ""
    try:
        payload = resp.json() or {}
    except ValueError:
        payload = {}
        message = (resp.text or "")[:200]

    errors = payload.get("Errors") or payload.get("OperationErrors") or []
    if isinstance(errors, list) and errors:
        first = errors[0] if isinstance(errors[0], dict) else {}
        code_name = str(first.get("ErrorCode") or first.get("Code") or "")
        message = first.get("Message") or first.get("Details") or message
    elif payload.get("Message"):
        message = payload["Message"]

    # An expired or rejected token can arrive as a 400 with a named code rather
    # than a 401, and must still route the user to a reconnect.
    if any(marker in code_name for marker in
           ("AuthenticationToken", "InvalidCredentials", "UserIsNotAuthorized")):
        return ApiError(
            ErrorCode.AUTH_EXPIRED,
            f"Microsoft Advertising rejected the credentials ({code_name}): "
            f"{message or 'reconnect the source.'}",
        )

    label = f"{status} {code_name}".strip()
    return ApiError(
        ErrorCode.UPSTREAM_ERROR,
        f"Microsoft Advertising API error ({label}): "
        f"{message or 'unknown error'}",
        retriable=status == 429 or status >= 500,
    )


class _AuthError(Exception):
    """Internal marker for a 401 from Microsoft, mapped to AUTH_EXPIRED."""


_UNSET = object()


class MicrosoftAdsConnector(ApiConnector):
    def __init__(self, datasource, http: Optional[Callable] = None,
                 download: Optional[Callable] = None,
                 sleep: Optional[Callable] = None,
                 now: Optional[Callable] = None,
                 token_refresher: Optional[Callable] = None):
        super().__init__(datasource, token_refresher=token_refresher)
        self._http = http or _default_http
        self._download = download or _default_download
        self._sleep = sleep or time.sleep
        self._now = now or time.monotonic
        self._customer_id_cache: Any = _UNSET

    # -- transport ----------------------------------------------------------

    def _call(self, method: str, url: str, body: Optional[Dict] = None, *,
              account_id: Optional[str] = None,
              with_customer: bool = True) -> Dict[str, Any]:
        headers: Dict[str, str] = {}
        if with_customer:
            customer_id = self._customer()
            if customer_id:
                headers["CustomerId"] = str(customer_id)
        if account_id:
            headers["CustomerAccountId"] = str(account_id)
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
            logger.warning("Microsoft Advertising request failed: %s", exc)
            raise ApiError(
                ErrorCode.UPSTREAM_ERROR,
                "Microsoft Advertising returned an error. Try again.",
            )

    def _customer(self):
        """The manager-account (customer) id for the `CustomerId` header.

        Microsoft wants it on essentially every call, but nothing in the OAuth
        response carries it, so it is read once from `User/Query` and cached for
        the connector's lifetime. That call must not ask for the header it is
        fetching, hence `with_customer=False`.
        """
        if self._customer_id_cache is _UNSET:
            data = self._call("POST", f"{_CM_BASE}/User/Query",
                              {"UserId": None}, with_customer=False)
            user = data.get("User") or {}
            self._customer_id_cache = user.get("CustomerId")
        return self._customer_id_cache

    # -- discovery ----------------------------------------------------------

    def list_accounts(self) -> List[Account]:
        """Ad accounts reachable with this connection.

        `currency` is left unset: `AccountsInfo/Query` does not return it, and
        resolving it would cost one `Account/Query` per account. Query the
        `CurrencyCode` column instead when combining spend across accounts.
        """
        data = self._call("POST", f"{_CM_BASE}/AccountsInfo/Query",
                          {"OnlyParentAccounts": False})
        accounts: List[Account] = []
        for info in data.get("AccountsInfo") or []:
            account_id = info.get("Id")
            if account_id is None:
                continue
            extra = {}
            if info.get("Number"):
                extra["number"] = info["Number"]
            if info.get("AccountLifeCycleStatus"):
                extra["status"] = info["AccountLifeCycleStatus"]
            accounts.append(Account(
                id=str(account_id),
                name=info.get("Name") or str(account_id),
                extra=extra,
            ))
        return accounts

    def list_fields(self, report_type: Optional[str] = None) -> List[Field]:
        return list(_fields_for(report_type).values())

    # -- query --------------------------------------------------------------

    def _run(self, spec: QuerySpec) -> QueryResult:
        report_type = (spec.report_type if spec.report_type in _REQUEST_TYPE
                       else _DEFAULT_REPORT)
        catalogue = _fields_for(report_type)

        unknown = [f for f in spec.fields if f not in catalogue]
        if unknown:
            raise invalid_field(unknown[0], list(catalogue.keys()))

        requested = list(spec.fields)
        if not requested:
            # A report with no selected columns is a dead end; default to the
            # report's core metrics.
            requested = [m.id for m in _SHARED_METRICS[:3]]

        multi = len(spec.accounts) > 1
        columns = list(requested)
        if multi and _ACCOUNT_ID not in columns:
            # Rows from several accounts are indistinguishable otherwise. The
            # column is fetched for tagging only and is not added to the output.
            columns.append(_ACCOUNT_ID)

        account_ids = _account_ids(spec.accounts)
        aggregation = "Daily" if _TIME_PERIOD in columns else "Summary"

        request_id = self._submit(report_type, columns, spec,
                                  account_ids, aggregation)
        download_url = self._poll(request_id)

        rows: List[Dict[str, Any]] = []
        if download_url:
            rows = self._download_rows(download_url, requested, catalogue,
                                       tag_account=multi)

        rows = _sort_rows(rows, requested, catalogue)

        warnings: List[str] = []
        if len(rows) > spec.max_rows:
            warnings.append(
                f"The report returned {len(rows)} rows; only the first "
                f"{spec.max_rows} are included. Microsoft Advertising cannot "
                f"limit rows server-side, so these are the top rows after "
                f"sorting. Narrow the date range or add filters for the rest."
            )
            rows = rows[:spec.max_rows]

        notes = _currency_notes(requested, catalogue, report_type, multi)

        return QueryResult(
            requested_field_ids=requested,
            rows=rows,
            row_count=len(rows),
            notes=notes,
            warnings=warnings,
        )

    # -- reporting steps ----------------------------------------------------

    def _submit(self, report_type: str, columns: List[str], spec: QuerySpec,
                account_ids: List[int], aggregation: str) -> str:
        """Submit the report request and return its id.

        `ReportTimeZone` is deliberately omitted: Microsoft takes its own
        enumeration of zone names, not IANA ones like `spec.timezone`, and a
        wrong guess would silently shift every date. Omitting it makes Microsoft
        use the account's own time zone.
        """
        body = {
            "ReportRequest": {
                "Type": _REQUEST_TYPE[report_type],
                "Format": "Csv",
                "FormatVersion": "2.0",
                "ReportName": f"terno-{report_type}",
                "ReturnOnlyCompleteData": False,
                "ExcludeReportHeader": True,
                "ExcludeReportFooter": True,
                # The column-header row is what maps cells back to field ids.
                "ExcludeColumnHeaders": False,
                "Aggregation": aggregation,
                "Columns": columns,
                "Scope": {"AccountIds": account_ids},
                "Time": {
                    "CustomDateRangeStart": _ymd(spec.date_range.start),
                    "CustomDateRangeEnd": _ymd(spec.date_range.end),
                },
            },
        }
        data = self._call("POST", f"{_REPORTING_BASE}/GenerateReport/Submit",
                          body, account_id=str(account_ids[0]))
        request_id = data.get("ReportRequestId")
        if not request_id:
            raise ApiError(
                ErrorCode.UPSTREAM_ERROR,
                "Microsoft Advertising accepted the report request but "
                "returned no request id.",
            )
        return request_id

    def _poll(self, request_id: str) -> Optional[str]:
        """Poll until the report is ready; return its download URL.

        Returns `None` when Microsoft reports success with no download URL,
        which is how a report with zero matching rows comes back.
        """
        deadline = self._now() + _poll_timeout()
        delay = _POLL_INITIAL_DELAY
        while True:
            data = self._call("POST", f"{_REPORTING_BASE}/GenerateReport/Poll",
                              {"ReportRequestId": request_id})
            status_obj = data.get("ReportRequestStatus") or {}
            status = status_obj.get("Status")

            if status == "Success":
                return status_obj.get("ReportDownloadUrl") or None
            if status == "Error":
                raise ApiError(
                    ErrorCode.UPSTREAM_ERROR,
                    "Microsoft Advertising failed to generate the report. "
                    "Submit the query again.",
                    retriable=True,
                )
            if self._now() + delay >= deadline:
                raise ApiError(
                    ErrorCode.TIMEOUT,
                    "Microsoft Advertising is still generating the report. "
                    "Large date ranges can take a while — run the query again, "
                    "or narrow the range.",
                    retriable=True,
                )
            self._sleep(delay)
            delay = min(delay * 2, _POLL_MAX_DELAY)

    def _download_rows(self, url: str, requested: List[str],
                       catalogue: Dict[str, Field], *,
                       tag_account: bool) -> List[Dict[str, Any]]:
        # The download does not go through `_call`, so it needs its own guard:
        # a transport failure or a truncated archive must surface as an
        # `ApiError` with a stable code, never as a raw exception.
        try:
            raw = self._download(url)
            text = _unpack(raw)
        except ApiError:
            raise
        except Exception as exc:   # noqa: BLE001
            logger.warning("Microsoft Advertising report download failed: %s", exc)
            raise ApiError(
                ErrorCode.UPSTREAM_ERROR,
                "The Microsoft Advertising report could not be downloaded. "
                "Run the query again.",
                retriable=True,
            )
        reader = csv.reader(io.StringIO(text))

        header: Optional[List[str]] = None
        rows: List[Dict[str, Any]] = []
        for cells in reader:
            if not cells or not any(c.strip() for c in cells):
                continue
            if header is None:
                header = [c.strip() for c in cells]
                continue
            by_name = dict(zip(header, cells))
            record: Dict[str, Any] = {}
            if tag_account:
                record["_account"] = (by_name.get(_ACCOUNT_ID) or "").strip()
            for field_id in requested:
                record[field_id] = _coerce(
                    field_id, by_name.get(field_id), catalogue)
            rows.append(record)
        return rows


def _account_ids(accounts: List[str]) -> List[int]:
    """Microsoft's `Scope.AccountIds` takes numeric ids; reject anything else."""
    ids: List[int] = []
    for account in accounts:
        try:
            ids.append(int(str(account).strip()))
        except (TypeError, ValueError):
            raise ApiError(
                ErrorCode.UPSTREAM_ERROR,
                f"{account!r} is not a Microsoft Advertising account id. Use "
                f"the numeric ids returned by list_accounts.",
                retriable=False,
            )
    if not ids:
        raise ApiError(
            ErrorCode.UPSTREAM_ERROR,
            "No account was selected. Pass at least one account id from "
            "list_accounts.",
            retriable=False,
        )
    return ids


def _ymd(value: str) -> Dict[str, int]:
    """'2026-08-01' -> {'Year': 2026, 'Month': 8, 'Day': 1}."""
    try:
        year, month, day = (int(part) for part in value.split("-"))
    except (AttributeError, TypeError, ValueError):
        raise ApiError(
            ErrorCode.UPSTREAM_ERROR,
            f"{value!r} is not a YYYY-MM-DD date.",
            retriable=False,
        )
    return {"Year": year, "Month": month, "Day": day}


def _unpack(raw: bytes) -> str:
    """The report bytes as CSV text.

    Microsoft compresses the download, but tolerate a plain CSV body too rather
    than failing on a shape that is already readable.
    """
    if raw[:2] == b"PK":
        with zipfile.ZipFile(io.BytesIO(raw)) as archive:
            names = [n for n in archive.namelist() if not n.endswith("/")]
            if not names:
                return ""
            raw = archive.read(names[0])
    return raw.decode("utf-8-sig", errors="replace")


def _sort_rows(rows: List[Dict[str, Any]], requested: List[str],
               catalogue: Dict[str, Field]) -> List[Dict[str, Any]]:
    """Order rows the way the sibling connectors do.

    Microsoft supports no ORDER BY, and `max_rows` is applied here, so an
    unsorted result would make truncation an arbitrary sample. A time series
    sorts ascending by date; anything else sorts by its first metric, largest
    first.
    """
    dates = [f for f in requested
             if f in catalogue and catalogue[f].data_type == "date"]
    if dates:
        key = dates[0]
        return sorted(rows, key=lambda r: (r.get(key) is None, r.get(key) or ""))

    metrics = [f for f in requested
               if f in catalogue and catalogue[f].kind == "metric"]
    if not metrics:
        return rows
    key = metrics[0]

    def sort_value(row):
        value = row.get(key)
        return value if isinstance(value, (int, float)) else float("-inf")

    return sorted(rows, key=sort_value, reverse=True)


def _currency_notes(requested: List[str], catalogue: Dict[str, Field],
                    report_type: str, multi: bool) -> List[str]:
    """Warn when money is combined across accounts of unknown currency.

    The dispatch layer refuses to mix currencies, but only when it knows each
    account's currency — and Microsoft does not report it. This note is what
    stands in for that guard.
    """
    if not multi:
        return []
    monetary = [catalogue[f].name for f in requested
                if f in catalogue and catalogue[f].is_monetary]
    if not monetary or _CURRENCY_CODE in requested:
        return []
    if report_type == "SearchTerm":
        return [
            "This result combines " + ", ".join(monetary) + " across several "
            "accounts, which may not share a currency. The search-term report "
            "cannot return a currency column, so check the accounts' "
            "currencies before totalling these values."
        ]
    return [
        "This result combines " + ", ".join(monetary) + " across several "
        "accounts, which may not share a currency. Microsoft Advertising does "
        "not report an account's currency, so add the 'CurrencyCode' field to "
        "confirm before totalling these values."
    ]


def _coerce(field_id: str, raw: Any, catalogue: Dict[str, Field]):
    """One CSV cell as its typed value.

    Microsoft formats metrics for humans: percentages carry a '%', large numbers
    may carry thousands separators, and unavailable cells are '--'.
    """
    if raw is None:
        return None
    text = str(raw).strip()
    if text in _NULL_CELLS:
        return None

    field = catalogue.get(field_id)
    if field is None or field.kind != "metric":
        return text

    cleaned = text.rstrip("%").replace(",", "").strip()
    try:
        number = float(cleaned)
    except (TypeError, ValueError):
        return text
    return int(number) if number.is_integer() else number


def make_microsoft_ads_connector(datasource) -> MicrosoftAdsConnector:
    """Build a Microsoft Ads connector wired to refresh its own OAuth token."""
    from terno_dbi.connectors.api.auth.oauth import make_ensure_token
    return MicrosoftAdsConnector(
        datasource, token_refresher=make_ensure_token(datasource))


__all__ = ["MicrosoftAdsConnector", "make_microsoft_ads_connector"]
