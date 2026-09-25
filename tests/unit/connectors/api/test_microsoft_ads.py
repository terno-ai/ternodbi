"""The Microsoft Advertising connector, against mocked REST v13 responses.

The mock returns the real shapes from `AccountsInfo/Query`, `User/Query` and the
`GenerateReport` Submit/Poll pair, and a genuine ZIP-of-CSV for the download, so
the submit body, the poll loop, ZIP unpacking, CSV coercion and client-side
sorting are all exercised without a live provider or a developer token.
"""

import csv
import io
import zipfile

import pytest

from terno_dbi.connectors.api.model.errors import ApiError, ErrorCode
from terno_dbi.connectors.api.model.types import DateRange, QuerySpec
from terno_dbi.connectors.api.sources.microsoft_ads import MicrosoftAdsConnector


class _Catalog:
    key = "microsoft_ads"
    report_types = [
        {"id": "Campaign", "settings": []},
        {"id": "Keyword", "settings": []},
        {"id": "SearchTerm", "settings": []},
    ]
    has_report_types = True


class _DS:
    type = "microsoft_ads"
    catalog = _Catalog()
    connection_json = {"ACCESS_TOKEN": "tok"}


USER = {"User": {"Id": 42, "CustomerId": 9988}}

ACCOUNTS = {
    "AccountsInfo": [
        {"Id": 1112223333, "Name": "Acme Search", "Number": "X0001",
         "AccountLifeCycleStatus": "Active"},
        {"Id": 4445556666, "Name": "Acme Shopping", "Number": "X0002",
         "AccountLifeCycleStatus": "Active"},
    ],
}

# Deliberately out of date order, with Microsoft's human formatting: thousands
# separators, a percentage suffix, and "--" for an unavailable cell.
CSV_ROWS = [
    ["TimePeriod", "CampaignName", "Impressions", "Clicks", "Spend", "Ctr"],
    ["2026-08-02", "Generic", "1,200", "10", "3.00", "0.83%"],
    ["2026-08-01", "Brand", "5000", "40", "12.50", "5.26%"],
    ["2026-08-03", "Dormant", "0", "0", "--", "--"],
]


def _csv_text(rows=None):
    buf = io.StringIO()
    csv.writer(buf).writerows(rows or CSV_ROWS)
    return buf.getvalue()


def _zipped(rows=None) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as archive:
        archive.writestr("report.csv", _csv_text(rows))
    return buf.getvalue()


class _Clock:
    """A fake monotonic clock that only advances when the code sleeps."""

    def __init__(self):
        self.t = 0.0
        self.slept = []

    def now(self):
        return self.t

    def sleep(self, seconds):
        self.slept.append(seconds)
        self.t += seconds


def _http(routes, captured=None):
    """Route by URL fragment. A list value is consumed one call at a time."""
    def http(method, url, token, body=None, headers=None):
        assert token == "tok"
        for needle, response in routes.items():
            if needle in url:
                if captured is not None:
                    captured.setdefault(needle, []).append(
                        {"body": body, "headers": headers or {}})
                if isinstance(response, list):
                    return response.pop(0) if len(response) > 1 else response[0]
                if isinstance(response, Exception):
                    raise response
                return response
        raise AssertionError(f"unexpected call: {method} {url}")
    return http


def _connector(routes, captured=None, download=None, clock=None):
    clock = clock or _Clock()
    return MicrosoftAdsConnector(
        _DS(),
        http=_http(routes, captured),
        download=download or (lambda url: _zipped()),
        sleep=clock.sleep,
        now=clock.now,
    )


_UNSET = object()


def _report_routes(poll=_UNSET, submit=_UNSET):
    """Routes for a full submit/poll/download cycle.

    `poll` and `submit` default via a sentinel, not `or`, so a test can pass an
    empty response to model a provider that answered with nothing.
    """
    return {
        "User/Query": USER,
        "AccountsInfo/Query": ACCOUNTS,
        "GenerateReport/Submit": (
            {"ReportRequestId": "rr-1"} if submit is _UNSET else submit),
        "GenerateReport/Poll": (
            {"ReportRequestStatus": {
                "Status": "Success",
                "ReportDownloadUrl": "https://download.example/report.zip",
            }} if poll is _UNSET else poll),
    }


def _spec(fields=("TimePeriod", "CampaignName", "Impressions", "Clicks",
                  "Spend", "Ctr"),
          accounts=("1112223333",), report_type="Campaign", max_rows=1000):
    return QuerySpec(
        accounts=list(accounts), fields=list(fields),
        date_range=DateRange("2026-08-01", "2026-08-31"),
        report_type=report_type, max_rows=max_rows,
    )


class TestListAccounts:
    def test_maps_accounts_info(self):
        conn = _connector({"User/Query": USER, "AccountsInfo/Query": ACCOUNTS})
        accounts = conn.list_accounts()
        assert {a.id for a in accounts} == {"1112223333", "4445556666"}
        assert accounts[0].name == "Acme Search"
        assert accounts[0].extra["number"] == "X0001"

    def test_currency_is_unknown_because_microsoft_omits_it(self):
        # The dispatch mixed-currency guard relies on this being honest rather
        # than guessed; see `_currency_notes` for what stands in for it.
        conn = _connector({"User/Query": USER, "AccountsInfo/Query": ACCOUNTS})
        assert all(a.currency is None for a in conn.list_accounts())

    def test_customer_id_header_is_fetched_once_and_reused(self):
        captured = {}
        conn = _connector(
            {"User/Query": USER, "AccountsInfo/Query": ACCOUNTS}, captured)
        conn.list_accounts()
        conn.list_accounts()
        # One User/Query for two list_accounts calls; the id is cached.
        assert len(captured["User/Query"]) == 1
        assert captured["AccountsInfo/Query"][0]["headers"]["CustomerId"] == "9988"


class TestListFields:
    def test_fields_are_scoped_to_the_report_type(self):
        conn = _connector({})
        campaign = {f.id for f in conn.list_fields("Campaign")}
        keyword = {f.id for f in conn.list_fields("Keyword")}
        assert "CampaignName" in campaign
        assert "Keyword" in keyword and "Keyword" not in campaign
        assert "Impressions" in campaign and "Impressions" in keyword

    def test_search_term_report_has_no_currency_column(self):
        # Microsoft's search-query report genuinely does not offer CurrencyCode;
        # advertising it would produce a provider error at query time.
        conn = _connector({})
        assert "CurrencyCode" not in {f.id for f in conn.list_fields("SearchTerm")}
        assert "CurrencyCode" in {f.id for f in conn.list_fields("Campaign")}

    def test_money_and_ratio_flags(self):
        conn = _connector({})
        by_id = {f.id: f for f in conn.list_fields("Campaign")}
        assert by_id["Spend"].is_monetary is True
        assert by_id["Ctr"].is_non_aggregatable is True
        assert by_id["AverageCpc"].is_monetary is True
        assert by_id["Clicks"].is_non_aggregatable is False

    def test_unknown_report_type_falls_back_to_default(self):
        conn = _connector({})
        assert "CampaignName" in {f.id for f in conn.list_fields("NotAReport")}


class TestSubmit:
    def test_submit_body_carries_type_columns_scope_and_dates(self):
        captured = {}
        conn = _connector(_report_routes(), captured)
        conn.query(_spec())

        body = captured["GenerateReport/Submit"][0]["body"]["ReportRequest"]
        # REST uses "Type", not the SOAP-JSON "__type".
        assert body["Type"] == "CampaignPerformanceReportRequest"
        assert body["Format"] == "Csv"
        assert body["ExcludeColumnHeaders"] is False   # headers map the cells
        assert body["ExcludeReportHeader"] is True
        assert body["Columns"] == ["TimePeriod", "CampaignName", "Impressions",
                                   "Clicks", "Spend", "Ctr"]
        assert body["Scope"]["AccountIds"] == [1112223333]   # ints, not strings
        assert body["Time"]["CustomDateRangeStart"] == {
            "Year": 2026, "Month": 8, "Day": 1}
        assert body["Time"]["CustomDateRangeEnd"] == {
            "Year": 2026, "Month": 8, "Day": 31}
        # An IANA zone cannot be mapped to Microsoft's enum, so none is sent.
        assert "ReportTimeZone" not in body["Time"]

    def test_time_period_selects_daily_aggregation(self):
        captured = {}
        conn = _connector(_report_routes(), captured)
        conn.query(_spec())
        body = captured["GenerateReport/Submit"][0]["body"]["ReportRequest"]
        assert body["Aggregation"] == "Daily"

    def test_no_date_field_selects_summary_aggregation(self):
        captured = {}
        conn = _connector(_report_routes(), captured)
        conn.query(_spec(fields=("CampaignName", "Clicks")))
        body = captured["GenerateReport/Submit"][0]["body"]["ReportRequest"]
        assert body["Aggregation"] == "Summary"

    def test_report_type_selects_the_request_type(self):
        captured = {}
        conn = _connector(_report_routes(), captured)
        conn.query(_spec(fields=("Keyword", "Clicks"), report_type="Keyword"))
        body = captured["GenerateReport/Submit"][0]["body"]["ReportRequest"]
        assert body["Type"] == "KeywordPerformanceReportRequest"

    def test_missing_request_id_is_an_error(self):
        conn = _connector(_report_routes(submit={}))
        with pytest.raises(ApiError) as exc:
            conn.query(_spec())
        assert exc.value.code == ErrorCode.UPSTREAM_ERROR

    def test_non_numeric_account_is_rejected_clearly(self):
        conn = _connector(_report_routes())
        with pytest.raises(ApiError) as exc:
            conn.query(_spec(accounts=("customers/abc",)))
        assert exc.value.retriable is False
        assert "list_accounts" in exc.value.message


class TestPoll:
    def test_pending_then_success_sleeps_and_retries(self):
        clock = _Clock()
        routes = _report_routes(poll=[
            {"ReportRequestStatus": {"Status": "Pending"}},
            {"ReportRequestStatus": {
                "Status": "Success",
                "ReportDownloadUrl": "https://download.example/report.zip"}},
        ])
        conn = _connector(routes, clock=clock)
        result = conn.query(_spec())
        assert clock.slept == [2.0]          # backed off once before retrying
        assert result.row_count == 3

    def test_error_status_is_surfaced_as_retriable(self):
        conn = _connector(_report_routes(
            poll={"ReportRequestStatus": {"Status": "Error"}}))
        with pytest.raises(ApiError) as exc:
            conn.query(_spec())
        assert exc.value.code == ErrorCode.UPSTREAM_ERROR
        assert exc.value.retriable is True

    def test_forever_pending_times_out_rather_than_hanging(self):
        clock = _Clock()
        conn = _connector(
            _report_routes(poll={"ReportRequestStatus": {"Status": "Pending"}}),
            clock=clock)
        with pytest.raises(ApiError) as exc:
            conn.query(_spec())
        assert exc.value.code == ErrorCode.TIMEOUT
        assert exc.value.retriable is True
        # It gave up inside the budget instead of spinning forever.
        assert clock.t <= 60.0

    def test_backoff_is_capped(self):
        clock = _Clock()
        conn = _connector(
            _report_routes(poll={"ReportRequestStatus": {"Status": "Pending"}}),
            clock=clock)
        with pytest.raises(ApiError):
            conn.query(_spec())
        assert max(clock.slept) <= 10.0

    def test_success_with_no_download_url_is_an_empty_result(self):
        # How Microsoft returns a report that matched nothing.
        conn = _connector(_report_routes(
            poll={"ReportRequestStatus": {"Status": "Success",
                                          "ReportDownloadUrl": ""}}))
        result = conn.query(_spec())
        assert result.row_count == 0
        assert result.rows == []


class TestDownloadAndParse:
    def test_unzips_and_coerces_cells(self):
        conn = _connector(_report_routes())
        result = conn.query(_spec())
        rows = result.rows
        assert len(rows) == 3
        brand = rows[0]
        assert brand["TimePeriod"] == "2026-08-01"
        assert brand["CampaignName"] == "Brand"
        assert brand["Impressions"] == 5000
        assert brand["Clicks"] == 40
        assert brand["Spend"] == 12.5
        assert brand["Ctr"] == 5.26            # "5.26%" -> 5.26

    def test_thousands_separator_is_stripped(self):
        conn = _connector(_report_routes())
        rows = conn.query(_spec()).rows
        generic = next(r for r in rows if r["CampaignName"] == "Generic")
        assert generic["Impressions"] == 1200   # "1,200"

    def test_unavailable_cells_become_none(self):
        conn = _connector(_report_routes())
        rows = conn.query(_spec()).rows
        dormant = next(r for r in rows if r["CampaignName"] == "Dormant")
        assert dormant["Spend"] is None         # "--"
        assert dormant["Ctr"] is None

    def test_corrupt_archive_becomes_an_api_error(self):
        # The download bypasses the `_call` guard, so it needs its own.
        def bad_download(url):
            return b"PK\x03\x04 truncated"

        conn = _connector(_report_routes(), download=bad_download)
        with pytest.raises(ApiError) as exc:
            conn.query(_spec())
        assert exc.value.code == ErrorCode.UPSTREAM_ERROR
        assert exc.value.retriable is True

    def test_transport_failure_during_download_becomes_an_api_error(self):
        def boom(url):
            raise OSError("connection reset")

        conn = _connector(_report_routes(), download=boom)
        with pytest.raises(ApiError) as exc:
            conn.query(_spec())
        assert exc.value.code == ErrorCode.UPSTREAM_ERROR

    def test_plain_csv_download_is_tolerated(self):
        conn = _connector(_report_routes(),
                          download=lambda url: _csv_text().encode("utf-8"))
        assert conn.query(_spec()).row_count == 3

    def test_only_requested_fields_are_returned(self):
        conn = _connector(_report_routes())
        result = conn.query(_spec(fields=("CampaignName", "Clicks")))
        assert set(result.rows[0]) == {"CampaignName", "Clicks"}
        assert result.requested_field_ids == ["CampaignName", "Clicks"]


class TestSorting:
    def test_time_series_sorts_by_date_ascending(self):
        conn = _connector(_report_routes())
        rows = conn.query(_spec()).rows
        assert [r["TimePeriod"] for r in rows] == [
            "2026-08-01", "2026-08-02", "2026-08-03"]

    def test_breakdown_sorts_by_first_metric_descending(self):
        conn = _connector(_report_routes())
        rows = conn.query(_spec(fields=("CampaignName", "Clicks"))).rows
        assert [r["Clicks"] for r in rows] == [40, 10, 0]

    def test_truncation_keeps_the_top_rows_and_warns(self):
        # Microsoft cannot limit rows server-side, so max_rows is applied after
        # sorting — otherwise the kept rows would be an arbitrary sample.
        conn = _connector(_report_routes())
        result = conn.query(_spec(fields=("CampaignName", "Clicks"), max_rows=2))
        assert result.row_count == 2
        assert [r["Clicks"] for r in result.rows] == [40, 10]
        assert any("only the first 2" in w for w in result.warnings)


class TestMultiAccount:
    def _multi_csv(self):
        return [
            ["AccountId", "CampaignName", "Clicks"],
            ["1112223333", "Brand", "40"],
            ["4445556666", "Shopping", "10"],
        ]

    def test_account_column_is_added_for_tagging_but_not_returned(self):
        captured = {}
        conn = _connector(_report_routes(), captured,
                          download=lambda url: _zipped(self._multi_csv()))
        result = conn.query(_spec(fields=("CampaignName", "Clicks"),
                                  accounts=("1112223333", "4445556666")))

        body = captured["GenerateReport/Submit"][0]["body"]["ReportRequest"]
        assert body["Scope"]["AccountIds"] == [1112223333, 4445556666]
        assert "AccountId" in body["Columns"]      # fetched, for attribution
        # ...but it is not silently added to the caller's columns.
        assert set(result.rows[0]) == {"CampaignName", "Clicks", "_account"}
        assert {r["_account"] for r in result.rows} == {"1112223333", "4445556666"}

    def test_single_account_is_not_tagged(self):
        conn = _connector(_report_routes())
        rows = conn.query(_spec(fields=("CampaignName", "Clicks"))).rows
        assert all("_account" not in r for r in rows)

    def test_all_accounts_share_one_submit(self):
        captured = {}
        conn = _connector(_report_routes(), captured,
                          download=lambda url: _zipped(self._multi_csv()))
        conn.query(_spec(fields=("CampaignName", "Clicks"),
                         accounts=("1112223333", "4445556666")))
        # One report job for both accounts, not a per-account fan-out.
        assert len(captured["GenerateReport/Submit"]) == 1


class TestCurrencyNotes:
    def test_multi_account_money_warns_about_unknown_currency(self):
        conn = _connector(_report_routes(),
                          download=lambda url: _zipped(
                              [["AccountId", "Spend"], ["1112223333", "5.00"]]))
        result = conn.query(_spec(fields=("Spend",),
                                  accounts=("1112223333", "4445556666")))
        assert any("CurrencyCode" in n for n in result.notes)

    def test_no_note_when_currency_was_requested(self):
        conn = _connector(_report_routes(),
                          download=lambda url: _zipped(
                              [["AccountId", "CurrencyCode", "Spend"],
                               ["1112223333", "USD", "5.00"]]))
        result = conn.query(_spec(fields=("CurrencyCode", "Spend"),
                                  accounts=("1112223333", "4445556666")))
        assert result.notes == []

    def test_no_note_for_a_single_account(self):
        conn = _connector(_report_routes())
        result = conn.query(_spec(fields=("CampaignName", "Spend")))
        assert result.notes == []

    def test_search_term_note_admits_there_is_no_currency_column(self):
        conn = _connector(_report_routes(),
                          download=lambda url: _zipped(
                              [["AccountId", "Spend"], ["1112223333", "5.00"]]))
        result = conn.query(_spec(fields=("Spend",), report_type="SearchTerm",
                                  accounts=("1112223333", "4445556666")))
        assert any("cannot return a currency column" in n for n in result.notes)


class TestFieldValidation:
    def test_unknown_field_is_rejected_with_a_suggestion(self):
        conn = _connector(_report_routes())
        with pytest.raises(ApiError) as exc:
            conn.query(_spec(fields=("CampaignName", "Clickz")))
        assert exc.value.code == ErrorCode.INVALID_FIELD
        assert "Clicks" in exc.value.message

    def test_currency_on_search_term_is_rejected(self):
        conn = _connector(_report_routes())
        with pytest.raises(ApiError) as exc:
            conn.query(_spec(fields=("SearchQuery", "CurrencyCode"),
                             report_type="SearchTerm"))
        assert exc.value.code == ErrorCode.INVALID_FIELD


class TestAuthMapping:
    def test_401_becomes_auth_expired(self):
        from terno_dbi.connectors.api.sources.microsoft_ads import _AuthError

        def http(method, url, token, body=None, headers=None):
            raise _AuthError()

        conn = MicrosoftAdsConnector(_DS(), http=http)
        with pytest.raises(ApiError) as exc:
            conn.list_accounts()
        assert exc.value.code == ErrorCode.AUTH_EXPIRED


class TestErrorSurfacing:
    class _Resp:
        def __init__(self, status, payload=None, text=""):
            self.status_code = status
            self._payload = payload
            self.text = text

        def json(self):
            if self._payload is None:
                raise ValueError("no json")
            return self._payload

    def test_named_error_code_is_surfaced_and_non_retriable(self):
        from terno_dbi.connectors.api.sources.microsoft_ads import _ms_error
        err = _ms_error(self._Resp(400, {"Errors": [{
            "Code": 1001, "ErrorCode": "CampaignServiceInvalidAccountId",
            "Message": "The account id is not valid."}]}))
        assert err.code == ErrorCode.UPSTREAM_ERROR
        assert err.retriable is False
        assert "CampaignServiceInvalidAccountId" in err.message
        assert "not valid" in err.message

    def test_credential_error_routes_to_reconnect(self):
        # Microsoft can report an expired token as a 400 with a named code
        # rather than a 401; it must still become AUTH_EXPIRED.
        from terno_dbi.connectors.api.sources.microsoft_ads import _ms_error
        err = _ms_error(self._Resp(400, {"Errors": [{
            "Code": 105, "ErrorCode": "AuthenticationTokenExpired",
            "Message": "The authentication token has expired."}]}))
        assert err.code == ErrorCode.AUTH_EXPIRED

    def test_operation_errors_are_read_too(self):
        from terno_dbi.connectors.api.sources.microsoft_ads import _ms_error
        err = _ms_error(self._Resp(400, {"OperationErrors": [{
            "ErrorCode": "UserIsNotAuthorized", "Message": "No access."}]}))
        assert err.code == ErrorCode.AUTH_EXPIRED

    def test_server_errors_stay_retriable(self):
        from terno_dbi.connectors.api.sources.microsoft_ads import _ms_error
        err = _ms_error(self._Resp(503, {"Message": "backend"}))
        assert err.retriable is True

    def test_non_json_body_does_not_explode(self):
        from terno_dbi.connectors.api.sources.microsoft_ads import _ms_error
        err = _ms_error(self._Resp(502, None, text="<html>Bad Gateway</html>"))
        assert err.retriable is True
        assert "Bad Gateway" in err.message

    def test_missing_developer_token_is_a_clear_config_error(self, monkeypatch):
        from terno_dbi.connectors.api.sources import microsoft_ads as ms
        monkeypatch.delenv(ms._DEVELOPER_TOKEN_ENV, raising=False)
        with pytest.raises(ApiError) as exc:
            ms._default_http("POST", "https://x", "tok")
        assert exc.value.retriable is False
        assert ms._DEVELOPER_TOKEN_ENV in exc.value.message


class TestCatalogAndRegistration:
    def test_microsoft_ads_is_registered_at_startup(self):
        from terno_dbi.connectors.api import registry
        assert registry.is_supported("microsoft_ads")

    def test_registered_factory_binds_a_token_refresher(self):
        from terno_dbi.connectors.api.sources.microsoft_ads import (
            make_microsoft_ads_connector,
        )
        conn = make_microsoft_ads_connector(_DS())
        assert conn._token_refresher is not None

    def test_declared_report_types_match_the_connector(self):
        # A catalog report type with no request type would pass validation and
        # then silently run as a Campaign report.
        from terno_dbi.catalog.declarations import get_spec
        from terno_dbi.connectors.api.sources.microsoft_ads import _REQUEST_TYPE
        spec = get_spec("microsoft_ads")
        assert {r.id for r in spec.report_types} == set(_REQUEST_TYPE)
        assert spec.default_report_type in _REQUEST_TYPE

    def test_oauth_provider_requests_offline_access(self):
        # Without offline_access Microsoft issues no refresh token and the
        # connection dies at the first expiry.
        from terno_dbi.connectors.api.auth.providers import get_provider
        provider = get_provider("microsoft_ads")
        assert provider is not None
        assert "offline_access" in provider.scope
        assert "msads.manage" in provider.scope
