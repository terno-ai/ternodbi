"""The Pipedrive connector, against mocked API v1/v2 responses.

The mock returns the real response shapes from `users/me`, the v2 field
metadata and list endpoints (cursor-paged), v1 `leads` (offset-paged) and the
lookups behind the name columns, so discovery, date-range paging, name
resolution, value flattening and error mapping are exercised without a live
provider — the same approach as the HubSpot and Zoho CRM tests.
"""

import pytest

from terno_dbi.connectors.api.model.errors import ApiError, ErrorCode
from terno_dbi.connectors.api.model.types import DateRange, QuerySpec
from terno_dbi.connectors.api.sources import pipedrive
from terno_dbi.connectors.api.sources.pipedrive import (
    PipedriveConnector,
    _AuthError,
    _date_of,
    _date_range,
    _default_http,
)

HOST = "https://acme.pipedrive.com"
REPORTS = ("Deals", "Leads", "Persons", "Organizations", "Activities")

# 40-character hashes, as Pipedrive keys custom fields.
SEATS = "a" * 40
TIER = "b" * 40
ADDONS = "c" * 40
MARGIN = "d" * 40
CHAMPION = "e" * 40
HQ = "f" * 40


class _Catalog:
    key = "pipedrive"
    report_types = [
        {"id": r, "settings": [{"setting_id": "date_field", "required": False}]}
        for r in REPORTS
    ]
    has_report_types = True


class _DS:
    type = "pipedrive"
    catalog = _Catalog()

    def __init__(self, api_domain=HOST, scopes=None):
        self.connection_json = {"ACCESS_TOKEN": "tok", "API_DOMAIN": api_domain}
        if scopes is not None:
            self.connection_json["GRANTED_SCOPES"] = scopes


# --- canned Pipedrive responses --------------------------------------------

ME = {"success": True, "data": {
    "id": 11, "name": "Ada Lovelace", "company_id": 7001,
    "company_name": "Acme Ltd", "company_domain": "acme",
    "default_currency": "EUR", "timezone_name": "Europe/Berlin",
}}

DEAL_FIELDS = {"success": True, "data": [
    # Standard fields are curated; only custom ones are discovered.
    {"field_code": "title", "field_name": "Title", "field_type": "varchar",
     "is_custom_field": False},
    {"field_code": SEATS, "field_name": "Seats", "field_type": "int",
     "is_custom_field": True},
    {"field_code": TIER, "field_name": "Tier", "field_type": "enum",
     "is_custom_field": True,
     "options": [{"id": 3, "label": "Gold"}, {"id": 4, "label": "Silver"}]},
    {"field_code": ADDONS, "field_name": "Add-ons", "field_type": "set",
     "is_custom_field": True,
     "options": [{"id": 1, "label": "SSO"}, {"id": 2, "label": "Audit"}]},
    {"field_code": MARGIN, "field_name": "Margin", "field_type": "monetary",
     "is_custom_field": True},
    {"field_code": CHAMPION, "field_name": "Champion", "field_type": "user",
     "is_custom_field": True},
    {"field_code": HQ, "field_name": "HQ address", "field_type": "address",
     "is_custom_field": True},
], "additional_data": {"next_cursor": None}}

USERS = {"success": True, "data": [
    {"id": 11, "name": "Ada Lovelace", "email": "ada@acme.com"},
    {"id": 12, "name": "", "email": "alan@acme.com"},
]}
STAGES = {"success": True, "data": [{"id": 1, "name": "Qualified"},
                                    {"id": 2, "name": "Negotiations"}],
          "additional_data": {"next_cursor": None}}
PIPELINES = {"success": True, "data": [{"id": 1, "name": "Sales"}],
             "additional_data": {"next_cursor": None}}
ORGS = {"success": True, "data": [{"id": 501, "name": "Globex"}],
        "additional_data": {"next_cursor": None}}
PERSONS = {"success": True, "data": [{"id": 601, "name": "Hank Scorpio"}],
           "additional_data": {"next_cursor": None}}


def _deal(did, add_time, **kw):
    deal = {"id": did, "title": f"Deal {did}", "status": "open", "owner_id": 11,
            "stage_id": 1, "pipeline_id": 1, "org_id": 501, "person_id": 601,
            "value": 1000, "currency": "EUR", "add_time": add_time,
            "update_time": add_time, "custom_fields": {}}
    deal.update(kw)
    return deal


def _v2(items, cursor=None):
    return {"success": True, "data": items,
            "additional_data": {"next_cursor": cursor}}


# Newest first, as the connector asks for: one after the range, two in it, one
# before it (where paging stops).
DEALS = _v2([
    _deal(4, "2026-09-02T09:00:00Z"),
    _deal(3, "2026-08-31T23:59:59Z", value=250.5, stage_id=2, custom_fields={
        SEATS: 12, TIER: {"id": 3, "label": "Gold"},
        ADDONS: [{"id": 1, "label": "SSO"}, {"id": 2, "label": "Audit"}],
        MARGIN: {"value": 90, "currency": "EUR"}, CHAMPION: 12,
        HQ: {"value": "1 Main St, Springfield", "locality": "Springfield"}}),
    _deal(2, "2026-08-01T00:00:00Z"),
    _deal(1, "2026-07-31T23:59:59Z"),
])


def _router(routes, calls=None):
    """An injected `http` that answers by (method, url fragment)."""
    def http(method, url, token, body=None):
        assert token == "tok"
        if calls is not None:
            calls.append((method, url, dict(body) if body else body))
        for (m, needle), response in routes.items():
            if method == m and url.endswith(needle):
                return response(body) if callable(response) else response
        raise AssertionError(f"unexpected call: {method} {url}")
    return http


def _connector(routes, calls=None, **ds_kwargs):
    return PipedriveConnector(_DS(**ds_kwargs), http=_router(routes, calls))


def _deal_routes(deals=DEALS):
    return {
        ("GET", "/api/v2/dealFields"): DEAL_FIELDS,
        ("GET", "/api/v2/deals"): deals,
        ("GET", "/api/v1/users"): USERS,
        ("GET", "/api/v2/stages"): STAGES,
        ("GET", "/api/v2/pipelines"): PIPELINES,
        ("GET", "/api/v2/organizations"): ORGS,
        ("GET", "/api/v2/persons"): PERSONS,
    }


def _spec(fields=("title", "value"), report_type="Deals",
          start="2026-08-01", end="2026-08-31", **kw):
    return QuerySpec(
        accounts=["7001"], fields=list(fields),
        date_range=DateRange(start, end), report_type=report_type, **kw,
    )


def _lists(calls, path):
    return [c for c in calls if c[1].endswith(path)]


class TestDates:
    def test_range_is_parsed_to_dates(self):
        from datetime import date
        assert _date_range("2026-08-01", "2026-08-31") == (
            date(2026, 8, 1), date(2026, 8, 31))

    @pytest.mark.parametrize("start", ["today", "2026-08-01T00:00", ""])
    def test_non_dates_are_rejected(self, start):
        with pytest.raises(ApiError) as exc:
            _date_range(start, "2026-08-31")
        assert exc.value.code == ErrorCode.INVALID_FILTER
        assert "get_today" in exc.value.message

    def test_reversed_range_is_rejected(self):
        with pytest.raises(ApiError) as exc:
            _date_range("2026-09-01", "2026-08-01")
        assert "after" in exc.value.message

    @pytest.mark.parametrize("raw, expected", [
        ("2026-08-01T10:20:00Z", "2026-08-01"),            # v2 RFC 3339
        ("2026-08-01T10:20:00.000Z", "2026-08-01"),        # v1 ISO 8601
        ("2026-08-01 23:30:00", "2026-08-01"),             # v1 legacy
        ("2026-08-01T23:30:00-02:00", "2026-08-02"),       # read in UTC
        ("2026-08-01", "2026-08-01"),                      # a plain date
        (None, None), ("", None), ("soon", None),
    ])
    def test_date_of_reads_every_format_pipedrive_uses(self, raw, expected):
        got = _date_of(raw)
        assert (got.isoformat() if got else None) == expected


class TestListAccounts:
    def test_returns_the_company_the_token_belongs_to(self):
        calls = []
        conn = _connector({("GET", "/api/v1/users/me"): ME}, calls)
        [account] = conn.list_accounts()
        assert account.id == "7001"
        assert account.name == "Acme Ltd"
        assert account.extra == {"company_domain": "acme", "default_currency": "EUR"}
        # Deals carry their own currencies, so the account claims none.
        assert account.currency is None
        assert calls[0][1] == f"{HOST}/api/v1/users/me"


class TestApiDomain:
    @pytest.mark.parametrize("domain", [
        "", "https://evil.example.com", "http://acme.pipedrive.com",
        "https://acme.pipedrive.com.evil.io", "https://pipedrive.com",
        "https://acme.pipedrive.com@evil.io", "https://a.b.pipedrive.com",
    ])
    def test_unrecognised_domain_never_receives_the_token(self, domain):
        calls = []
        conn = _connector({("GET", "/api/v1/users/me"): ME}, calls,
                          api_domain=domain)
        with pytest.raises(ApiError) as exc:
            conn.list_accounts()
        assert exc.value.code == ErrorCode.AUTH_EXPIRED
        assert calls == []

    def test_a_trailing_slash_is_tolerated(self):
        calls = []
        conn = _connector({("GET", "/api/v1/users/me"): ME}, calls,
                          api_domain=f"{HOST}/")
        conn.list_accounts()
        assert calls[0][1] == f"{HOST}/api/v1/users/me"


class TestListFields:
    def test_curated_fields_plus_discovered_custom_ones(self):
        conn = _connector({("GET", "/api/v2/dealFields"): DEAL_FIELDS})
        fields = {f.id: f for f in conn.list_fields("Deals")}
        assert {"title", "value", "stage", "owner", "add_time"} <= set(fields)
        assert fields["value"].is_monetary is True
        assert fields["probability"].is_non_aggregatable is True
        assert fields[SEATS].kind == "metric"
        assert fields[SEATS].data_type == "integer"
        assert fields[SEATS].group == "Custom fields"
        assert fields[MARGIN].is_monetary is True
        assert fields[TIER].kind == "dimension"
        assert fields[TIER].name == "Tier"

    def test_standard_fields_in_the_metadata_are_not_duplicated(self):
        conn = _connector({("GET", "/api/v2/dealFields"): DEAL_FIELDS})
        ids = [f.id for f in conn.list_fields("Deals")]
        assert ids.count("title") == 1

    def test_field_metadata_follows_the_cursor(self):
        pages = [
            _v2([{"field_code": SEATS, "field_name": "Seats", "field_type": "int",
                  "is_custom_field": True}], cursor="next"),
            _v2([{"field_code": TIER, "field_name": "Tier", "field_type": "enum",
                  "is_custom_field": True}]),
        ]
        calls = []
        conn = _connector({("GET", "/api/v2/dealFields"): lambda b: pages.pop(0)},
                          calls)
        ids = {f.id for f in conn.list_fields("Deals")}
        assert {SEATS, TIER} <= ids
        assert calls[1][2]["cursor"] == "next"

    def test_discovery_failure_falls_back_to_the_curated_catalogue(self):
        def boom(body):
            raise ApiError(ErrorCode.UPSTREAM_ERROR, "down")

        conn = _connector({("GET", "/api/v2/dealFields"): boom})
        ids = {f.id for f in conn.list_fields("Deals")}
        assert {"title", "value", "stage", "owner"} <= ids
        assert SEATS not in ids

    def test_catalogue_is_fetched_once_per_report(self):
        calls = []
        conn = _connector({("GET", "/api/v2/dealFields"): DEAL_FIELDS}, calls)
        conn.list_fields("Deals")
        conn.list_fields("Deals")
        assert len(calls) == 1

    @pytest.mark.parametrize("report", ["Leads", "Activities"])
    def test_leads_and_activities_are_curated_without_a_call(self, report):
        calls = []
        conn = _connector({}, calls)
        assert conn.list_fields(report)
        assert calls == []

    def test_unknown_report_type_uses_deals(self):
        calls = []
        conn = _connector({("GET", "/api/v2/dealFields"): DEAL_FIELDS}, calls)
        ids = {f.id for f in conn.list_fields("Nope")}
        assert "stage" in ids
        assert calls[0][1].endswith("/api/v2/dealFields")


class TestRunDeals:
    def test_reads_newest_first_and_stops_past_the_range(self):
        calls = []
        conn = _connector(_deal_routes(), calls)
        result = conn.query(_spec(fields=("id",)))
        # Deal 4 is after the range, deal 1 before it.
        assert [r["id"] for r in result.rows] == [3, 2]
        [(_, url, params)] = _lists(calls, "/api/v2/deals")
        assert url == f"{HOST}/api/v2/deals"
        assert params["sort_by"] == "add_time"
        assert params["sort_direction"] == "desc"
        assert params["limit"] == 500
        assert params["include_option_labels"] == "true"
        assert "updated_since" not in params
        assert any("'add_time'" in n and "(UTC)" in n for n in result.notes)

    def test_paging_follows_the_cursor_until_the_range_is_passed(self):
        pages = [
            _v2([_deal(3, "2026-08-20T10:00:00Z")], cursor="c2"),
            _v2([_deal(2, "2026-08-10T10:00:00Z")], cursor="c3"),
            _v2([_deal(1, "2026-07-01T10:00:00Z")], cursor="c4"),
        ]
        calls = []
        conn = _connector(_deal_routes(deals=lambda b: pages.pop(0)), calls)
        result = conn.query(_spec(fields=("id",)))
        assert [r["id"] for r in result.rows] == [3, 2]
        cursors = [p.get("cursor") for _, _, p in _lists(calls, "/api/v2/deals")]
        assert cursors == [None, "c2", "c3"]   # never asked for c4
        assert result.warnings == []

    def test_names_are_resolved_for_the_reference_columns(self):
        conn = _connector(_deal_routes())
        result = conn.query(_spec(fields=(
            "id", "owner", "stage", "pipeline", "organization", "person")))
        assert result.rows[0] == {
            "id": 3, "owner": "Ada Lovelace", "stage": "Negotiations",
            "pipeline": "Sales", "organization": "Globex",
            "person": "Hank Scorpio"}

    def test_person_and_org_names_are_fetched_by_id(self):
        calls = []
        conn = _connector(_deal_routes(), calls)
        conn.query(_spec(fields=("organization", "person")))
        [(_, _, org_params)] = _lists(calls, "/api/v2/organizations")
        assert org_params["ids"] == "501"
        [(_, _, person_params)] = _lists(calls, "/api/v2/persons")
        assert person_params["ids"] == "601"

    def test_ids_are_fetched_in_batches_of_a_hundred(self):
        deals = _v2([_deal(i, "2026-08-10T10:00:00Z", org_id=1000 + i)
                     for i in range(150)])
        calls = []
        conn = _connector(_deal_routes(deals=deals), calls)
        conn.query(_spec(fields=("organization",)))
        batches = [p["ids"].split(",") for _, _, p in _lists(calls, "/api/v2/organizations")]
        assert [len(b) for b in batches] == [100, 50]

    def test_no_lookups_without_a_name_column(self):
        calls = []
        conn = _connector(_deal_routes(), calls)
        conn.query(_spec(fields=("id", "owner_id", "stage_id")))
        assert [c[1].rsplit("/", 1)[-1] for c in calls] == ["dealFields", "deals"]

    def test_a_failed_lookup_degrades_to_the_id(self):
        def boom(body):
            raise ApiError(ErrorCode.UPSTREAM_ERROR, "no users:read")

        routes = _deal_routes()
        routes[("GET", "/api/v1/users")] = boom
        conn = _connector(routes)
        result = conn.query(_spec(fields=("owner", "stage")))
        assert result.rows[0] == {"owner": 11, "stage": "Negotiations"}

    def test_a_user_with_no_name_falls_back_to_the_email(self):
        deals = _v2([_deal(3, "2026-08-10T10:00:00Z", owner_id=12)])
        conn = _connector(_deal_routes(deals=deals))
        result = conn.query(_spec(fields=("owner",)))
        assert result.rows[0]["owner"] == "alan@acme.com"

    def test_custom_values_are_flattened(self):
        conn = _connector(_deal_routes())
        result = conn.query(_spec(fields=(SEATS, TIER, ADDONS, MARGIN, CHAMPION, HQ)))
        assert result.rows[0] == {
            SEATS: 12, TIER: "Gold", ADDONS: "SSO, Audit", MARGIN: 90.0,
            CHAMPION: "alan@acme.com", HQ: "1 Main St, Springfield"}
        assert result.rows[1] == {SEATS: None, TIER: None, ADDONS: None,
                                  MARGIN: None, CHAMPION: None, HQ: None}

    def test_requested_custom_fields_narrow_the_response(self):
        calls = []
        conn = _connector(_deal_routes(), calls)
        conn.query(_spec(fields=("title", SEATS, TIER)))
        [(_, _, params)] = _lists(calls, "/api/v2/deals")
        assert params["custom_fields"] == f"{SEATS},{TIER}"

    def test_more_than_fifteen_custom_fields_are_read_whole(self):
        many = _v2([{"field_code": f"{i:040d}", "field_name": f"F{i}",
                     "field_type": "varchar", "is_custom_field": True}
                    for i in range(16)])
        routes = _deal_routes()
        routes[("GET", "/api/v2/dealFields")] = many
        calls = []
        conn = _connector(routes, calls)
        conn.query(_spec(fields=[f"{i:040d}" for i in range(16)]))
        [(_, _, params)] = _lists(calls, "/api/v2/deals")
        assert "custom_fields" not in params

    def test_no_fields_selects_the_curated_defaults(self):
        conn = _connector(_deal_routes())
        result = conn.query(_spec(fields=()))
        ids = result.requested_field_ids
        assert {"id", "title", "status", "stage", "owner", "value",
                "currency", "add_time", "won_time"} <= set(ids)
        # Raw ids and custom fields are available but not selected by default.
        assert not {"owner_id", "stage_id", "update_time", SEATS} & set(ids)

    def test_repeated_fields_are_selected_once(self):
        conn = _connector(_deal_routes())
        result = conn.query(_spec(fields=("title", "value", "title")))
        assert result.requested_field_ids == ["title", "value"]

    def test_unknown_field_is_rejected_with_a_suggestion(self):
        calls = []
        conn = _connector(_deal_routes(), calls)
        with pytest.raises(ApiError) as exc:
            conn.query(_spec(fields=("title", "valeu")))
        assert exc.value.code == ErrorCode.INVALID_FIELD
        assert "value" in exc.value.message
        assert not _lists(calls, "/api/v2/deals")

    def test_truncation_at_max_rows_is_warned(self):
        conn = _connector(_deal_routes())
        result = conn.query(_spec(fields=("id",), max_rows=1))
        assert [r["id"] for r in result.rows] == [3]
        assert "max_rows" in result.warnings[0]

    def test_the_scan_is_capped(self, monkeypatch):
        monkeypatch.setattr(pipedrive, "_MAX_SCAN", 3)
        pages = iter([_v2([_deal(i, "2026-09-10T00:00:00Z") for i in range(2)],
                          cursor=f"c{n}") for n in range(10)])
        calls = []
        conn = _connector(_deal_routes(deals=lambda b: next(pages)), calls)
        result = conn.query(_spec(fields=("id",)))
        assert result.rows == []
        assert len(_lists(calls, "/api/v2/deals")) == 2
        assert "Stopped after reading 3 records" in result.warnings[0]

    def test_mixed_currencies_are_warned_when_money_is_requested(self):
        deals = _v2([_deal(3, "2026-08-10T10:00:00Z", currency="USD"),
                     _deal(2, "2026-08-09T10:00:00Z", currency="EUR")])
        conn = _connector(_deal_routes(deals=deals))
        with_money = conn.query(_spec(fields=("title", "value")))
        assert "2 currencies (EUR, USD)" in with_money.warnings[0]
        without = conn.query(_spec(fields=("title",)))
        assert without.warnings == []

    def test_empty_result(self):
        conn = _connector(_deal_routes(deals=_v2([])))
        result = conn.query(_spec())
        assert result.rows == []
        assert result.row_count == 0


class TestDateField:
    def test_an_event_time_is_narrowed_with_updated_since(self):
        deals = _v2([
            _deal(1, "2026-01-01T00:00:00Z", status="won",
                  won_time="2026-08-15T12:00:00Z"),
            _deal(2, "2026-02-01T00:00:00Z", status="won",
                  won_time="2026-08-20T12:00:00Z"),
            _deal(3, "2026-03-01T00:00:00Z", status="won",
                  won_time="2026-09-03T12:00:00Z"),
            _deal(4, "2026-04-01T00:00:00Z"),    # open: no won_time
        ])
        calls = []
        conn = _connector(_deal_routes(deals=deals), calls)
        result = conn.query(_spec(fields=("id", "won_time"),
                                  settings={"date_field": "won_time"}))
        # Every deal is read (no early stop), and matches come newest first.
        assert [r["id"] for r in result.rows] == [2, 1]
        [(_, _, params)] = _lists(calls, "/api/v2/deals")
        assert params["updated_since"] == "2026-08-01T00:00:00Z"
        assert "sort_by" not in params
        assert any("'won_time'" in n for n in result.notes)

    def test_update_time_is_sorted_server_side(self):
        calls = []
        conn = _connector(_deal_routes(), calls)
        conn.query(_spec(fields=("id",), settings={"date_field": "update_time"}))
        [(_, _, params)] = _lists(calls, "/api/v2/deals")
        assert params["sort_by"] == "update_time"

    def test_a_date_without_server_support_is_checked_per_record(self):
        deals = _v2([
            _deal(1, "2026-01-01T00:00:00Z", expected_close_date="2026-08-31"),
            _deal(2, "2026-02-01T00:00:00Z", expected_close_date="2026-10-01"),
        ])
        calls = []
        conn = _connector(_deal_routes(deals=deals), calls)
        result = conn.query(_spec(fields=("id",),
                                  settings={"date_field": "expected_close_date"}))
        assert [r["id"] for r in result.rows] == [1]
        [(_, _, params)] = _lists(calls, "/api/v2/deals")
        assert not {"sort_by", "updated_since"} & set(params)
        # A plain date has no time zone to speak of.
        assert not any("(UTC)" in n for n in result.notes)

    def test_a_date_the_report_cannot_filter_on_is_refused(self):
        calls = []
        conn = _connector(_deal_routes(), calls)
        with pytest.raises(ApiError) as exc:
            conn.query(_spec(settings={"date_field": "due_date"}))
        assert exc.value.code == ErrorCode.INVALID_SETTING
        assert "won_time" in exc.value.message
        assert calls == []

    def test_catalog_help_text_names_every_accepted_date_field(self):
        from terno_dbi.catalog.declarations import _APIS
        spec = next(s for s in _APIS if s.key == "pipedrive")
        declared = {r.id: r for r in spec.report_types}
        assert set(declared) == set(pipedrive._REPORTS)
        for rid, report in pipedrive._REPORTS.items():
            [setting] = declared[rid].settings
            assert setting.setting_id == "date_field"
            assert setting.required is False
            for field in report.date_fields:
                assert field in setting.help_text, (rid, field)


class TestOtherReports:
    def test_leads_page_by_offset_and_sort_with_v1_syntax(self):
        pages = [
            {"success": True, "data": [
                {"id": "uuid-2", "title": "Lead B", "owner_id": 11,
                 "value": {"amount": 500, "currency": "EUR"},
                 "add_time": "2026-08-20T10:00:00.000Z"}],
             "additional_data": {"pagination": {
                 "start": 0, "limit": 500, "more_items_in_collection": True}}},
            {"success": True, "data": [
                {"id": "uuid-1", "title": "Lead A", "owner_id": 11,
                 "value": None, "add_time": "2026-08-02T10:00:00.000Z"}],
             "additional_data": {"pagination": {
                 "start": 1, "limit": 500, "more_items_in_collection": False}}},
        ]
        calls = []
        conn = _connector({("GET", "/api/v1/leads"): lambda b: pages.pop(0),
                           ("GET", "/api/v1/users"): USERS}, calls)
        result = conn.query(_spec(fields=("title", "value", "currency", "owner"),
                                  report_type="Leads"))
        assert result.rows == [
            {"title": "Lead B", "value": 500.0, "currency": "EUR",
             "owner": "Ada Lovelace"},
            {"title": "Lead A", "value": None, "currency": None,
             "owner": "Ada Lovelace"},
        ]
        leads = _lists(calls, "/api/v1/leads")
        assert leads[0][2]["sort"] == "add_time DESC"
        assert "start" not in leads[0][2]
        assert leads[1][2]["start"] == 1
        # v1 has no option labels to ask for.
        assert "include_option_labels" not in leads[0][2]

    def test_persons_keep_the_primary_email_and_phone(self):
        persons = _v2([{
            "id": 601, "name": "Hank Scorpio", "org_id": 501,
            "add_time": "2026-08-05T10:00:00Z",
            "emails": [{"value": "old@globex.com", "primary": False, "label": "home"},
                       {"value": "hank@globex.com", "primary": True, "label": "work"}],
            "phones": [{"value": "+1 555 0100", "primary": False, "label": "work"}],
            "custom_fields": {}}])
        conn = _connector({("GET", "/api/v2/personFields"): _v2([]),
                           ("GET", "/api/v2/persons"): persons,
                           ("GET", "/api/v2/organizations"): ORGS})
        result = conn.query(_spec(fields=("name", "email", "phone", "organization"),
                                  report_type="Persons"))
        assert result.rows == [{"name": "Hank Scorpio", "email": "hank@globex.com",
                                "phone": "+1 555 0100", "organization": "Globex"}]

    def test_organization_address_parts(self):
        orgs = _v2([{"id": 501, "name": "Globex", "add_time": "2026-08-05T10:00:00Z",
                     "address": {"value": "1 Main St, Springfield",
                                 "country": "United States",
                                 "locality": "Springfield"},
                     "custom_fields": {}}])
        conn = _connector({("GET", "/api/v2/organizationFields"): _v2([]),
                           ("GET", "/api/v2/organizations"): orgs})
        result = conn.query(_spec(fields=("name", "address", "country", "city"),
                                  report_type="Organizations"))
        assert result.rows == [{"name": "Globex", "address": "1 Main St, Springfield",
                                "country": "United States", "city": "Springfield"}]

    def test_activities_can_be_dated_by_due_date(self):
        activities = _v2([
            {"id": 9, "subject": "Call Hank", "type": "call", "done": False,
             "due_date": "2026-08-30", "deal_id": 3,
             "add_time": "2026-07-01T10:00:00Z"},
            {"id": 8, "subject": "Demo", "type": "meeting", "done": True,
             "due_date": "2026-07-15", "add_time": "2026-07-01T09:00:00Z"},
        ])
        calls = []
        conn = _connector({("GET", "/api/v2/activities"): activities,
                           ("GET", "/api/v2/deals"): _v2([{"id": 3, "title": "Big deal"}])},
                          calls)
        result = conn.query(_spec(fields=("subject", "done", "deal"),
                                  report_type="Activities",
                                  settings={"date_field": "due_date"}))
        assert result.rows == [{"subject": "Call Hank", "done": False,
                                "deal": "Big deal"}]
        [(_, _, params)] = _lists(calls, "/api/v2/activities")
        assert params["sort_by"] == "due_date"
        assert "include_option_labels" not in params


class TestScopes:
    def test_a_report_whose_scope_was_not_granted_fails_before_any_call(self):
        calls = []
        conn = _connector(_deal_routes(), calls,
                          scopes="base,contacts:read,users:read")
        with pytest.raises(ApiError) as exc:
            conn.query(_spec())
        assert exc.value.code == ErrorCode.AUTH_EXPIRED
        assert "deals:read" in exc.value.message
        assert calls == []

    @pytest.mark.parametrize("scopes", ["base,deals:read", "base,deals:full", None])
    def test_read_full_or_unknown_scopes_are_allowed(self, scopes):
        conn = _connector(_deal_routes(), scopes=scopes)
        assert conn.query(_spec()).row_count == 2


class _Resp:
    def __init__(self, status, body=None, text="", headers=None):
        self.status_code = status
        self._body = body
        self.text = text
        self.headers = headers or {}

    def json(self):
        if self._body is None:
            raise ValueError("no json")
        return self._body


class TestTransport:
    def _patch(self, monkeypatch, resp):
        import requests
        seen = {}

        def fake_request(method, url, headers=None, timeout=None, **kw):
            seen.update(method=method, url=url, headers=headers, **kw)
            return resp

        monkeypatch.setattr(requests, "request", fake_request)
        return seen

    def test_uses_a_bearer_token_and_query_params(self, monkeypatch):
        seen = self._patch(monkeypatch, _Resp(200, {"data": []}))
        _default_http("GET", f"{HOST}/api/v2/deals", "tok", {"limit": 500})
        assert seen["headers"] == {"Authorization": "Bearer tok"}
        assert seen["params"] == {"limit": 500}

    def test_401_is_an_auth_error(self, monkeypatch):
        self._patch(monkeypatch, _Resp(401, {"success": False, "error": "unauthorized"}))
        with pytest.raises(_AuthError):
            _default_http("GET", f"{HOST}/api/v2/deals", "tok")

    def test_scope_mismatch_asks_for_the_scope_and_a_reconnect(self, monkeypatch):
        self._patch(monkeypatch, _Resp(403, {"success": False,
                                             "error": "Scope and URL mismatch"}))
        with pytest.raises(ApiError) as exc:
            _default_http("GET", f"{HOST}/api/v1/leads", "tok")
        assert exc.value.code == ErrorCode.AUTH_EXPIRED
        assert "reconnect" in exc.value.message
        assert exc.value.retriable is False

    def test_other_errors_surface_pipedrives_message(self, monkeypatch):
        self._patch(monkeypatch, _Resp(400, {"success": False,
                                             "error": "Invalid cursor"}))
        with pytest.raises(ApiError) as exc:
            _default_http("GET", f"{HOST}/api/v2/deals", "tok")
        assert exc.value.code == ErrorCode.UPSTREAM_ERROR
        assert "Invalid cursor" in exc.value.message
        assert exc.value.retriable is False

    def test_rate_limit_is_retriable_with_a_delay(self, monkeypatch):
        self._patch(monkeypatch, _Resp(429, text="Too Many Requests",
                                       headers={"X-RateLimit-Reset": "2"}))
        with pytest.raises(ApiError) as exc:
            _default_http("GET", f"{HOST}/api/v2/deals", "tok")
        assert exc.value.code == ErrorCode.RATE_LIMITED
        assert exc.value.retriable is True
        assert exc.value.retry_after_seconds == 2

    def test_server_errors_are_retriable(self, monkeypatch):
        self._patch(monkeypatch, _Resp(503, text="<html>down</html>"))
        with pytest.raises(ApiError) as exc:
            _default_http("GET", f"{HOST}/api/v2/deals", "tok")
        assert exc.value.retriable is True


class TestAuthMapping:
    def test_401_becomes_auth_expired(self):
        def http(method, url, token, body=None):
            raise _AuthError()

        conn = PipedriveConnector(_DS(), http=http)
        with pytest.raises(ApiError) as exc:
            conn.list_accounts()
        assert exc.value.code == ErrorCode.AUTH_EXPIRED

    def test_an_unexpected_failure_is_a_generic_upstream_error(self):
        def http(method, url, token, body=None):
            raise RuntimeError(f"boom with {token}")

        conn = PipedriveConnector(_DS(), http=http)
        with pytest.raises(ApiError) as exc:
            conn.list_accounts()
        assert exc.value.code == ErrorCode.UPSTREAM_ERROR
        assert "tok" not in exc.value.message


class TestRegistration:
    def test_pipedrive_is_registered_at_startup(self):
        from terno_dbi.connectors.api import registry
        assert registry.is_supported("pipedrive")

    def test_registered_factory_binds_a_token_refresher(self):
        from terno_dbi.connectors.api.sources.pipedrive import make_pipedrive_connector
        conn = make_pipedrive_connector(_DS())
        assert conn._token_refresher is not None


# --- write actions ----------------------------------------------------------

LEAD_ID = "adf21080-0e10-11eb-879b-05d71fb426ec"


def _one(record):
    return {"success": True, "data": record}


class TestWriteActions:
    ACCOUNT = "7001"

    def _conn(self, extra=None, calls=None, **ds_kwargs):
        routes = {
            ("GET", "/api/v2/dealFields"): DEAL_FIELDS,
            ("GET", "/api/v1/users"): USERS,
            ("GET", "/api/v2/stages"): {"success": True, "data": [
                {"id": 1, "name": "Qualified", "pipeline_id": 1},
                {"id": 2, "name": "Negotiations", "pipeline_id": 1},
                {"id": 7, "name": "Onboarding", "pipeline_id": 2}],
                "additional_data": {"next_cursor": None}},
            ("GET", "/api/v2/pipelines"): PIPELINES,
            ("GET", "/api/v2/organizations"): ORGS,
            ("GET", "/api/v2/persons"): PERSONS,
        }
        routes.update(extra or {})
        return _connector(routes, calls, **ds_kwargs)

    @staticmethod
    def _writes(calls):
        return [c for c in calls if c[0] in ("POST", "PATCH", "PUT", "DELETE")]

    def _update(self, conn, fields, record_type="Deals", record_id="3", **kw):
        return conn.execute_action("update_record", self.ACCOUNT, {
            "record_type": record_type, "record_id": record_id, "fields": fields},
            **kw)

    def _create(self, conn, fields, record_type="Deals", **kw):
        return conn.execute_action("create_record", self.ACCOUNT, {
            "record_type": record_type, "fields": fields}, **kw)

    # -- discovery

    def test_offers_create_and_update_but_no_delete(self):
        actions = {a.id: a for a in _connector({}).list_actions()}
        assert set(actions) == {"create_record", "update_record"}
        assert actions["create_record"].destructive is False
        assert actions["update_record"].destructive is True
        assert actions["update_record"].schema["required"] == [
            "record_type", "record_id", "fields"]
        schema = actions["create_record"].schema["properties"]
        assert schema["record_type"]["enum"] == list(REPORTS)
        # The description names every writable field, so an agent need not guess.
        for field in ("stage_id", "organization_id", "emails", "due_time"):
            assert field in schema["fields"]["description"]

    # -- create

    def test_create_posts_the_deal_and_returns_its_id(self):
        calls = []
        conn = self._conn({("POST", "/api/v2/deals"): _one({"id": 42})}, calls)
        result = self._create(conn, {"title": "New", "value": 500,
                                     "currency": "usd", "owner_id": "11"})
        [(method, url, body)] = self._writes(calls)
        assert (method, url) == ("POST", f"{HOST}/api/v2/deals")
        assert body == {"title": "New", "value": 500, "currency": "USD",
                        "owner_id": 11}
        assert result.after == {"id": "42", "title": "New", "value": 500,
                                "currency": "usd", "owner_id": "11"}
        assert result.before is None
        assert result.details == {"record_type": "Deals", "record_id": "42"}
        assert "42" in result.summary

    def test_standard_fields_alone_need_no_metadata_call(self):
        calls = []
        conn = self._conn({("POST", "/api/v2/deals"): _one({"id": 42})}, calls)
        self._create(conn, {"title": "New"})
        assert [c[1] for c in calls] == [f"{HOST}/api/v2/deals"]

    def test_custom_options_are_sent_by_id(self):
        calls = []
        conn = self._conn({("POST", "/api/v2/deals"): _one({"id": 42})}, calls)
        self._create(conn, {"title": "New", TIER: "gold", ADDONS: ["SSO", 2],
                            SEATS: 12, MARGIN: {"value": 90, "currency": "EUR"}})
        [(_, _, body)] = self._writes(calls)
        assert body["custom_fields"] == {
            TIER: 3, ADDONS: [1, 2], SEATS: 12,
            MARGIN: {"value": 90, "currency": "EUR"}}

    def test_an_empty_selection_is_cleared_with_null(self):
        calls = []
        conn = self._conn({("GET", "/api/v2/deals/3"): _one(_deal(3, "2026-08-01T00:00:00Z")),
                           ("PATCH", "/api/v2/deals/3"): _one({"id": 3})}, calls)
        self._update(conn, {ADDONS: []})
        [(_, _, body)] = self._writes(calls)
        assert body == {"custom_fields": {ADDONS: None}}

    def test_an_unknown_option_names_the_choices(self):
        conn = self._conn()
        with pytest.raises(ApiError) as exc:
            self._create(conn, {"title": "New", TIER: "Platinum"})
        assert exc.value.code == ErrorCode.INVALID_ACTION_PARAMS
        assert "Gold, Silver" in exc.value.message

    def test_create_a_lead_linked_to_a_person(self):
        calls = []
        conn = self._conn({("POST", "/api/v1/leads"): _one({"id": LEAD_ID})}, calls)
        result = self._create(conn, {
            "title": "Inbound", "person_id": 601,
            "value": {"amount": 200, "currency": "eur"}}, record_type="Leads")
        [(method, url, body)] = self._writes(calls)
        assert (method, url) == ("POST", f"{HOST}/api/v1/leads")
        assert body == {"title": "Inbound", "person_id": 601,
                        "value": {"amount": 200, "currency": "EUR"}}
        assert result.details["record_id"] == LEAD_ID

    def test_a_lead_must_be_linked_to_someone(self):
        calls = []
        conn = self._conn({}, calls)
        with pytest.raises(ApiError) as exc:
            self._create(conn, {"title": "Orphan"}, record_type="Leads")
        assert "person_id or an organization_id" in exc.value.message
        assert calls == []

    def test_one_email_becomes_the_primary_entry(self):
        calls = []
        conn = self._conn({("POST", "/api/v2/persons"): _one({"id": 602})}, calls)
        self._create(conn, {"name": "Marge", "emails": "marge@globex.com",
                            "phones": [{"value": "+1 555", "label": "work"},
                                       {"value": "+1 556", "primary": True}]},
                     record_type="Persons")
        [(_, _, body)] = self._writes(calls)
        assert body["emails"] == [{"value": "marge@globex.com", "primary": True}]
        assert body["phones"] == [
            {"value": "+1 555", "primary": False, "label": "work"},
            {"value": "+1 556", "primary": True}]

    def test_an_organization_address_can_be_one_string(self):
        calls = []
        conn = self._conn({("POST", "/api/v2/organizations"): _one({"id": 502})}, calls)
        self._create(conn, {"name": "Initech", "address": "1 Office Park"},
                     record_type="Organizations")
        [(_, _, body)] = self._writes(calls)
        assert body == {"name": "Initech", "address": {"value": "1 Office Park"}}

    def test_an_activity_person_is_set_as_its_primary_participant(self):
        calls = []
        conn = self._conn({("POST", "/api/v2/activities"): _one({"id": 90})}, calls)
        self._create(conn, {"subject": "Call Hank", "type": "call",
                            "person_id": 601, "due_date": "2026-10-02",
                            "due_time": "09:30"}, record_type="Activities")
        [(_, _, body)] = self._writes(calls)
        assert body == {"subject": "Call Hank", "type": "call",
                        "due_date": "2026-10-02", "due_time": "09:30",
                        "participants": [{"person_id": 601, "primary": True}]}

    def test_a_new_record_needs_its_name(self):
        calls = []
        conn = self._conn({}, calls)
        with pytest.raises(ApiError) as exc:
            self._create(conn, {"value": 5})
        assert "needs: title" in exc.value.message
        assert calls == []

    # -- update

    def test_update_reads_the_deal_before_writing(self):
        calls = []
        deal = _deal(3, "2026-08-01T00:00:00Z", custom_fields={
            TIER: {"id": 4, "label": "Silver"}})
        conn = self._conn({("GET", "/api/v2/deals/3"): _one(deal),
                           ("PATCH", "/api/v2/deals/3"): _one({"id": 3})}, calls)
        result = self._update(conn, {"stage_id": 2, "status": "won", TIER: "Gold"})
        read = [c for c in calls if c[1].endswith("/api/v2/deals/3")]
        assert read[0] == ("GET", f"{HOST}/api/v2/deals/3",
                           {"include_option_labels": "true"})
        assert read[1] == ("PATCH", f"{HOST}/api/v2/deals/3",
                           {"stage_id": 2, "status": "won",
                            "custom_fields": {TIER: 3}})
        assert result.before == {"id": "3", "stage_id": 1, "status": "open",
                                 TIER: "Silver"}
        assert result.after == {"id": "3", "stage_id": 2, "status": "won",
                                TIER: "Gold"}
        assert result.details == {"record_type": "Deals", "record_id": "3"}

    def test_an_activity_person_change_keeps_the_other_participants(self):
        calls = []
        activity = {"id": 9, "subject": "Demo", "person_id": 601, "participants": [
            {"person_id": 601, "primary": True},
            {"person_id": 603, "primary": False}]}
        conn = self._conn({("GET", "/api/v2/activities/9"): _one(activity),
                           ("PATCH", "/api/v2/activities/9"): _one({"id": 9}),
                           ("GET", "/api/v2/persons"): _v2([{"id": 604, "name": "Otto"}])},
                          calls)
        result = self._update(conn, {"person_id": 604, "done": True},
                              record_type="Activities", record_id="9")
        [(_, _, body)] = self._writes(calls)
        assert body == {"done": True, "participants": [
            {"person_id": 604, "primary": True},
            {"person_id": 603, "primary": False}]}
        assert result.before == {"id": "9", "person_id": 601, "done": None}
        # Activities have no field metadata to read.
        assert "include_option_labels" not in (calls[0][2] or {})

    def test_a_lead_is_updated_by_its_uuid_in_lower_case(self):
        calls = []
        lead = {"id": LEAD_ID, "title": "Inbound", "owner_id": 11}
        conn = self._conn({("GET", f"/api/v1/leads/{LEAD_ID}"): _one(lead),
                           ("PATCH", f"/api/v1/leads/{LEAD_ID}"): _one(lead)}, calls)
        result = self._update(conn, {"owner_id": 12}, record_type="Leads",
                              record_id=LEAD_ID.upper())
        [(method, url, body)] = self._writes(calls)
        assert (method, url) == ("PATCH", f"{HOST}/api/v1/leads/{LEAD_ID}")
        assert body == {"owner_id": 12}
        assert result.before == {"id": LEAD_ID, "owner_id": 11}

    def test_an_activity_lead_link_is_sent_in_lower_case(self):
        calls = []
        conn = self._conn({("POST", "/api/v2/activities"): _one({"id": 90})}, calls)
        self._create(conn, {"subject": "Follow up", "lead_id": LEAD_ID.upper()},
                     record_type="Activities")
        [(_, _, body)] = self._writes(calls)
        assert body == {"subject": "Follow up", "lead_id": LEAD_ID}

    def test_update_of_a_missing_record_is_refused(self):
        def not_found(body):
            raise ApiError(ErrorCode.UPSTREAM_ERROR, "Pipedrive API error (404)",
                           details={"status": 404, "error": "Deal not found"})

        calls = []
        conn = self._conn({("GET", "/api/v2/deals/3"): not_found}, calls)
        with pytest.raises(ApiError) as exc:
            self._update(conn, {"title": "X"})
        assert exc.value.code == ErrorCode.INVALID_ACTION_PARAMS
        assert "No Deals record with id 3" in exc.value.message
        assert self._writes(calls) == []

    def test_a_deleted_record_is_not_updated(self):
        calls = []
        deal = _deal(3, "2026-08-01T00:00:00Z", is_deleted=True)
        conn = self._conn({("GET", "/api/v2/deals/3"): _one(deal)}, calls)
        with pytest.raises(ApiError) as exc:
            self._update(conn, {"title": "X"})
        assert "deleted" in exc.value.message
        assert self._writes(calls) == []

    # -- dry run

    def test_dry_run_create_sends_nothing(self):
        calls = []
        conn = self._conn({}, calls)
        result = self._create(conn, {"title": "New", "stage_id": 1}, dry_run=True)
        assert self._writes(calls) == []
        assert result.summary.startswith("[dry-run")
        assert result.details["applied"] is False
        assert "record exists" not in result.details["validated"]
        assert "unverified" not in result.details

    def test_dry_run_update_reads_but_does_not_write(self):
        calls = []
        conn = self._conn({("GET", "/api/v2/deals/3"): _one(
            _deal(3, "2026-08-01T00:00:00Z"))}, calls)
        result = self._update(conn, {"value": 9000}, dry_run=True)
        assert self._writes(calls) == []
        assert result.before == {"id": "3", "value": 1000}
        assert result.details["dry_run"] is True
        assert "record exists" in result.details["validated"]

    def test_dry_run_names_links_it_could_not_check(self):
        def boom(body):
            raise ApiError(ErrorCode.AUTH_EXPIRED, "no users:read")

        conn = self._conn({("GET", "/api/v1/users"): boom})
        result = self._create(conn, {"title": "New", "owner_id": 11}, dry_run=True)
        assert result.details["unverified"] == ["owner_id"]

    # -- linked records

    def test_an_unknown_stage_names_the_stages(self):
        calls = []
        conn = self._conn({}, calls)
        with pytest.raises(ApiError) as exc:
            self._create(conn, {"title": "New", "stage_id": 99})
        assert exc.value.code == ErrorCode.INVALID_ACTION_PARAMS
        assert "no Pipedrive stage has id 99" in exc.value.message
        assert "2 Negotiations (pipeline 1)" in exc.value.message
        assert self._writes(calls) == []

    def test_an_unknown_owner_names_the_users(self):
        conn = self._conn()
        with pytest.raises(ApiError) as exc:
            self._create(conn, {"title": "New", "owner_id": 99})
        assert "11 Ada Lovelace" in exc.value.message

    def test_a_missing_person_is_refused(self):
        calls = []
        conn = self._conn({("GET", "/api/v2/persons"): _v2([])}, calls)
        with pytest.raises(ApiError) as exc:
            self._create(conn, {"title": "New", "person_id": 999})
        assert "no Pipedrive person has id 999" in exc.value.message
        assert self._writes(calls) == []

    def test_a_custom_user_field_is_checked_too(self):
        conn = self._conn()
        with pytest.raises(ApiError) as exc:
            self._create(conn, {"title": "New", CHAMPION: 99})
        assert f"{CHAMPION!r}: no Pipedrive user has id 99" in exc.value.message

    def test_a_stage_from_another_pipeline_is_refused(self):
        conn = self._conn()
        with pytest.raises(ApiError) as exc:
            self._create(conn, {"title": "New", "pipeline_id": 1, "stage_id": 7})
        assert "belongs to pipeline 2, not 1" in exc.value.message

    # -- refusals before any call

    @pytest.mark.parametrize("field", ["is_deleted", "is_archived", "archive_time"])
    def test_deleting_or_archiving_is_never_forwarded(self, field):
        calls = []
        conn = self._conn({}, calls)
        with pytest.raises(ApiError) as exc:
            self._update(conn, {field: True})
        assert exc.value.code == ErrorCode.INVALID_ACTION_PARAMS
        assert "never deleted or archived" in exc.value.message
        assert self._writes(calls) == []

    @pytest.mark.parametrize("field, hint", [
        ("owner", "Set 'owner_id' instead"),
        ("stage", "Set 'stage_id' instead"),
        ("add_time", "cannot be set on a Deals record"),
        ("id", "cannot be set on a Deals record"),
    ])
    def test_read_only_columns_point_at_what_to_set(self, field, hint):
        calls = []
        conn = self._conn({}, calls)
        with pytest.raises(ApiError) as exc:
            self._update(conn, {field: 1})
        assert exc.value.code == ErrorCode.INVALID_ACTION_PARAMS
        assert hint in exc.value.message
        assert self._writes(calls) == []

    def test_a_persons_email_column_points_at_emails(self):
        conn = self._conn({("GET", "/api/v2/personFields"): _v2([])})
        with pytest.raises(ApiError) as exc:
            self._update(conn, {"email": "x@y.com"}, record_type="Persons")
        assert "Set 'emails' instead" in exc.value.message

    def test_unknown_field_is_refused_with_a_suggestion(self):
        conn = self._conn()
        with pytest.raises(ApiError) as exc:
            self._update(conn, {"titel": "X"})
        assert exc.value.code == ErrorCode.INVALID_FIELD
        assert "title" in exc.value.message

    @pytest.mark.parametrize("fields, needle", [
        ({"status": "closed"}, "open, won or lost"),
        ({"expected_close_date": "31/08/2026"}, "'YYYY-MM-DD' date"),
        ({"owner_id": "abc"}, "numeric Pipedrive id"),
        ({"owner_id": True}, "numeric Pipedrive id"),
        ({"value": "500"}, "a number"),
        ({"currency": "euro"}, "three-letter currency code"),
        ({"title": None}, "cannot be cleared"),
        ({"title": 5}, "text"),
    ])
    def test_malformed_values_are_refused_before_any_call(self, fields, needle):
        calls = []
        conn = self._conn({}, calls)
        with pytest.raises(ApiError) as exc:
            self._update(conn, fields)
        assert exc.value.code == ErrorCode.INVALID_ACTION_PARAMS
        assert needle in exc.value.message
        assert calls == []

    @pytest.mark.parametrize("fields, record_type, needle", [
        ({"value": {"amount": 5}}, "Leads", '"amount"'),
        ({"lead_id": "not-a-uuid"}, "Activities", "UUID"),
        ({"due_time": "9:30am"}, "Activities", "'HH:MM' time"),
        ({"done": "yes"}, "Activities", "true or false"),
        ({"emails": []}, "Persons", "list of"),
        ({"emails": [{"value": "a@b.c", "primary": True},
                     {"value": "d@e.f", "primary": True}]}, "Persons", "one primary"),
    ])
    def test_malformed_values_per_record_type(self, fields, record_type, needle):
        conn = self._conn()
        record_id = LEAD_ID if record_type == "Leads" else "3"
        with pytest.raises(ApiError) as exc:
            self._update(conn, fields, record_type=record_type, record_id=record_id)
        assert needle in exc.value.message

    @pytest.mark.parametrize("record_type, record_id", [
        ("Deals", "3/../../users"), ("Deals", "abc"), ("Deals", ""),
        ("Deals", "0"), ("Deals", LEAD_ID), ("Leads", "42"),
        ("Leads", f"{LEAD_ID}/x"),
    ])
    def test_a_malformed_record_id_is_refused_before_any_call(self, record_type, record_id):
        calls = []
        conn = self._conn({}, calls)
        with pytest.raises(ApiError) as exc:
            self._update(conn, {"title": "X"}, record_type=record_type,
                         record_id=record_id)
        assert exc.value.code == ErrorCode.INVALID_ACTION_PARAMS
        assert "record_id" in exc.value.message
        assert calls == []

    @pytest.mark.parametrize("params, needle", [
        ({"record_type": "Products", "fields": {"name": "X"}}, "'record_type' must be"),
        ({"record_type": "Deals", "fields": {}}, "non-empty object"),
        ({"record_type": "Deals", "fields": "title=X"}, "non-empty object"),
        ({"record_type": "Deals", "fields": {"title": "X"}, "force": True},
         "does not accept: force"),
    ])
    def test_malformed_params_are_refused(self, params, needle):
        calls = []
        conn = self._conn({}, calls)
        with pytest.raises(ApiError) as exc:
            conn.execute_action("create_record", self.ACCOUNT, params)
        assert exc.value.code == ErrorCode.INVALID_ACTION_PARAMS
        assert needle in exc.value.message
        assert calls == []

    def test_unknown_action_is_refused(self):
        with pytest.raises(ApiError) as exc:
            _connector({}).execute_action("delete_record", self.ACCOUNT, {})
        assert exc.value.code == ErrorCode.UNKNOWN_ACTION
        assert "create_record" in exc.value.message

    # -- scopes and provider refusals

    def test_writing_needs_the_full_scope(self):
        calls = []
        conn = self._conn({}, calls, scopes="base,deals:read,users:read")
        with pytest.raises(ApiError) as exc:
            self._create(conn, {"title": "New"})
        assert exc.value.code == ErrorCode.AUTH_EXPIRED
        assert "'deals:full'" in exc.value.message
        assert calls == []

    def test_the_full_scope_allows_both_reads_and_writes(self):
        conn = self._conn({("POST", "/api/v2/deals"): _one({"id": 42}),
                           ("GET", "/api/v2/deals"): DEALS},
                          scopes="base,deals:full,users:read")
        assert self._create(conn, {"title": "New"}).details["record_id"] == "42"
        assert conn.query(_spec()).row_count == 2

    def test_pipedrives_refusal_is_surfaced(self):
        def refuse(body):
            raise ApiError(ErrorCode.UPSTREAM_ERROR, "Pipedrive API error (400)",
                           details={"status": 400,
                                    "error": "Lost reason can only be set for lost deals"})

        conn = self._conn({("POST", "/api/v2/deals"): refuse})
        with pytest.raises(ApiError) as exc:
            self._create(conn, {"title": "New", "lost_reason": "Price"})
        assert exc.value.code == ErrorCode.INVALID_ACTION_PARAMS
        assert "Lost reason can only be set for lost deals" in exc.value.message
        assert exc.value.retriable is False

    def test_a_server_error_on_write_stays_retriable(self):
        def down(body):
            raise ApiError(ErrorCode.UPSTREAM_ERROR, "Pipedrive API error (503)",
                           retriable=True, details={"status": 503, "error": ""})

        conn = self._conn({("POST", "/api/v2/deals"): down})
        with pytest.raises(ApiError) as exc:
            self._create(conn, {"title": "New"})
        assert exc.value.code == ErrorCode.UPSTREAM_ERROR
        assert exc.value.retriable is True


class TestWriteTransport:
    def test_errors_carry_the_status_and_pipedrives_message(self, monkeypatch):
        import requests

        monkeypatch.setattr(requests, "request", lambda *a, **k: _Resp(
            400, {"success": False, "error": "Validation failed: title"}))
        with pytest.raises(ApiError) as exc:
            _default_http("PATCH", f"{HOST}/api/v2/deals/3", "tok", {"title": ""})
        assert exc.value.details == {"status": 400,
                                     "error": "Validation failed: title"}

    def test_a_write_sends_a_json_body(self, monkeypatch):
        import requests
        seen = {}

        def fake_request(method, url, headers=None, timeout=None, **kw):
            seen.update(method=method, **kw)
            return _Resp(200, {"success": True, "data": {"id": 3}})

        monkeypatch.setattr(requests, "request", fake_request)
        _default_http("PATCH", f"{HOST}/api/v2/deals/3", "tok", {"title": "X"})
        assert seen == {"method": "PATCH", "json": {"title": "X"}}
