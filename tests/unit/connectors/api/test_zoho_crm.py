"""The Zoho CRM connector, against mocked CRM API v8 responses.

The mock returns the real response shapes from `org`, `settings/fields`, `coql`
and `users`, so field discovery, COQL building, paging, lookup flattening and
error mapping are exercised without a live provider — the same approach as the
HubSpot tests.
"""

import pytest

from terno_dbi.connectors.api.model.errors import ApiError, ErrorCode
from terno_dbi.connectors.api.model.types import DateRange, QuerySpec
from terno_dbi.connectors.api.sources.zoho_crm import (
    ZohoCRMConnector,
    _AuthError,
    _day_bounds,
    _default_http,
)

API = "https://www.zohoapis.eu/crm/v8"


class _Catalog:
    key = "zoho_crm"
    report_types = [
        {"id": m, "settings": []}
        for m in ("Leads", "Contacts", "Accounts", "Deals", "Cases",
                  "Tasks", "Calls", "Events")
    ]
    has_report_types = True


class _DS:
    type = "zoho_crm"
    catalog = _Catalog()

    def __init__(self, api_domain="https://www.zohoapis.eu"):
        self.connection_json = {"ACCESS_TOKEN": "tok", "API_DOMAIN": api_domain}


# --- canned Zoho responses -------------------------------------------------

ORG = {"org": [{
    "id": "5725767000000020005", "zgid": "808232144",
    "company_name": "Zylker", "domain_name": "org808232144",
    "iso_code": "EUR", "time_zone": "Europe/Berlin",
}]}

DEAL_FIELDS = {"fields": [
    {"api_name": "Deal_Name", "field_label": "Deal Name", "data_type": "text"},
    # The org renamed "Amount" — the label follows the org, the flags stay curated.
    {"api_name": "Amount", "field_label": "Deal Value", "data_type": "currency"},
    {"api_name": "Probability", "field_label": "Probability (%)",
     "data_type": "integer"},
    {"api_name": "Stage", "field_label": "Stage", "data_type": "picklist"},
    {"api_name": "Account_Name", "field_label": "Account Name",
     "data_type": "lookup"},
    {"api_name": "Owner", "field_label": "Deal Owner", "data_type": "ownerlookup"},
    {"api_name": "Created_By", "field_label": "Created By",
     "data_type": "ownerlookup"},
    {"api_name": "Created_Time", "field_label": "Created Time",
     "data_type": "datetime"},
    {"api_name": "Closing_Date", "field_label": "Closing Date", "data_type": "date"},
    {"api_name": "Discount_Pct", "field_label": "Discount", "data_type": "percent",
     "custom_field": True},
    {"api_name": "Seats", "field_label": "Seats", "data_type": "integer",
     "custom_field": True},
    {"api_name": "Margin", "field_label": "Margin", "data_type": "formula",
     "formula": {"return_type": "currency"}, "custom_field": True},
    {"api_name": "Is_Renewal", "field_label": "Renewal", "data_type": "boolean",
     "custom_field": True},
    # Not selectable in COQL / not visible — must never reach the catalogue.
    {"api_name": "Contract", "field_label": "Contract", "data_type": "fileupload"},
    {"api_name": "Line_Items", "field_label": "Line Items", "data_type": "subform"},
    {"api_name": "Secret", "field_label": "Secret", "data_type": "text",
     "visible": False},
    # Selectable, but Zoho marks it read-only — it can be read, never written.
    {"api_name": "Legacy_Ref", "field_label": "Legacy ref", "data_type": "text",
     "field_read_only": True},
]}

USERS = {"users": [
    {"id": "111", "full_name": "Ada Lovelace", "email": "ada@zylker.com"},
    {"id": "222", "first_name": "Alan", "last_name": "Turing"},
], "info": {"more_records": False}}

DEALS = {"data": [
    {"id": "9001", "Deal_Name": "Big deal", "Amount": 1000.5,
     "Account_Name": {"id": "555", "name": "Zylker Inc"},
     "Owner": {"id": "111"}, "Created_By": {"id": "222"}},
    {"id": "9002", "Deal_Name": "Small deal", "Amount": "250",
     "Account_Name": None, "Owner": {"id": "999"}, "Created_By": {"id": "222"}},
], "info": {"count": 2, "more_records": False}}


def _router(routes, calls=None):
    """An injected `http` that answers by (method, url fragment)."""
    def http(method, url, token, body=None):
        assert token == "tok"
        if calls is not None:
            calls.append((method, url, body))
        for (m, needle), response in routes.items():
            if method == m and needle in url:
                return response(body) if callable(response) else response
        raise AssertionError(f"unexpected call: {method} {url}")
    return http


def _connector(routes, calls=None, **ds_kwargs):
    return ZohoCRMConnector(_DS(**ds_kwargs), http=_router(routes, calls))


def _spec(fields=("Deal_Name", "Amount"), report_type="Deals", **kw):
    return QuerySpec(
        accounts=["808232144"], fields=list(fields),
        date_range=DateRange("2026-08-01", "2026-08-31"),
        report_type=report_type, **kw,
    )


class TestDayBounds:
    def test_inclusive_utc_window(self):
        assert _day_bounds("2026-08-01", "2026-08-31") == (
            "2026-08-01T00:00:00+00:00", "2026-08-31T23:59:59+00:00")

    @pytest.mark.parametrize("start", ["today", "2026-08-01' or 1=1 --", ""])
    def test_non_dates_are_rejected_before_reaching_coql(self, start):
        with pytest.raises(ApiError) as exc:
            _day_bounds(start, "2026-08-31")
        assert exc.value.code == ErrorCode.INVALID_FILTER
        assert "get_today" in exc.value.message

    def test_reversed_range_is_rejected(self):
        with pytest.raises(ApiError) as exc:
            _day_bounds("2026-09-01", "2026-08-01")
        assert "after" in exc.value.message


class TestListAccounts:
    def test_returns_the_org_with_currency_and_timezone(self):
        calls = []
        conn = _connector({("GET", "/org"): ORG}, calls)
        [account] = conn.list_accounts()
        assert account.id == "808232144"
        assert account.name == "Zylker"
        assert account.currency == "EUR"
        assert account.timezone == "Europe/Berlin"
        # Calls go to the org's own regional API domain.
        assert calls[0][1] == f"{API}/org"


class TestListFields:
    def test_discovers_fields_and_maps_types(self):
        conn = _connector({("GET", "settings/fields"): DEAL_FIELDS})
        fields = {f.id: f for f in conn.list_fields("Deals")}

        assert fields["Deal_Name"].kind == "dimension"
        assert fields["Created_Time"].data_type == "date"
        assert fields["Is_Renewal"].data_type == "boolean"
        assert fields["Seats"].kind == "metric"
        assert fields["Seats"].data_type == "integer"
        assert fields["Discount_Pct"].is_non_aggregatable is True
        assert fields["Margin"].is_monetary is True       # formula -> currency
        assert fields["Seats"].group == "Custom fields"
        assert "id" in fields

    def test_curated_flags_survive_but_the_org_label_wins(self):
        conn = _connector({("GET", "settings/fields"): DEAL_FIELDS})
        fields = {f.id: f for f in conn.list_fields("Deals")}
        assert fields["Amount"].name == "Deal Value"
        assert fields["Amount"].is_monetary is True
        # Zoho reports Probability as a plain integer; curation marks it a ratio.
        assert fields["Probability"].is_non_aggregatable is True

    def test_unselectable_and_hidden_fields_are_left_out(self):
        conn = _connector({("GET", "settings/fields"): DEAL_FIELDS})
        ids = {f.id for f in conn.list_fields("Deals")}
        assert not ids & {"Contract", "Line_Items", "Secret"}

    def test_discovery_failure_falls_back_to_the_curated_catalogue(self):
        def boom(body):
            raise ApiError(ErrorCode.UPSTREAM_ERROR, "down")

        conn = _connector({("GET", "settings/fields"): boom})
        ids = {f.id for f in conn.list_fields("Deals")}
        assert {"Deal_Name", "Amount", "Stage", "Owner"} <= ids

    def test_catalogue_is_fetched_once_per_module(self):
        calls = []
        conn = _connector({("GET", "settings/fields"): DEAL_FIELDS}, calls)
        conn.list_fields("Deals")
        conn.list_fields("Deals")
        assert len(calls) == 1
        assert calls[0][2] == {"module": "Deals"}

    def test_unknown_report_type_uses_the_default_module(self):
        calls = []
        conn = _connector({("GET", "settings/fields"): {"fields": [
            {"api_name": "Email", "field_label": "Email", "data_type": "email"}]}},
            calls)
        conn.list_fields("Nope")
        assert calls[0][2] == {"module": "Leads"}


class TestRunReport:
    def _routes(self, coql=DEALS):
        return {
            ("GET", "settings/fields"): DEAL_FIELDS,
            ("GET", "/users"): USERS,
            ("POST", "/coql"): coql,
        }

    def test_builds_a_coql_query_over_the_module_and_range(self):
        calls = []
        conn = _connector(self._routes(), calls)
        conn.query(_spec())
        [(method, url, body)] = [c for c in calls if "/coql" in c[1]]
        assert url == f"{API}/coql"
        assert body["select_query"] == (
            "select Deal_Name, Amount from Deals "
            "where Created_Time between '2026-08-01T00:00:00+00:00' "
            "and '2026-08-31T23:59:59+00:00' "
            "order by Created_Time desc limit 0, 200")

    def test_parses_rows_flattens_lookups_and_coerces_money(self):
        conn = _connector(self._routes())
        result = conn.query(_spec(fields=("Deal_Name", "Amount", "Account_Name")))
        assert result.requested_field_ids == ["Deal_Name", "Amount", "Account_Name"]
        assert result.rows == [
            {"Deal_Name": "Big deal", "Amount": 1000.5, "Account_Name": "Zylker Inc"},
            {"Deal_Name": "Small deal", "Amount": 250.0, "Account_Name": None},
        ]
        assert any("Created_Time" in n for n in result.notes)

    def test_user_lookups_resolve_to_names_with_id_fallback(self):
        conn = _connector(self._routes())
        result = conn.query(_spec(fields=("Deal_Name", "Owner", "Created_By")))
        assert result.rows[0]["Owner"] == "Ada Lovelace"
        assert result.rows[0]["Created_By"] == "Alan Turing"   # first + last
        assert result.rows[1]["Owner"] == "999"                # unknown user

    def test_users_are_not_fetched_without_a_user_column(self):
        calls = []
        conn = _connector(self._routes(), calls)
        conn.query(_spec())
        assert not any("/users" in c[1] for c in calls)

    def test_user_lookup_failure_degrades_to_ids(self):
        def boom(body):
            raise ApiError(ErrorCode.UPSTREAM_ERROR, "no users scope")

        routes = self._routes()
        routes[("GET", "/users")] = boom
        conn = _connector(routes)
        result = conn.query(_spec(fields=("Deal_Name", "Owner")))
        assert result.rows[0]["Owner"] == "111"

    def test_paging_advances_the_offset_until_no_more_records(self):
        pages = [
            {"data": [{"Deal_Name": "A"}], "info": {"more_records": True}},
            {"data": [{"Deal_Name": "B"}], "info": {"more_records": False}},
        ]
        queries = []

        def coql(body):
            queries.append(body["select_query"])
            return pages.pop(0)

        conn = _connector(self._routes(coql=coql))
        result = conn.query(_spec(fields=("Deal_Name",)))
        assert [r["Deal_Name"] for r in result.rows] == ["A", "B"]
        assert queries[0].endswith("limit 0, 200")
        assert queries[1].endswith("limit 1, 200")
        assert result.warnings == []

    def test_truncation_at_max_rows_is_warned(self):
        def coql(body):
            return {"data": [{"Deal_Name": "A"}, {"Deal_Name": "B"}],
                    "info": {"more_records": True}}

        conn = _connector(self._routes(coql=coql))
        result = conn.query(_spec(fields=("Deal_Name",), max_rows=2))
        assert result.row_count == 2
        assert "more match" in result.warnings[0]

    def test_empty_result_is_an_empty_list(self):
        # `_default_http` turns Zoho's 204 No Content into {}.
        conn = _connector(self._routes(coql={}))
        result = conn.query(_spec())
        assert result.rows == []
        assert result.row_count == 0

    def test_no_fields_selects_the_curated_defaults_the_org_has(self):
        calls = []
        conn = _connector(self._routes(), calls)
        result = conn.query(_spec(fields=()))
        # Curated Deals fields the org lacks (e.g. Pipeline) are not requested.
        assert "Pipeline" not in result.requested_field_ids
        assert {"id", "Deal_Name", "Amount", "Owner"} <= set(result.requested_field_ids)
        # Custom fields are discoverable but not part of the default selection.
        assert "Seats" not in result.requested_field_ids

    def test_repeated_fields_are_selected_once(self):
        calls = []
        conn = _connector(self._routes(), calls)
        result = conn.query(_spec(fields=("Deal_Name", "Amount", "Deal_Name")))
        assert result.requested_field_ids == ["Deal_Name", "Amount"]

    def test_unknown_field_is_rejected_with_a_suggestion(self):
        conn = _connector(self._routes())
        with pytest.raises(ApiError) as exc:
            conn.query(_spec(fields=("Deal_Name", "Amountt")))
        assert exc.value.code == ErrorCode.INVALID_FIELD
        assert "Amount" in exc.value.message

    def test_too_many_fields_is_rejected_before_calling_zoho(self):
        many = {"fields": [
            {"api_name": f"F{i}", "field_label": f"F{i}", "data_type": "text"}
            for i in range(60)]}
        calls = []
        conn = _connector({("GET", "settings/fields"): many}, calls)
        with pytest.raises(ApiError) as exc:
            conn.query(_spec(fields=[f"F{i}" for i in range(51)]))
        assert "at most 50" in exc.value.message
        assert not any("/coql" in c[1] for c in calls)


class TestApiDomain:
    @pytest.mark.parametrize("domain", [
        "", "https://evil.example.com", "http://www.zohoapis.com",
        "https://www.zohoapis.com.evil.io",
    ])
    def test_unrecognised_domain_never_receives_the_token(self, domain):
        calls = []
        conn = _connector({("GET", "/org"): ORG}, calls, api_domain=domain)
        with pytest.raises(ApiError) as exc:
            conn.list_accounts()
        assert exc.value.code == ErrorCode.AUTH_EXPIRED
        assert calls == []

    @pytest.mark.parametrize("domain", [
        "https://www.zohoapis.com", "https://www.zohoapis.com.au",
        "https://www.zohoapis.ca", "https://www.zohocloud.ca",
        "https://www.zohoapis.eu/",
    ])
    def test_every_data_centre_is_accepted(self, domain):
        calls = []
        conn = _connector({("GET", "/org"): ORG}, calls, api_domain=domain)
        conn.list_accounts()
        assert calls[0][1] == f"{domain.rstrip('/')}/crm/v8/org"


class _Resp:
    def __init__(self, status, body=None, text=""):
        self.status_code = status
        self._body = body
        self.text = text

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

    def test_uses_the_zoho_oauthtoken_scheme(self, monkeypatch):
        seen = self._patch(monkeypatch, _Resp(200, {"org": []}))
        _default_http("GET", f"{API}/org", "tok")
        assert seen["headers"] == {"Authorization": "Zoho-oauthtoken tok"}

    def test_204_is_an_empty_payload(self, monkeypatch):
        self._patch(monkeypatch, _Resp(204))
        assert _default_http("POST", f"{API}/coql", "tok", {"select_query": "x"}) == {}

    def test_401_is_an_auth_error(self, monkeypatch):
        self._patch(monkeypatch, _Resp(401, {"code": "INVALID_TOKEN"}))
        with pytest.raises(_AuthError):
            _default_http("GET", f"{API}/org", "tok")

    def test_scope_mismatch_asks_for_a_reconnect(self, monkeypatch):
        self._patch(monkeypatch, _Resp(401, {"code": "OAUTH_SCOPE_MISMATCH"}))
        with pytest.raises(ApiError) as exc:
            _default_http("GET", f"{API}/org", "tok")
        assert exc.value.code == ErrorCode.AUTH_EXPIRED
        assert "Reconnect" in exc.value.message
        assert exc.value.retriable is False

    def test_query_error_surfaces_zohos_code_and_message(self, monkeypatch):
        self._patch(monkeypatch, _Resp(400, {
            "code": "INVALID_QUERY", "message": "invalid column given",
            "status": "error"}))
        with pytest.raises(ApiError) as exc:
            _default_http("POST", f"{API}/coql", "tok", {"select_query": "x"})
        assert "INVALID_QUERY" in exc.value.message
        assert "invalid column given" in exc.value.message
        assert exc.value.retriable is False

    def test_rate_limit_is_retriable(self, monkeypatch):
        self._patch(monkeypatch, _Resp(429, text="Too many requests"))
        with pytest.raises(ApiError) as exc:
            _default_http("GET", f"{API}/org", "tok")
        assert exc.value.retriable is True


class TestAuthMapping:
    def test_401_becomes_auth_expired(self):
        def http(method, url, token, body=None):
            raise _AuthError()

        conn = ZohoCRMConnector(_DS(), http=http)
        with pytest.raises(ApiError) as exc:
            conn.list_accounts()
        assert exc.value.code == ErrorCode.AUTH_EXPIRED


class TestRegistration:
    def test_zoho_crm_is_registered_at_startup(self):
        from terno_dbi.connectors.api import registry
        assert registry.is_supported("zoho_crm")

    def test_registered_factory_binds_a_token_refresher(self):
        from terno_dbi.connectors.api.sources.zoho_crm import make_zoho_crm_connector
        conn = make_zoho_crm_connector(_DS())
        assert conn._token_refresher is not None


def _ok(record_id):
    return {"data": [{"code": "SUCCESS", "status": "success",
                      "message": "record updated", "details": {"id": record_id}}]}


class TestWriteActions:
    ACCOUNT = "808232144"

    def _conn(self, extra=None, calls=None):
        routes = {("GET", "settings/fields"): DEAL_FIELDS}
        routes.update(extra or {})
        return _connector(routes, calls)

    @staticmethod
    def _writes(calls):
        return [c for c in calls if c[0] in ("POST", "PUT")]

    def test_offers_create_and_update_but_no_delete(self):
        actions = {a.id: a for a in _connector({}).list_actions()}
        assert set(actions) == {"create_record", "update_record"}
        assert actions["create_record"].destructive is False
        assert actions["update_record"].destructive is True
        assert actions["update_record"].schema["required"] == [
            "module", "record_id", "fields"]

    def test_create_posts_one_record_and_returns_its_id(self):
        calls = []
        conn = self._conn({("POST", "/Deals"): _ok("9100")}, calls)
        result = conn.execute_action("create_record", self.ACCOUNT, {
            "module": "Deals", "fields": {"Deal_Name": "New", "Amount": 500}})
        [(method, url, body)] = self._writes(calls)
        assert (method, url) == ("POST", f"{API}/Deals")
        assert body == {"data": [{"Deal_Name": "New", "Amount": 500}]}
        assert result.after == {"id": "9100", "Deal_Name": "New", "Amount": 500}
        assert result.before is None
        assert "9100" in result.summary

    def test_update_reads_the_record_before_writing(self):
        calls = []
        record = {"data": [{"id": "9001", "Stage": "Qualification"}]}
        conn = self._conn({("GET", "/Deals/9001"): record,
                           ("PUT", "/Deals/9001"): _ok("9001")}, calls)
        result = conn.execute_action("update_record", self.ACCOUNT, {
            "module": "Deals", "record_id": "9001",
            "fields": {"Stage": "Closed Won"}})
        touched = [(c[0], c[1], c[2]) for c in calls if "/Deals/9001" in c[1]]
        assert touched == [
            ("GET", f"{API}/Deals/9001", {"fields": "Stage"}),
            ("PUT", f"{API}/Deals/9001", {"data": [{"Stage": "Closed Won"}]}),
        ]
        assert result.before == {"id": "9001", "Stage": "Qualification"}
        assert result.after == {"id": "9001", "Stage": "Closed Won"}

    def test_dry_run_create_sends_nothing(self):
        calls = []
        conn = self._conn({}, calls)
        result = conn.execute_action("create_record", self.ACCOUNT, {
            "module": "Deals", "fields": {"Deal_Name": "New"}}, dry_run=True)
        assert self._writes(calls) == []
        assert result.summary.startswith("[dry-run")
        assert result.details["applied"] is False
        # A new record has no target to check — the details must not claim one.
        assert "record exists" not in result.details["validated"]

    def test_dry_run_update_reads_but_does_not_write(self):
        calls = []
        record = {"data": [{"id": "9001", "Stage": "Qualification"}]}
        conn = self._conn({("GET", "/Deals/9001"): record}, calls)
        result = conn.execute_action("update_record", self.ACCOUNT, {
            "module": "Deals", "record_id": "9001",
            "fields": {"Stage": "Closed Won"}}, dry_run=True)
        assert self._writes(calls) == []
        assert result.before == {"id": "9001", "Stage": "Qualification"}
        assert result.details["dry_run"] is True
        assert "record exists" in result.details["validated"]

    def test_update_of_a_missing_record_is_refused(self):
        calls = []
        conn = self._conn({("GET", "/Deals/9001"): {}}, calls)   # 204 -> {}
        with pytest.raises(ApiError) as exc:
            conn.execute_action("update_record", self.ACCOUNT, {
                "module": "Deals", "record_id": "9001", "fields": {"Stage": "X"}})
        assert exc.value.code == ErrorCode.INVALID_ACTION_PARAMS
        assert "No Deals record with id 9001" in exc.value.message
        assert self._writes(calls) == []

    @pytest.mark.parametrize("record_id", ["9001/../Leads", "abc", "", "1 OR 1"])
    def test_non_numeric_record_id_is_refused_before_any_call(self, record_id):
        calls = []
        conn = self._conn({}, calls)
        with pytest.raises(ApiError) as exc:
            conn.execute_action("update_record", self.ACCOUNT, {
                "module": "Deals", "record_id": record_id, "fields": {"Stage": "X"}})
        assert exc.value.code == ErrorCode.INVALID_ACTION_PARAMS
        assert calls == []

    def test_unknown_field_is_refused_with_a_suggestion(self):
        calls = []
        conn = self._conn({}, calls)
        with pytest.raises(ApiError) as exc:
            conn.execute_action("create_record", self.ACCOUNT, {
                "module": "Deals", "fields": {"Stagee": "X"}})
        assert exc.value.code == ErrorCode.INVALID_FIELD
        assert "Stage" in exc.value.message
        assert self._writes(calls) == []

    @pytest.mark.parametrize("field", ["Created_Time", "Created_By", "id",
                                       "Margin", "Legacy_Ref"])
    def test_read_only_fields_are_refused(self, field):
        # System fields, a formula, and a field Zoho flags read-only.
        calls = []
        conn = self._conn({}, calls)
        with pytest.raises(ApiError) as exc:
            conn.execute_action("create_record", self.ACCOUNT, {
                "module": "Deals", "fields": {"Deal_Name": "A", field: "x"}})
        assert exc.value.code == ErrorCode.INVALID_ACTION_PARAMS
        assert "read-only" in exc.value.message
        assert self._writes(calls) == []

    @pytest.mark.parametrize("params, needle", [
        ({"module": "Invoices", "fields": {"X": 1}}, "'module' must be one of"),
        ({"module": "Deals", "fields": {}}, "non-empty object"),
        ({"module": "Deals", "fields": "Stage=Won"}, "non-empty object"),
        ({"module": "Deals", "fields": {"Stage": "X"}, "trigger": ["workflow"]},
         "does not accept: trigger"),
    ])
    def test_malformed_params_are_refused(self, params, needle):
        calls = []
        conn = self._conn({}, calls)
        with pytest.raises(ApiError) as exc:
            conn.execute_action("create_record", self.ACCOUNT, params)
        assert exc.value.code == ErrorCode.INVALID_ACTION_PARAMS
        assert needle in exc.value.message
        assert self._writes(calls) == []

    def test_unknown_action_is_refused(self):
        with pytest.raises(ApiError) as exc:
            _connector({}).execute_action("delete_record", self.ACCOUNT, {})
        assert exc.value.code == ErrorCode.UNKNOWN_ACTION
        assert "create_record" in exc.value.message

    def test_a_refusal_inside_a_2xx_body_is_surfaced(self):
        # Zoho reports writes per record; a refusal can arrive with HTTP 2xx.
        refused = {"data": [{"code": "MANDATORY_NOT_FOUND", "status": "error",
                             "message": "required field not found",
                             "details": {"api_name": "Stage"}}]}
        conn = self._conn({("POST", "/Deals"): refused})
        with pytest.raises(ApiError) as exc:
            conn.execute_action("create_record", self.ACCOUNT, {
                "module": "Deals", "fields": {"Deal_Name": "New"}})
        assert exc.value.code == ErrorCode.INVALID_ACTION_PARAMS
        assert "MANDATORY_NOT_FOUND" in exc.value.message
        assert "api_name=Stage" in exc.value.message
        assert exc.value.retriable is False


class TestWriteErrorTransport:
    def test_http_error_names_the_failing_field(self, monkeypatch):
        import requests

        body = {"data": [{"code": "INVALID_DATA", "status": "error",
                          "message": "invalid data",
                          "details": {"api_name": "Email",
                                      "expected_data_type": "email"}}]}
        monkeypatch.setattr(requests, "request",
                            lambda *a, **k: _Resp(400, body))
        with pytest.raises(ApiError) as exc:
            _default_http("POST", f"{API}/Leads", "tok", {"data": [{}]})
        assert "INVALID_DATA" in exc.value.message
        assert "api_name=Email" in exc.value.message
