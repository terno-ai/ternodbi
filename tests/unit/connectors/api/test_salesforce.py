"""The Salesforce connector, against mocked REST API responses.

The mock returns the real response shapes from `query`, `sobjects/X/describe`
and a `nextRecordsUrl` page, so SOQL building, describe-driven field discovery,
relationship parsing and pagination are exercised without a live org.
"""

import pytest

from terno_dbi.connectors.api.model.errors import ApiError, ErrorCode
from terno_dbi.connectors.api.model.types import DateRange, QuerySpec
from terno_dbi.connectors.api.sources.salesforce import (
    SalesforceConnector, _AuthError,
)

INSTANCE = "https://acme.my.salesforce.com"

_DATE_FIELD = {"setting_id": "date_field", "required": False,
               "label": "Date field"}


class _Catalog:
    key = "salesforce"
    report_types = [
        {"id": "Opportunity", "settings": [_DATE_FIELD]},
        {"id": "Lead", "settings": [_DATE_FIELD]},
        {"id": "Case", "settings": [_DATE_FIELD]},
        {"id": "Custom", "settings": [
            {"setting_id": "object", "required": True, "label": "Object"},
            _DATE_FIELD,
        ]},
    ]
    has_report_types = True


class _DS:
    type = "salesforce"
    catalog = _Catalog()
    connection_json = {"ACCESS_TOKEN": "tok", "INSTANCE_URL": INSTANCE}


# --- canned Salesforce responses -------------------------------------------

ORG = {
    "totalSize": 1,
    "done": True,
    "records": [{
        "attributes": {"type": "Organization"},
        "Id": "00D5j000000abcdEAA",
        "Name": "Acme Corp",
        "OrganizationType": "Enterprise Edition",
        "DefaultCurrencyIsoCode": "GBP",
        "TimeZoneSidKey": "Europe/London",
    }],
}

OPPORTUNITY_DESCRIBE = {
    "name": "Opportunity",
    "fields": [
        {"name": "Id", "label": "Opportunity ID", "type": "id", "custom": False},
        {"name": "Name", "label": "Opportunity Name", "type": "string",
         "custom": False},
        {"name": "StageName", "label": "Stage", "type": "picklist",
         "custom": False},
        {"name": "Amount", "label": "Amount", "type": "currency",
         "custom": False},
        {"name": "Probability", "label": "Probability (%)", "type": "percent",
         "custom": False},
        {"name": "CloseDate", "label": "Close Date", "type": "date",
         "custom": False},
        {"name": "CreatedDate", "label": "Created Date", "type": "datetime",
         "custom": False},
        {"name": "IsWon", "label": "Won", "type": "boolean", "custom": False},
        {"name": "Pipeline_Health__c", "label": "Pipeline Health",
         "type": "double", "custom": True,
         "inlineHelpText": "Scored nightly by the ops team."},
        # A reference with a single, named parent: becomes Owner.Name too.
        {"name": "OwnerId", "label": "Owner ID", "type": "reference",
         "custom": False, "relationshipName": "Owner", "referenceTo": ["User"]},
        # Polymorphic: no single parent to name.
        {"name": "WhoId", "label": "Who ID", "type": "reference",
         "custom": False, "relationshipName": "Who",
         "referenceTo": ["Lead", "Contact"]},
        # Not selectable as a scalar.
        {"name": "BillingAddress", "label": "Billing Address",
         "type": "address", "custom": False},
    ],
}

LEAD_DESCRIBE = {
    "name": "Lead",
    "fields": [
        {"name": "Id", "label": "Lead ID", "type": "id", "custom": False},
        {"name": "Company", "label": "Company", "type": "string",
         "custom": False},
        {"name": "CreatedDate", "label": "Created Date", "type": "datetime",
         "custom": False},
    ],
}

# An object with no CreatedDate at all — the date filter has nothing to bite on.
PROJECT_DESCRIBE = {
    "name": "Project__c",
    "fields": [
        {"name": "Id", "label": "Record ID", "type": "id", "custom": False},
        {"name": "Name", "label": "Name", "type": "string", "custom": False},
        {"name": "Budget__c", "label": "Budget", "type": "currency",
         "custom": True},
    ],
}

OPPORTUNITIES = {
    "totalSize": 2,
    "done": True,
    "records": [
        {
            "attributes": {"type": "Opportunity"},
            "Id": "0065j00000AAA1",
            "Name": "Acme renewal",
            "Amount": 24000.0,
            "StageName": "Negotiation",
            "CloseDate": "2026-08-14",
            "Owner": {"attributes": {"type": "User"}, "Name": "Dana Reed"},
        },
        {
            "attributes": {"type": "Opportunity"},
            "Id": "0065j00000AAA2",
            "Name": "Northwind expansion",
            "Amount": 9500.0,
            "StageName": "Prospecting",
            "CloseDate": "2026-08-02",
            # A null lookup arrives as an explicit null, not a missing key.
            "Owner": None,
        },
    ],
}


def _mock_http(routes=None, records=None):
    """An http stub routed on a substring of the URL."""
    routes = routes or {}

    def http(method, url, token, params=None):
        assert token == "tok"
        assert url.startswith(INSTANCE)
        for needle, response in routes.items():
            if needle in url:
                return response
        if "/describe" in url:
            return OPPORTUNITY_DESCRIBE
        if "/query" in url:
            q = (params or {}).get("q", "")
            if "FROM Organization" in q:
                return ORG
            return records if records is not None else OPPORTUNITIES
        raise AssertionError(f"unexpected call: {method} {url}")

    return http


def _connector(routes=None, records=None, **kwargs):
    return SalesforceConnector(_DS(), http=_mock_http(routes, records),
                               **kwargs)


def _capture(describe=None, records=None):
    """A connector plus every call it makes, newest last."""
    calls = []

    def http(method, url, token, params=None):
        calls.append({"method": method, "url": url, "params": params or {}})
        if "/describe" in url:
            return describe or OPPORTUNITY_DESCRIBE
        q = (params or {}).get("q", "")
        if "FROM Organization" in q:
            return ORG
        return records if records is not None else OPPORTUNITIES

    return SalesforceConnector(_DS(), http=http), calls


def _spec(fields=("Id", "Name", "Amount"), accounts=("00D5j000000abcdEAA",),
          report_type="Opportunity", settings=None, max_rows=1000):
    return QuerySpec(
        accounts=list(accounts),
        fields=list(fields),
        date_range=DateRange("2026-08-01", "2026-08-31"),
        report_type=report_type,
        settings=dict(settings or {}),
        max_rows=max_rows,
    )


def _soql(calls):
    """The SOQL of the last data query (not the Organization lookup)."""
    queries = [c for c in calls
               if "/query" in c["url"]
               and "FROM Organization" not in c["params"].get("q", "")]
    return queries[-1]["params"]["q"]


class TestListAccounts:
    def test_the_org_is_the_account(self):
        conn = _connector()
        accounts = conn.list_accounts()
        assert len(accounts) == 1
        assert accounts[0].id == "00D5j000000abcdEAA"
        assert accounts[0].name == "Acme Corp"
        # Currency is load-bearing: the dispatch layer refuses to sum money
        # across accounts that bill differently.
        assert accounts[0].currency == "GBP"
        assert accounts[0].timezone == "Europe/London"
        assert accounts[0].extra["edition"] == "Enterprise Edition"

    def test_the_org_is_looked_up_once(self):
        conn, calls = _capture()
        conn.list_accounts()
        conn.list_accounts()
        org_calls = [c for c in calls
                     if "FROM Organization" in c["params"].get("q", "")]
        assert len(org_calls) == 1


class TestListFields:
    def test_describe_drives_the_catalogue(self):
        conn = _connector()
        by_id = {f.id: f for f in conn.list_fields("Opportunity")}
        assert by_id["Name"].kind == "dimension"
        assert by_id["Amount"].kind == "metric"
        assert by_id["Amount"].is_monetary is True
        assert by_id["CloseDate"].data_type == "date"
        assert by_id["CreatedDate"].data_type == "datetime"
        assert by_id["IsWon"].data_type == "boolean"

    def test_custom_fields_are_surfaced_and_grouped(self):
        # A curated field list would miss these entirely; they are the reason
        # discovery is live rather than static.
        conn = _connector()
        by_id = {f.id: f for f in conn.list_fields("Opportunity")}
        assert by_id["Pipeline_Health__c"].kind == "metric"
        assert by_id["Pipeline_Health__c"].group == "Custom fields"
        assert by_id["Pipeline_Health__c"].description == (
            "Scored nightly by the ops team.")
        assert by_id["Name"].group == "Standard fields"

    def test_a_percentage_is_flagged_non_aggregatable(self):
        conn = _connector()
        by_id = {f.id: f for f in conn.list_fields("Opportunity")}
        # Summing a column of per-record percentages is meaningless.
        assert by_id["Probability"].is_non_aggregatable is True

    def test_compound_fields_are_excluded(self):
        conn = _connector()
        ids = {f.id for f in conn.list_fields("Opportunity")}
        # SOQL returns an address as a nested object, never a scalar column.
        assert "BillingAddress" not in ids

    def test_named_parents_get_a_name_field(self):
        conn = _connector()
        by_id = {f.id: f for f in conn.list_fields("Opportunity")}
        assert "Owner.Name" in by_id
        assert by_id["Owner.Name"].group == "Related records"
        assert by_id["OwnerId"].id == "OwnerId"

    def test_polymorphic_lookups_get_no_name_field(self):
        conn = _connector()
        ids = {f.id for f in conn.list_fields("Opportunity")}
        # WhoId may point at a Lead or a Contact; naming one would be a lie.
        assert "Who.Name" not in ids

    def test_describe_is_cached_per_object(self):
        conn, calls = _capture()
        conn.list_fields("Opportunity")
        conn.list_fields("Opportunity")
        assert len([c for c in calls if "/describe" in c["url"]]) == 1

    def test_custom_report_has_no_fields_without_an_object(self):
        # list_fields carries no settings, so there is nothing to describe.
        conn = _connector()
        assert conn.list_fields("Custom") == []


class TestSoqlBuilding:
    def test_selects_the_requested_fields_from_the_report_object(self):
        conn, calls = _capture()
        conn.query(_spec())
        soql = _soql(calls)
        assert soql.startswith("SELECT Id, Name, Amount FROM Opportunity")

    def test_a_date_field_takes_a_bare_date_literal(self):
        conn, calls = _capture()
        conn.query(_spec())
        soql = _soql(calls)
        assert "WHERE CloseDate >= 2026-08-01 AND CloseDate <= 2026-08-31" in soql

    def test_a_datetime_field_takes_a_full_instant(self):
        conn, calls = _capture()
        conn.query(_spec(fields=("Id", "CreatedDate"),
                         settings={"date_field": "CreatedDate"}))
        soql = _soql(calls)
        # A bare date against a DateTime field is a SOQL parse error, and the
        # end of the range must reach the last second of the day.
        assert "CreatedDate >= 2026-08-01T00:00:00Z" in soql
        assert "CreatedDate <= 2026-08-31T23:59:59Z" in soql

    def test_opportunities_default_to_close_date_not_created_date(self):
        conn, calls = _capture()
        conn.query(_spec())
        assert "CloseDate >=" in _soql(calls)

    def test_other_objects_default_to_created_date(self):
        conn, calls = _capture(describe=LEAD_DESCRIBE)
        conn.query(_spec(fields=("Id", "Company"), report_type="Lead"))
        assert "FROM Lead" in _soql(calls)
        assert "CreatedDate >=" in _soql(calls)

    def test_orders_newest_first_and_limits_to_max_rows(self):
        conn, calls = _capture()
        conn.query(_spec(max_rows=250))
        soql = _soql(calls)
        # A truncated result should be the recent end of the range.
        assert "ORDER BY CloseDate DESC" in soql
        assert soql.endswith("LIMIT 250")

    def test_no_fields_requested_falls_back_to_identity(self):
        conn, calls = _capture()
        result = conn.query(_spec(fields=()))
        assert _soql(calls).startswith("SELECT Id, Name FROM Opportunity")
        assert result.requested_field_ids == ["Id", "Name"]


class TestCustomObject:
    def test_the_object_setting_selects_the_object(self):
        conn, calls = _capture(describe=PROJECT_DESCRIBE)
        conn.query(_spec(fields=("Id", "Budget__c"), report_type="Custom",
                         settings={"object": "Project__c"}))
        assert "FROM Project__c" in _soql(calls)
        assert "/sobjects/Project__c/describe" in "".join(
            c["url"] for c in calls)

    def test_a_missing_object_setting_is_an_actionable_error(self):
        conn = _connector()
        with pytest.raises(ApiError) as exc:
            conn.query(_spec(report_type="Custom", settings={}))
        assert exc.value.code == ErrorCode.MISSING_SETTING

    def test_an_object_without_the_default_date_field_is_unfiltered(self):
        conn, calls = _capture(describe=PROJECT_DESCRIBE,
                               records={"done": True, "records": []})
        result = conn.query(_spec(fields=("Id", "Name"), report_type="Custom",
                                  settings={"object": "Project__c"}))
        soql = _soql(calls)
        assert "WHERE" not in soql
        # Silently returning the whole object would misread as a filtered
        # result, so the caller is told.
        assert any("date range was not applied" in n for n in result.notes)


class TestInjection:
    def test_an_object_name_that_is_not_an_identifier_is_rejected(self):
        conn = _connector()
        with pytest.raises(ApiError) as exc:
            conn.query(_spec(report_type="Custom",
                             settings={"object": "Account WHERE Id != null--"}))
        assert exc.value.code == ErrorCode.INVALID_SETTING

    def test_a_date_field_that_is_not_an_identifier_is_rejected(self):
        conn = _connector()
        with pytest.raises(ApiError) as exc:
            conn.query(_spec(settings={"date_field": "CloseDate OR 1=1"}))
        assert exc.value.code == ErrorCode.INVALID_SETTING

    def test_an_unknown_date_field_is_rejected_with_a_suggestion(self):
        conn = _connector()
        with pytest.raises(ApiError) as exc:
            conn.query(_spec(settings={"date_field": "ClosedDate"}))
        # A typo must not silently widen the query to the whole object.
        assert exc.value.code == ErrorCode.INVALID_FIELD
        assert "CloseDate" in exc.value.message


class TestRunReport:
    def test_parses_records_and_drops_the_attributes_envelope(self):
        conn = _connector()
        result = conn.query(_spec())
        assert result.row_count == 2
        assert result.rows[0] == {
            "Id": "0065j00000AAA1", "Name": "Acme renewal", "Amount": 24000.0,
        }

    def test_relationship_fields_are_read_from_the_nested_parent(self):
        conn = _connector()
        result = conn.query(_spec(fields=("Id", "Owner.Name")))
        assert result.rows[0]["Owner.Name"] == "Dana Reed"

    def test_a_null_lookup_yields_none_rather_than_raising(self):
        conn = _connector()
        result = conn.query(_spec(fields=("Id", "Owner.Name")))
        assert result.rows[1]["Owner.Name"] is None

    def test_unknown_field_is_rejected_with_a_suggestion(self):
        conn = _connector()
        with pytest.raises(ApiError) as exc:
            conn.query(_spec(fields=("Id", "Ammount")))
        assert exc.value.code == ErrorCode.INVALID_FIELD
        assert "Amount" in exc.value.message

    def test_the_date_field_in_force_is_reported(self):
        conn = _connector()
        result = conn.query(_spec())
        assert any("CloseDate" in n for n in result.notes)


class TestPagination:
    def _paged(self):
        page2 = {
            "done": True,
            "records": [{"attributes": {}, "Id": "0065j00000BBB1",
                         "Name": "Third", "Amount": 100.0}],
        }
        page1 = {
            "done": False,
            "nextRecordsUrl": "/services/data/v64.0/query/01g5j-2000",
            "records": OPPORTUNITIES["records"],
        }
        calls = []

        def http(method, url, token, params=None):
            calls.append(url)
            if "/describe" in url:
                return OPPORTUNITY_DESCRIBE
            if "/query/01g5j" in url:
                return page2
            return page1

        return SalesforceConnector(_DS(), http=http), calls

    def test_follows_next_records_url(self):
        conn, calls = self._paged()
        result = conn.query(_spec())
        assert result.row_count == 3
        assert result.rows[2]["Id"] == "0065j00000BBB1"
        assert f"{INSTANCE}/services/data/v64.0/query/01g5j-2000" in calls

    def test_stops_at_max_rows_without_fetching_another_page(self):
        conn, calls = self._paged()
        result = conn.query(_spec(max_rows=2))
        assert result.row_count == 2
        assert not any("/query/01g5j" in c for c in calls)


class TestErrorMapping:
    def _erroring(self, status, body):
        from terno_dbi.connectors.api.sources import salesforce

        class _Resp:
            status_code = status

            def json(self):
                return body

            text = "boom"

        def http(method, url, token, params=None):
            if "/describe" in url:
                return OPPORTUNITY_DESCRIBE
            raise salesforce._salesforce_error(_Resp())

        return SalesforceConnector(_DS(), http=http)

    def test_the_error_code_and_message_are_surfaced(self):
        conn = self._erroring(400, [{"message": "No such column 'Foo'",
                                     "errorCode": "INVALID_FIELD"}])
        with pytest.raises(ApiError) as exc:
            conn.query(_spec())
        assert "INVALID_FIELD" in exc.value.message
        assert "No such column" in exc.value.message
        assert exc.value.retriable is False

    def test_daily_api_limit_is_a_quota_error_and_not_retriable(self):
        conn = self._erroring(403, [{
            "message": "TotalRequests Limit exceeded.",
            "errorCode": "REQUEST_LIMIT_EXCEEDED"}])
        with pytest.raises(ApiError) as exc:
            conn.query(_spec())
        # Retrying today cannot help; the org's allocation is spent.
        assert exc.value.code == ErrorCode.QUOTA_EXCEEDED
        assert exc.value.retriable is False

    def test_server_errors_are_retriable(self):
        conn = self._erroring(503, [{"message": "Server unavailable",
                                     "errorCode": "SERVER_UNAVAILABLE"}])
        with pytest.raises(ApiError) as exc:
            conn.query(_spec())
        assert exc.value.retriable is True


class TestInstanceUrl:
    def test_requests_go_to_the_org_instance_not_the_login_host(self):
        conn, calls = _capture()
        conn.query(_spec())
        assert all(c["url"].startswith(INSTANCE) for c in calls)

    def test_a_source_without_a_recorded_instance_asks_for_reconnection(self):
        class _NoInstance(_DS):
            connection_json = {"ACCESS_TOKEN": "tok"}

        conn = SalesforceConnector(_NoInstance(), http=_mock_http())
        with pytest.raises(ApiError) as exc:
            conn.list_accounts()
        assert exc.value.code == ErrorCode.AUTH_EXPIRED
        assert "reconnect" in exc.value.message.lower()


class TestAuthMapping:
    def test_401_becomes_auth_expired(self):
        def http(method, url, token, params=None):
            raise _AuthError()

        conn = SalesforceConnector(_DS(), http=http)
        with pytest.raises(ApiError) as exc:
            conn.list_accounts()
        assert exc.value.code == ErrorCode.AUTH_EXPIRED


class TestApiVersion:
    def test_defaults_and_can_be_overridden_by_environment(self, monkeypatch):
        from terno_dbi.connectors.api.sources import salesforce

        monkeypatch.delenv("TERNO_SALESFORCE_API_VERSION", raising=False)
        assert salesforce.api_version() == salesforce._DEFAULT_VERSION
        monkeypatch.setenv("TERNO_SALESFORCE_API_VERSION", "v58.0")
        assert salesforce.api_version() == "v58.0"

    def test_the_version_is_in_the_request_path(self, monkeypatch):
        monkeypatch.setenv("TERNO_SALESFORCE_API_VERSION", "v58.0")
        conn, calls = _capture()
        conn.query(_spec())
        assert all("/services/data/v58.0/" in c["url"] for c in calls)


class TestTokenRefreshOnProviderCall:
    def test_list_accounts_refreshes_the_token(self):
        calls = []
        conn = SalesforceConnector(
            _DS(), http=_mock_http(),
            token_refresher=lambda: calls.append(1),
        )
        conn.list_accounts()
        assert calls == [1]


class TestRegistration:
    def test_salesforce_is_registered_at_startup(self):
        from terno_dbi.connectors.api import registry
        assert registry.is_supported("salesforce")

    def test_registered_factory_binds_a_token_refresher(self):
        from terno_dbi.connectors.api.sources.salesforce import (
            make_salesforce_connector,
        )
        conn = make_salesforce_connector(_DS())
        assert conn._token_refresher is not None

    def test_oauth_provider_asks_for_a_refresh_token(self):
        from terno_dbi.connectors.api.auth.providers import get_provider
        provider = get_provider("salesforce")
        assert provider is not None
        assert "refresh_token" in provider.scope
        assert provider.use_pkce is True
        assert "login.salesforce.com" in provider.authorization_url

    def test_the_login_host_can_be_pointed_at_a_sandbox(self, monkeypatch):
        from terno_dbi.connectors.api.auth.providers import get_provider

        monkeypatch.setenv("TERNO_SALESFORCE_LOGIN_URL",
                           "https://test.salesforce.com")
        provider = get_provider("salesforce")
        assert provider.authorization_url == (
            "https://test.salesforce.com/services/oauth2/authorize")
        assert provider.token_url == (
            "https://test.salesforce.com/services/oauth2/token")


class TestTokenStorage:
    def test_the_instance_url_is_persisted_from_the_token_response(self):
        from terno_dbi.connectors.api.auth import oauth

        stored = {}

        class _FakeDS:
            connection_json = None
            auth_status = None
            auth_error = ""

            def save(self, update_fields=None):
                stored.update(self.connection_json or {})

        from terno_dbi.services.secrets import decrypt_dict

        ds = _FakeDS()
        oauth._store_tokens(ds, {
            "access_token": "AT", "refresh_token": "RT",
            "instance_url": INSTANCE,
        })
        bundle = decrypt_dict(ds.connection_json)
        # Without it every request would go to the login host and 404.
        assert bundle["INSTANCE_URL"] == INSTANCE
        assert bundle["ACCESS_TOKEN"] == "AT"
