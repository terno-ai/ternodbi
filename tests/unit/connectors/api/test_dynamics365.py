"""The Microsoft Dynamics 365 connector, against a mocked Dataverse Web API.

The fake answers with the real response shapes of the table and column
metadata (`EntityDefinitions`, its `Attributes` and their typed casts,
`ManyToOneRelationships`), `organizations`, entity-set queries carrying
formatted-value annotations and `@odata.nextLink`, and record reads and writes —
so discovery, date filtering, paging, value flattening, write validation and
error mapping are exercised without a live environment, the same approach as the
Zoho CRM and Pipedrive tests.
"""

import copy
from dataclasses import replace
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit

import pytest

from terno_dbi.connectors.api.model.errors import ApiError, ErrorCode
from terno_dbi.connectors.api.model.types import DateRange, QuerySpec
from terno_dbi.connectors.api.sources import dynamics365
from terno_dbi.connectors.api.sources.dynamics365 import (
    Dynamics365Connector,
    _AuthError,
    _dataverse_error,
    _default_http,
)

HOST = "contoso.crm4.dynamics.com"
BASE = f"https://{HOST}/api/data/v9.2"
FV = "@OData.Community.Display.V1.FormattedValue"
NAV = "@Microsoft.Dynamics.CRM.associatednavigationproperty"
TARGET = "@Microsoft.Dynamics.CRM.lookuplogicalname"

ORG = "11111111-1111-1111-1111-111111111111"
USD = "22222222-2222-2222-2222-222222222222"
EUR = "33333333-3333-3333-3333-333333333333"
OPP1 = "aaaaaaaa-0000-0000-0000-000000000001"
OPP2 = "aaaaaaaa-0000-0000-0000-000000000002"
ACC = "bbbbbbbb-0000-0000-0000-000000000001"
USER = "cccccccc-0000-0000-0000-000000000001"
MISSING = "dddddddd-0000-0000-0000-000000000001"

REPORTS = ("Accounts", "Contacts", "Leads", "Opportunities", "Cases", "Tasks",
           "PhoneCalls", "Appointments")


class _Catalog:
    key = "dynamics365"
    report_types = [
        {"id": r, "settings": [{"setting_id": "date_field", "required": False}]}
        for r in REPORTS
    ] + [{"id": "Custom", "settings": [
        {"setting_id": "table", "required": True},
        {"setting_id": "date_field", "required": False},
    ]}]
    has_report_types = True


class _DS:
    type = "dynamics365"
    catalog = _Catalog()

    def __init__(self, instance=HOST):
        self.connection_json = {"ACCESS_TOKEN": "tok", "INSTANCE": instance}


# --- canned Dataverse responses ---------------------------------------------


def _lbl(text):
    return {"UserLocalizedLabel": {"Label": text}, "LocalizedLabels": []}


def _attr(name, atype, label, *, create=True, update=True, custom=False,
          type_name=None, attribute_of=None, read=True, odata=True):
    return {
        "LogicalName": name, "AttributeType": atype,
        "AttributeTypeName": {"Value": type_name or f"{atype}Type"},
        "DisplayName": _lbl(label), "Description": _lbl(""),
        "IsValidForRead": read, "IsValidForCreate": create,
        "IsValidForUpdate": update, "IsValidODataAttribute": odata,
        "AttributeOf": attribute_of, "IsCustomAttribute": custom,
    }


def _entity(logical, entity_set, primary_id, primary_name="name", custom=False):
    return {"LogicalName": logical, "EntitySetName": entity_set,
            "PrimaryIdAttribute": primary_id, "PrimaryNameAttribute": primary_name,
            "IsCustomEntity": custom, "DisplayName": _lbl(logical.title())}


OPP_ATTRS = {"value": [
    _attr("opportunityid", "Uniqueidentifier", "Opportunity", update=False),
    _attr("name", "String", "Topic"),
    _attr("customerid", "Customer", "Potential Customer"),
    _attr("parentaccountid", "Lookup", "Account"),
    _attr("estimatedvalue", "Money", "Est. Revenue"),
    _attr("estimatedvalue_base", "Money", "Est. Revenue (Base)", create=False,
          update=False, attribute_of="estimatedvalue"),
    _attr("closeprobability", "Integer", "Probability"),
    _attr("salesstage", "Picklist", "Sales Stage"),
    _attr("stepname", "String", "Pipeline Phase"),
    _attr("statecode", "State", "Status", create=False),
    _attr("statuscode", "Status", "Status Reason"),
    _attr("estimatedclosedate", "DateTime", "Est. Close Date"),
    _attr("actualclosedate", "DateTime", "Actual Close Date"),
    _attr("new_followup", "DateTime", "Follow-up", custom=True),
    _attr("createdon", "DateTime", "Created On", create=False, update=False),
    _attr("modifiedon", "DateTime", "Modified On", create=False, update=False),
    _attr("ownerid", "Owner", "Owner"),
    _attr("transactioncurrencyid", "Lookup", "Currency"),
    _attr("description", "Memo", "Description"),
    _attr("new_tier", "Picklist", "Tier", custom=True),
    _attr("new_products", "Virtual", "Products", custom=True,
          type_name="MultiSelectPicklistType"),
    _attr("new_isstrategic", "Boolean", "Strategic", custom=True),
    # Never catalogued: a helper column, bookkeeping, an image, unreadable ones.
    _attr("owneridname", "String", "Owner name", attribute_of="ownerid"),
    _attr("versionnumber", "BigInt", "Version Number"),
    _attr("entityimage", "Virtual", "Image", type_name="ImageType"),
    _attr("new_secret", "String", "Secret", read=False),
    # Activity tables flag inherited columns as not OData; the Web API serves them.
    _attr("new_inherited", "String", "Inherited", odata=False),
]}

OPP_DATES = {"value": [
    {"LogicalName": "estimatedclosedate", "DateTimeBehavior": {"Value": "DateOnly"}},
    {"LogicalName": "actualclosedate", "DateTimeBehavior": {"Value": "DateOnly"}},
    {"LogicalName": "new_followup", "DateTimeBehavior": {"Value": "TimeZoneIndependent"}},
    {"LogicalName": "createdon", "DateTimeBehavior": {"Value": "UserLocal"}},
    {"LogicalName": "modifiedon", "DateTimeBehavior": {"Value": "UserLocal"}},
]}


def _options(*pairs, global_set=False):
    options = [{"Value": v, "Label": _lbl(label)} for v, label in pairs]
    key = "GlobalOptionSet" if global_set else "OptionSet"
    return {"LogicalName": "x", key: {"Options": options}}


OPP_RELATIONSHIPS = {"value": [
    {"ReferencingAttribute": "customerid", "ReferencedEntity": "account",
     "ReferencingEntityNavigationPropertyName": "customerid_account"},
    {"ReferencingAttribute": "customerid", "ReferencedEntity": "contact",
     "ReferencingEntityNavigationPropertyName": "customerid_contact"},
    {"ReferencingAttribute": "parentaccountid", "ReferencedEntity": "account",
     "ReferencingEntityNavigationPropertyName": "parentaccountid"},
    {"ReferencingAttribute": "ownerid", "ReferencedEntity": "owner",
     "ReferencingEntityNavigationPropertyName": "ownerid"},
    {"ReferencingAttribute": "owninguser", "ReferencedEntity": "systemuser",
     "ReferencingEntityNavigationPropertyName": "owninguser"},
    {"ReferencingAttribute": "transactioncurrencyid",
     "ReferencedEntity": "transactioncurrency",
     "ReferencingEntityNavigationPropertyName": "transactioncurrencyid"},
]}

OPP_ROWS = {"value": [
    {"opportunityid": OPP1, "name": "Big deal",
     "_customerid_value": ACC, f"_customerid_value{FV}": "Contoso Ltd",
     "estimatedvalue": 50000.0, f"estimatedvalue{FV}": "$50,000.00",
     "closeprobability": 60, "salesstage": 2, f"salesstage{FV}": "Propose",
     "stepname": "3-Propose", "statecode": 0, f"statecode{FV}": "Open",
     "statuscode": 1, f"statuscode{FV}": "In Progress",
     "estimatedclosedate": "2026-11-30", "actualclosedate": None,
     "_transactioncurrencyid_value": USD,
     f"_transactioncurrencyid_value{FV}": "US Dollar",
     "_ownerid_value": USER, f"_ownerid_value{FV}": "Ada Lovelace",
     "createdon": "2026-09-14T10:20:00Z"},
    {"opportunityid": OPP2, "name": "Renewal",
     "_customerid_value": ACC, f"_customerid_value{FV}": "Contoso Ltd",
     "estimatedvalue": 1200.5, "closeprobability": None, "salesstage": 0,
     f"salesstage{FV}": "Qualify", "stepname": None, "statecode": 1,
     f"statecode{FV}": "Won", "statuscode": 3, f"statuscode{FV}": "Won",
     "estimatedclosedate": "2026-09-20", "actualclosedate": "2026-09-21",
     "_transactioncurrencyid_value": EUR,
     f"_transactioncurrencyid_value{FV}": "Euro",
     "_ownerid_value": USER, f"_ownerid_value{FV}": "Ada Lovelace",
     "createdon": "2026-09-02T08:00:00Z"},
]}

OPP_DEF = "EntityDefinitions(LogicalName='opportunity')"


def _routes():
    return {
        "GET organizations": {"value": [
            {"organizationid": ORG, "name": "Contoso", "_basecurrencyid_value": USD}]},
        f"GET transactioncurrencies({USD})": {"isocurrencycode": "USD"},
        f"GET {OPP_DEF}": _entity("opportunity", "opportunities", "opportunityid"),
        f"GET {OPP_DEF}/Attributes": OPP_ATTRS,
        f"GET {OPP_DEF}/Attributes/Microsoft.Dynamics.CRM.DateTimeAttributeMetadata":
            OPP_DATES,
        f"GET {OPP_DEF}/ManyToOneRelationships": OPP_RELATIONSHIPS,
        f"GET {OPP_DEF}/Attributes(LogicalName='customerid')/"
        f"Microsoft.Dynamics.CRM.LookupAttributeMetadata":
            {"LogicalName": "customerid", "Targets": ["account", "contact"]},
        f"GET {OPP_DEF}/Attributes(LogicalName='parentaccountid')/"
        f"Microsoft.Dynamics.CRM.LookupAttributeMetadata":
            {"LogicalName": "parentaccountid", "Targets": ["account"]},
        f"GET {OPP_DEF}/Attributes(LogicalName='ownerid')/"
        f"Microsoft.Dynamics.CRM.LookupAttributeMetadata":
            {"LogicalName": "ownerid", "Targets": ["systemuser", "team"]},
        f"GET {OPP_DEF}/Attributes(LogicalName='salesstage')/"
        f"Microsoft.Dynamics.CRM.PicklistAttributeMetadata":
            _options((0, "Qualify"), (1, "Develop"), (2, "Propose"), (3, "Close")),
        f"GET {OPP_DEF}/Attributes(LogicalName='new_tier')/"
        f"Microsoft.Dynamics.CRM.PicklistAttributeMetadata":
            _options((100000000, "Gold"), (100000001, "Silver"), global_set=True),
        f"GET {OPP_DEF}/Attributes(LogicalName='new_products')/"
        f"Microsoft.Dynamics.CRM.MultiSelectPicklistAttributeMetadata":
            _options((100000000, "SSO"), (100000001, "Audit"), (100000002, "API")),
        "GET EntityDefinitions(LogicalName='account')":
            _entity("account", "accounts", "accountid"),
        "GET EntityDefinitions(LogicalName='contact')":
            _entity("contact", "contacts", "contactid", "fullname"),
        "GET EntityDefinitions(LogicalName='systemuser')":
            _entity("systemuser", "systemusers", "systemuserid", "fullname"),
        f"GET accounts({ACC})": {"accountid": ACC},
        f"GET accounts({MISSING})": _status(404, "Entity 'account' With Id = "
                                                 f"{MISSING} Does Not Exist"),
        f"GET systemusers({USER})": {"systemuserid": USER},
        "GET opportunities": OPP_ROWS,
    }


def _status(status, message="boom", code="0x80040217"):
    """The ApiError `_dataverse_error` raises for an HTTP error status."""
    return ApiError(
        ErrorCode.UPSTREAM_ERROR, f"Dynamics 365 API error ({status}): {message}",
        retriable=status >= 500,
        details={"status": status, "code": code, "error": message},
    )


class _Dataverse:
    """A fake Dataverse Web API: canned responses by method and path."""

    def __init__(self, routes=None):
        self.routes = _routes()
        self.routes.update(routes or {})
        self.calls = []

    def __call__(self, method, url, token, body=None, headers=None):
        assert token == "tok"
        parts = urlsplit(url)
        assert parts.netloc == HOST and parts.path.startswith("/api/data/v9.2/")
        call = SimpleNamespace(
            method=method, path=parts.path[len("/api/data/v9.2/"):],
            query={k: v[0] for k, v in parse_qs(parts.query).items()},
            raw_query=parts.query, body=copy.deepcopy(body),
            headers=headers or {}, url=url)
        self.calls.append(call)
        key = f"{method} {call.path}"
        if key not in self.routes:
            raise AssertionError(f"unexpected call: {key}")
        response = self.routes[key]
        if callable(response):
            response = response(call)
        if isinstance(response, Exception):
            raise response
        return copy.deepcopy(response)

    def find(self, method, path):
        return [c for c in self.calls if c.method == method and c.path == path]


def _conn(routes=None, instance=HOST):
    fake = _Dataverse(routes)
    return Dynamics365Connector(_DS(instance), http=fake), fake


def _spec(fields=(), report_type="Opportunities", settings=None,
          start="2026-09-01", end="2026-09-30", max_rows=1000):
    return QuerySpec(
        accounts=[ORG], fields=list(fields),
        date_range=DateRange(start, end), report_type=report_type,
        settings=settings or {}, max_rows=max_rows,
    )


# --- discovery --------------------------------------------------------------


class TestAccounts:
    def test_the_environment_is_the_one_account(self):
        conn, fake = _conn()
        [account] = conn.list_accounts()
        assert account.id == ORG
        assert account.name == "Contoso"
        assert account.currency == "USD"
        assert account.as_dict()["environment_url"] == f"https://{HOST}"
        assert fake.calls[0].query["$select"] == "name,_basecurrencyid_value"

    def test_an_unreadable_currency_is_left_out(self):
        conn, _ = _conn({f"GET transactioncurrencies({USD})": _status(403, "no")})
        [account] = conn.list_accounts()
        assert account.currency is None

    @pytest.mark.parametrize("instance", [
        "", "evil.com", "contoso.crm.dynamics.com.evil.com", "contoso.dynamics.com",
    ])
    def test_the_token_only_goes_to_a_dataverse_host(self, instance):
        conn, fake = _conn(instance=instance)
        with pytest.raises(ApiError) as e:
            conn.list_accounts()
        assert e.value.code == ErrorCode.AUTH_EXPIRED
        assert fake.calls == []


class TestFields:
    def test_columns_come_from_the_live_metadata(self):
        conn, _ = _conn()
        fields = {f.id: f for f in conn.list_fields("Opportunities")}
        # A lookup is its linked record's name, plus a GUID companion.
        assert fields["customerid"].name == "Potential Customer"
        assert fields["_customerid_value"].name == "Potential Customer (ID)"
        assert fields["estimatedvalue"].kind == "metric"
        assert fields["estimatedvalue"].is_monetary
        assert fields["estimatedvalue_base"].is_monetary   # a real value, kept
        assert "base currency" in fields["estimatedvalue_base"].description
        assert fields["closeprobability"].is_non_aggregatable
        assert fields["closeprobability"].data_type == "integer"
        assert fields["salesstage"].kind == "dimension"
        assert fields["new_isstrategic"].data_type == "boolean"
        # Date-only columns are dates; the rest date-times.
        assert fields["actualclosedate"].data_type == "date"
        assert fields["createdon"].data_type == "datetime"
        assert fields["new_tier"].group == "Custom columns"
        assert "new_products" in fields   # a multi-select choice
        # Curated descriptions replace Dataverse's form hints.
        assert "won or lost" in fields["actualclosedate"].description
        for gone in ("owneridname", "versionnumber", "entityimage", "new_secret"):
            assert gone not in fields
        assert "new_inherited" in fields

    def test_custom_lists_nothing_until_a_query_names_the_table(self):
        conn, fake = _conn()
        assert conn.list_fields("Custom") == []
        assert fake.calls == []

    def test_discovery_failure_falls_back_to_the_curated_columns(self):
        conn, _ = _conn({f"GET {OPP_DEF}/Attributes": _status(500)})
        fields = {f.id: f for f in conn.list_fields("Opportunities")}
        assert {"opportunityid", "name", "customerid", "_customerid_value",
                "estimatedvalue", "createdon"} <= set(fields)
        assert fields["actualclosedate"].data_type == "date"


# --- queries ----------------------------------------------------------------


class TestQuery:
    def test_default_query_reads_names_and_labels_newest_first(self):
        conn, fake = _conn()
        result = conn.query(_spec())
        [call] = fake.find("GET", "opportunities")
        assert call.query["$select"].split(",") == [
            "opportunityid", "name", "_customerid_value", "estimatedvalue",
            "closeprobability", "salesstage", "stepname", "estimatedclosedate",
            "actualclosedate", "_transactioncurrencyid_value", "_ownerid_value",
            "statecode", "statuscode", "createdon"]
        assert call.query["$filter"] == (
            "createdon ge 2026-09-01T00:00:00Z and createdon lt 2026-10-01T00:00:00Z")
        assert call.query["$orderby"] == "createdon desc"
        assert "%20" in call.raw_query and "+" not in call.raw_query
        assert call.headers["Prefer"] == (
            'odata.include-annotations="OData.Community.Display.V1.FormattedValue",'
            'odata.maxpagesize=1000')

        first = result.rows[0]
        assert first["customerid"] == "Contoso Ltd"
        assert first["ownerid"] == "Ada Lovelace"
        assert first["salesstage"] == "Propose"
        assert first["statecode"] == "Open"
        assert first["estimatedvalue"] == 50000.0
        assert first["closeprobability"] == 60
        assert first["actualclosedate"] is None
        assert result.row_count == 2
        assert "'createdon'" in result.notes[0] and "(UTC)" in result.notes[0]

    def test_a_date_only_column_is_filtered_with_date_literals(self):
        conn, fake = _conn()
        result = conn.query(_spec(settings={"date_field": "actualclosedate"}))
        [call] = fake.find("GET", "opportunities")
        assert call.query["$filter"] == (
            "actualclosedate ge 2026-09-01 and actualclosedate le 2026-09-30")
        assert call.query["$orderby"] == "actualclosedate desc"
        assert "(UTC)" not in result.notes[0]

    def test_requested_fields_and_lookup_ids(self):
        conn, fake = _conn()
        result = conn.query(_spec(["name", "_customerid_value", "customerid"]))
        [call] = fake.find("GET", "opportunities")
        assert call.query["$select"] == "name,_customerid_value"
        assert result.requested_field_ids == ["name", "_customerid_value", "customerid"]
        assert result.rows[0] == {"name": "Big deal", "_customerid_value": ACC,
                                  "customerid": "Contoso Ltd"}

    def test_mixed_currencies_are_flagged(self):
        conn, fake = _conn()
        result = conn.query(_spec(["name", "estimatedvalue"]))
        [call] = fake.find("GET", "opportunities")
        assert call.query["$select"] == "name,estimatedvalue,_transactioncurrencyid_value"
        assert result.rows[0] == {"name": "Big deal", "estimatedvalue": 50000.0}
        assert any("2 currencies (Euro, US Dollar)" in w for w in result.warnings)

    def test_base_currency_columns_need_no_currency_check(self):
        conn, fake = _conn()
        result = conn.query(_spec(["estimatedvalue_base"]))
        [call] = fake.find("GET", "opportunities")
        assert call.query["$select"] == "estimatedvalue_base"
        assert result.warnings == []

    def test_unknown_field_suggests_close_matches(self):
        conn, _ = _conn()
        with pytest.raises(ApiError) as e:
            conn.query(_spec(["estimatedvalu"]))
        assert e.value.code == ErrorCode.INVALID_FIELD
        assert "estimatedvalue" in e.value.details["suggestions"]

    def test_date_field_must_be_a_date_column(self):
        conn, _ = _conn()
        with pytest.raises(ApiError) as e:
            conn.query(_spec(settings={"date_field": "name"}))
        assert e.value.code == ErrorCode.INVALID_SETTING
        assert "actualclosedate" in e.value.message
        with pytest.raises(ApiError) as e:
            conn.query(_spec(settings={"date_field": "closedate"}))
        assert e.value.code == ErrorCode.INVALID_FIELD

    def test_bad_date_range_is_refused_before_any_data_call(self):
        conn, fake = _conn()
        with pytest.raises(ApiError) as e:
            conn.query(_spec(start="2026-09-30", end="2026-09-01"))
        assert e.value.code == ErrorCode.INVALID_FILTER
        assert not fake.find("GET", "opportunities")

    def test_pages_are_followed_and_max_rows_stops_them(self):
        page2 = f"{BASE}/opportunities?$skiptoken=abc"
        first = {"value": OPP_ROWS["value"][:1], "@odata.nextLink": page2}
        second = {"value": OPP_ROWS["value"][1:]}

        def route(call):
            return second if "skiptoken" in call.url else first

        conn, fake = _conn({"GET opportunities": route})
        result = conn.query(_spec(["name"]))
        assert [r["name"] for r in result.rows] == ["Big deal", "Renewal"]
        assert result.warnings == []

        conn, fake = _conn({"GET opportunities": route})
        result = conn.query(_spec(["name"], max_rows=1))
        assert result.row_count == 1
        assert len(fake.find("GET", "opportunities")) == 1
        assert "odata.maxpagesize=1" in fake.calls[-1].headers["Prefer"]
        assert any("more match" in w for w in result.warnings)

    def test_a_paging_link_to_another_host_is_not_followed(self):
        conn, _ = _conn({"GET opportunities": {
            "value": OPP_ROWS["value"][:1],
            "@odata.nextLink": "https://evil.example/api/data/v9.2/opportunities"}})
        with pytest.raises(ApiError) as e:
            conn.query(_spec(["name"]))
        assert "paging link" in e.value.message

    def test_custom_report_reads_any_table(self):
        project = "EntityDefinitions(LogicalName='new_project')"
        conn, fake = _conn({
            f"GET {project}": _entity("new_project", "new_projects", "new_projectid",
                                      "new_name", custom=True),
            f"GET {project}/Attributes": {"value": [
                _attr("new_projectid", "Uniqueidentifier", "Project"),
                _attr("new_name", "String", "Name"),
                _attr("new_budget", "Money", "Budget", custom=True),
                _attr("createdon", "DateTime", "Created On"),
            ]},
            f"GET {project}/Attributes/Microsoft.Dynamics.CRM.DateTimeAttributeMetadata":
                {"value": []},
            "GET new_projects": {"value": [
                {"new_projectid": OPP1, "new_name": "Apollo",
                 "createdon": "2026-09-03T00:00:00Z"}]},
        })
        result = conn.query(_spec(report_type="Custom",
                                  settings={"table": "New_Project"}))
        [call] = fake.find("GET", "new_projects")
        assert call.query["$select"] == "new_projectid,new_name,createdon"
        assert result.rows == [{"new_projectid": OPP1, "new_name": "Apollo",
                                "createdon": "2026-09-03T00:00:00Z"}]

    def test_custom_report_without_createdon_returns_every_row(self):
        table = "EntityDefinitions(LogicalName='new_log')"
        conn, fake = _conn({
            f"GET {table}": _entity("new_log", "new_logs", "new_logid", ""),
            f"GET {table}/Attributes": {"value": [
                _attr("new_logid", "Uniqueidentifier", "Log")]},
            f"GET {table}/Attributes/Microsoft.Dynamics.CRM.DateTimeAttributeMetadata":
                {"value": []},
            "GET new_logs": {"value": [{"new_logid": OPP1}]},
        })
        result = conn.query(_spec(report_type="Custom", settings={"table": "new_log"}))
        [call] = fake.find("GET", "new_logs")
        assert "$filter" not in call.query
        assert "not applied" in result.notes[0]

    def test_custom_table_name_is_checked(self):
        conn, fake = _conn()
        with pytest.raises(ApiError) as e:
            conn.query(_spec(report_type="Custom",
                             settings={"table": "account')/x?$top=1"}))
        assert e.value.code == ErrorCode.INVALID_SETTING
        assert fake.calls == []

    def test_unknown_custom_table_is_named(self):
        conn, _ = _conn({"GET EntityDefinitions(LogicalName='new_nope')":
                         _status(404, "Could not find entity")})
        with pytest.raises(ApiError) as e:
            conn.query(_spec(report_type="Custom", settings={"table": "new_nope"}))
        assert e.value.code == ErrorCode.INVALID_SETTING
        assert "'new_nope'" in e.value.message

    def test_a_report_whose_app_is_not_installed_says_so(self):
        conn, _ = _conn({"GET EntityDefinitions(LogicalName='incident')":
                         _status(400, "Could not find entity with name incident")})
        with pytest.raises(ApiError) as e:
            conn.query(_spec(report_type="Cases"))
        assert e.value.code == ErrorCode.INVALID_REPORT_TYPE
        assert "Customer Service" in e.value.message

    def test_a_column_the_web_api_lacks_is_named_and_dropped(self):
        conn, _ = _conn({"GET opportunities": _status(
            400, "Could not find a property named 'new_inherited' on type "
                 "'Microsoft.Dynamics.CRM.opportunity'.")})
        with pytest.raises(ApiError) as e:
            conn.query(_spec(["name", "new_inherited"]))
        assert e.value.code == ErrorCode.INVALID_FIELD
        assert "'new_inherited'" in e.value.message
        assert "new_inherited" not in {f.id for f in conn.list_fields("Opportunities")}

    def test_a_table_with_no_default_columns_reads_its_id(self):
        table = "EntityDefinitions(LogicalName='new_log')"
        conn, fake = _conn({
            f"GET {table}": _entity("new_log", "new_logs", "new_logid", ""),
            f"GET {table}/Attributes": {"value": [
                _attr("new_logid", "Uniqueidentifier", "Log"),
                _attr("new_note", "String", "Note")]},
            f"GET {table}/Attributes/Microsoft.Dynamics.CRM.DateTimeAttributeMetadata":
                {"value": []},
            "GET new_logs": {"value": [{"new_logid": OPP1}]},
        })
        # Nothing is a default (as when the curated defaults are all missing).
        cols = conn._catalogue(conn._entity("new_log")).cols
        for fid, col in cols.items():
            cols[fid] = replace(col, default=False)
        conn.query(_spec(report_type="Custom", settings={"table": "new_log"}))
        [call] = fake.find("GET", "new_logs")
        assert call.query["$select"] == "new_logid"

    def test_unreadable_table_metadata_falls_back_for_a_standard_report(self):
        conn, fake = _conn({
            f"GET {OPP_DEF}": _status(503),
            f"GET {OPP_DEF}/Attributes": _status(503),
        })
        conn.query(_spec(["name"]))
        assert fake.find("GET", "opportunities")


# --- writes -----------------------------------------------------------------


def _create(fields, table="Opportunities"):
    return {"table": table, "fields": fields}


def _update(fields, record_id=OPP1, table="opportunity"):
    return {"table": table, "record_id": record_id, "fields": fields}


def _created(call):
    return {"opportunityid": OPP2}


class TestCreate:
    def test_lists_create_and_update_and_never_delete(self):
        conn, _ = _conn()
        actions = {a.id: a for a in conn.list_actions()}
        assert set(actions) == {"create_record", "update_record"}
        assert actions["create_record"].destructive is False
        assert actions["update_record"].destructive is True

    def test_create_resolves_choices_and_binds_lookups(self):
        conn, fake = _conn({"POST opportunities": _created})
        result = conn.execute_action("create_record", ORG, _create({
            "name": "New deal",
            "estimatedvalue": 25000,
            "salesstage": "propose",
            "new_tier": 100000001,
            "new_products": ["SSO", "api"],
            "customerid": {"table": "account", "id": ACC.upper()},
            "ownerid": {"table": "systemuser", "id": USER},
            "parentaccountid": ACC,
            "estimatedclosedate": "2026-12-31",
            "new_followup": "2026-10-06T15:00:00+05:30",
            "new_isstrategic": True,
        }))
        [post] = fake.find("POST", "opportunities")
        assert post.query == {"$select": "opportunityid"}
        assert post.headers["Prefer"] == "return=representation"
        assert post.body == {
            "name": "New deal",
            "estimatedvalue": 25000,
            "salesstage": 2,
            "new_tier": 100000001,
            "new_products": "100000000,100000002",
            "customerid_account@odata.bind": f"/accounts({ACC})",
            "ownerid@odata.bind": f"/systemusers({USER})",
            "parentaccountid@odata.bind": f"/accounts({ACC})",
            "estimatedclosedate": "2026-12-31",
            "new_followup": "2026-10-06T09:30:00Z",
            "new_isstrategic": True,
        }
        assert result.details == {"table": "opportunity", "record_id": OPP2}
        assert result.after["id"] == OPP2
        assert result.after["customerid"] == {"table": "account", "id": ACC}

    def test_dry_run_validates_everything_and_writes_nothing(self):
        conn, fake = _conn()
        result = conn.execute_action("create_record", ORG, _create({
            "name": "New deal", "customerid": {"table": "account", "id": ACC},
            "salesstage": "Close"}), dry_run=True)
        assert result.details["dry_run"] is True
        assert result.details["applied"] is False
        assert "unverified" not in result.details
        assert result.summary.startswith("[dry-run")
        assert fake.find("GET", f"accounts({ACC})")    # the link was checked
        assert not [c for c in fake.calls if c.method != "GET"]

    def test_a_link_that_cannot_be_checked_is_reported(self):
        conn, _ = _conn({f"GET accounts({ACC})": _status(403, "no read")})
        result = conn.execute_action("create_record", ORG, _create({
            "name": "x", "parentaccountid": ACC}), dry_run=True)
        assert result.details["unverified"] == ["parentaccountid"]

    def test_a_missing_linked_record_is_refused(self):
        conn, fake = _conn()
        with pytest.raises(ApiError) as e:
            conn.execute_action("create_record", ORG, _create({
                "name": "x", "parentaccountid": MISSING}))
        assert e.value.code == ErrorCode.INVALID_ACTION_PARAMS
        assert MISSING in e.value.message
        assert not fake.find("POST", "opportunities")

    def test_a_polymorphic_lookup_needs_its_table(self):
        conn, _ = _conn()
        with pytest.raises(ApiError) as e:
            conn.execute_action("create_record", ORG, _create({"customerid": ACC}))
        assert e.value.details["targets"] == ["account", "contact"]
        with pytest.raises(ApiError) as e:
            conn.execute_action("create_record", ORG, _create({
                "customerid": {"table": "lead", "id": ACC}}))
        assert "cannot link to a 'lead'" in e.value.message

    def test_an_unknown_choice_lists_the_options(self):
        conn, _ = _conn()
        with pytest.raises(ApiError) as e:
            conn.execute_action("create_record", ORG, _create({"salesstage": "Won"}))
        assert e.value.code == ErrorCode.INVALID_ACTION_PARAMS
        assert "Qualify (0)" in e.value.message and "Close (3)" in e.value.message

    @pytest.mark.parametrize("fields, fragment", [
        ({"estimatedvalue": "lots"}, "a number"),
        ({"closeprobability": 1.5}, "a whole number"),
        ({"closeprobability": True}, "a whole number"),
        ({"name": 7}, "text"),
        ({"new_isstrategic": "yes"}, "true or false"),
        ({"estimatedclosedate": "2026-12-31T00:00:00Z"}, "'YYYY-MM-DD'"),
        ({"new_followup": "2026-10-06T15:00:00"}, "with 'Z' or an offset"),
        ({"parentaccountid": "not-a-guid"}, "a GUID"),
        ({"customerid": {"table": "account"}}, '"table"'),
        ({"salesstage": True}, "label or numeric value"),
        ({"parentaccountid": None}, "leave it out"),
    ])
    def test_values_are_checked_before_any_write(self, fields, fragment):
        conn, fake = _conn()
        with pytest.raises(ApiError) as e:
            conn.execute_action("create_record", ORG, _create(fields))
        assert e.value.code == ErrorCode.INVALID_ACTION_PARAMS
        assert fragment in e.value.message
        assert not [c for c in fake.calls if c.method != "GET"]

    @pytest.mark.parametrize("fields, fragment", [
        ({"nmae": "x"}, "Did you mean: name"),
        ({"createdon": "2026-01-01T00:00:00Z"}, "cannot be set when creating"),
        ({"statecode": 1}, "cannot be set when creating"),
        ({"_customerid_value": ACC}, "set the lookup 'customerid'"),
        ({"opportunityid": OPP1}, "record's id"),
        ({"owneridname": "x"}, "Unknown field"),
    ])
    def test_columns_are_checked_against_the_metadata(self, fields, fragment):
        conn, _ = _conn()
        with pytest.raises(ApiError) as e:
            conn.execute_action("create_record", ORG, _create(fields))
        assert fragment in e.value.message

    def test_dataverse_rejection_is_surfaced(self):
        conn, _ = _conn({"POST opportunities": _status(
            400, "A record with the given name already exists.")})
        with pytest.raises(ApiError) as e:
            conn.execute_action("create_record", ORG, _create({"name": "dup"}))
        assert e.value.code == ErrorCode.INVALID_ACTION_PARAMS
        assert "already exists" in e.value.message
        assert e.value.retriable is False

    def test_a_missing_privilege_is_surfaced(self):
        conn, _ = _conn({"POST opportunities": ApiError(
            ErrorCode.UPSTREAM_ERROR, "Dynamics 365 refused this: missing "
            "prvCreateOpportunity privilege.", retriable=False,
            details={"status": 403, "error": "missing prvCreateOpportunity"})})
        with pytest.raises(ApiError) as e:
            conn.execute_action("create_record", ORG, _create({"name": "x"}))
        assert "prvCreateOpportunity" in e.value.message

    def test_writes_need_the_column_metadata(self):
        conn, _ = _conn({f"GET {OPP_DEF}/Attributes": _status(503)})
        with pytest.raises(ApiError) as e:
            conn.execute_action("create_record", ORG, _create({"name": "x"}))
        assert e.value.code == ErrorCode.UPSTREAM_ERROR
        assert "metadata" in e.value.message


class TestUpdate:
    RECORD = {
        "opportunityid": OPP1, "salesstage": 1, f"salesstage{FV}": "Develop",
        "estimatedvalue": 50000.0, "_customerid_value": ACC,
        f"_customerid_value{FV}": "Contoso Ltd",
        f"_customerid_value{NAV}": "customerid_account",
        f"_customerid_value{TARGET}": "account",
        "_parentaccountid_value": None, "new_products": None,
    }

    def _routes(self):
        return {f"GET opportunities({OPP1})": self.RECORD,
                f"PATCH opportunities({OPP1})": {}}

    def test_update_reads_first_and_never_upserts(self):
        conn, fake = _conn(self._routes())
        result = conn.execute_action("update_record", ORG, _update({
            "salesstage": "Propose", "estimatedvalue": 60000}, record_id=OPP1.upper()))
        [read] = fake.find("GET", f"opportunities({OPP1})")
        assert read.query["$select"] == "opportunityid,salesstage,estimatedvalue"
        assert read.headers["Prefer"] == 'odata.include-annotations="*"'
        [patch] = fake.find("PATCH", f"opportunities({OPP1})")
        assert patch.headers["If-Match"] == "*"
        assert patch.body == {"salesstage": 2, "estimatedvalue": 60000}
        assert result.before == {"id": OPP1, "salesstage": "Develop",
                                 "estimatedvalue": 50000.0}
        assert result.after == {"id": OPP1, "salesstage": "Propose",
                                "estimatedvalue": 60000}

    def test_clearing_a_lookup_uses_its_current_navigation_property(self):
        conn, fake = _conn(self._routes())
        conn.execute_action("update_record", ORG, _update({
            "customerid": None, "parentaccountid": None, "new_products": []}))
        [patch] = fake.find("PATCH", f"opportunities({OPP1})")
        # parentaccountid is already empty, so only customerid is sent.
        assert patch.body == {"customerid_account": None, "new_products": None}

    def test_a_no_op_update_sends_nothing(self):
        conn, fake = _conn(self._routes())
        conn.execute_action("update_record", ORG, _update({"parentaccountid": None}))
        assert not fake.find("PATCH", f"opportunities({OPP1})")

    def test_dry_run_shows_before_and_after(self):
        conn, fake = _conn(self._routes())
        result = conn.execute_action("update_record", ORG, _update({
            "customerid": {"table": "account", "id": ACC}}), dry_run=True)
        assert result.before == {"id": OPP1, "customerid": "Contoso Ltd"}
        assert result.details["dry_run"] is True
        assert not fake.find("PATCH", f"opportunities({OPP1})")

    def test_a_missing_record_is_refused(self):
        conn, fake = _conn({f"GET opportunities({MISSING})": _status(404)})
        with pytest.raises(ApiError) as e:
            conn.execute_action("update_record", ORG, _update(
                {"name": "x"}, record_id=MISSING))
        assert e.value.code == ErrorCode.INVALID_ACTION_PARAMS
        assert "No opportunity record" in e.value.message

    def test_a_record_deleted_meanwhile_is_reported(self):
        routes = self._routes()
        routes[f"PATCH opportunities({OPP1})"] = _status(412, "Precondition failed")
        conn, _ = _conn(routes)
        with pytest.raises(ApiError) as e:
            conn.execute_action("update_record", ORG, _update({"name": "x"}))
        assert "no longer exists" in e.value.message

    def test_read_only_and_bad_ids_are_refused(self):
        conn, _ = _conn(self._routes())
        with pytest.raises(ApiError) as e:
            conn.execute_action("update_record", ORG, _update({"modifiedon": None}))
        assert "cannot be set when updating" in e.value.message
        with pytest.raises(ApiError) as e:
            conn.execute_action("update_record", ORG, _update(
                {"name": "x"}, record_id="1 or 1=1"))
        assert e.value.details == {"param": "record_id"}


class TestWriteTargets:
    def test_unknown_action_and_extra_params(self):
        conn, _ = _conn()
        with pytest.raises(ApiError) as e:
            conn.execute_action("delete_record", ORG, {})
        assert e.value.code == ErrorCode.UNKNOWN_ACTION
        with pytest.raises(ApiError) as e:
            conn.execute_action("create_record", ORG, {**_create({"name": "x"}),
                                                       "module": "Deals"})
        assert e.value.details == {"unexpected": ["module"]}

    def test_system_tables_are_not_written(self):
        conn, fake = _conn()
        with pytest.raises(ApiError) as e:
            conn.execute_action("create_record", ORG, _create(
                {"fullname": "x"}, table="systemuser"))
        assert "system table" in e.value.message
        assert not fake.find("GET", "EntityDefinitions(LogicalName='systemuser')/Attributes")

    def test_custom_tables_are_written(self):
        project = "EntityDefinitions(LogicalName='new_project')"
        conn, fake = _conn({
            f"GET {project}": _entity("new_project", "new_projects", "new_projectid",
                                      "new_name", custom=True),
            f"GET {project}/Attributes": {"value": [
                _attr("new_projectid", "Uniqueidentifier", "Project"),
                _attr("new_name", "String", "Name"),
            ]},
            f"GET {project}/Attributes/Microsoft.Dynamics.CRM.DateTimeAttributeMetadata":
                {"value": []},
            "POST new_projects": {"new_projectid": OPP1},
        })
        result = conn.execute_action("create_record", ORG, _create(
            {"new_name": "Apollo"}, table="new_project"))
        assert result.details == {"table": "new_project", "record_id": OPP1}

    @pytest.mark.parametrize("table", ["", "opportunity')", "Deals!"])
    def test_table_is_checked(self, table):
        conn, fake = _conn()
        with pytest.raises(ApiError) as e:
            conn.execute_action("create_record", ORG, _create({"name": "x"}, table))
        assert e.value.details == {"param": "table"}
        assert fake.calls == []

    def test_unknown_table_is_an_action_error(self):
        conn, _ = _conn({"GET EntityDefinitions(LogicalName='new_nope')":
                         _status(404)})
        with pytest.raises(ApiError) as e:
            conn.execute_action("create_record", ORG, _create({"x": 1}, "new_nope"))
        assert e.value.code == ErrorCode.INVALID_ACTION_PARAMS


# --- transport and errors ---------------------------------------------------


class _Resp:
    def __init__(self, status, body=None, text="", headers=None):
        self.status_code = status
        self._body = body
        self.text = text
        self.headers = headers or {}
        self.content = b"" if body is None and not text else b"x"

    def json(self):
        if self._body is None:
            raise ValueError("not json")
        return self._body


class TestErrors:
    def test_401_is_an_auth_error(self):
        assert isinstance(_dataverse_error(_Resp(401, {})), _AuthError)

    def test_403_names_the_privilege_and_is_not_retried(self):
        err = _dataverse_error(_Resp(403, {"error": {
            "code": "0x80040220",
            "message": "Principal user is missing prvReadIncident privilege"}}))
        assert err.code == ErrorCode.UPSTREAM_ERROR
        assert "prvReadIncident" in err.message
        assert "security roles" in err.message
        assert err.retriable is False

    def test_429_is_rate_limited_with_retry_after(self):
        err = _dataverse_error(_Resp(429, {"error": {"message": "slow down"}},
                                     headers={"Retry-After": "12"}))
        assert err.code == ErrorCode.RATE_LIMITED
        assert err.retry_after_seconds == 12

    def test_5xx_is_retriable_and_4xx_is_not(self):
        assert _dataverse_error(_Resp(503, {})).retriable is True
        err = _dataverse_error(_Resp(400, {"error": {
            "code": "0x0", "message": "Could not find a property named 'x'"}}))
        assert err.retriable is False
        assert err.details["error"] == "Could not find a property named 'x'"

    def test_non_json_error_body(self):
        err = _dataverse_error(_Resp(502, text="<html>Bad gateway</html>"))
        assert "Bad gateway" in err.message

    def test_connector_maps_401_and_unexpected_failures(self):
        conn, _ = _conn({"GET organizations": _AuthError()})
        with pytest.raises(ApiError) as e:
            conn.list_accounts()
        assert e.value.code == ErrorCode.AUTH_EXPIRED
        conn, _ = _conn({"GET organizations": RuntimeError("socket closed")})
        with pytest.raises(ApiError) as e:
            conn.list_accounts()
        assert e.value.code == ErrorCode.UPSTREAM_ERROR
        assert "socket" not in e.value.message

    def test_default_http_sends_odata_headers_and_reads_204(self, monkeypatch):
        seen = {}

        def fake_request(method, url, headers=None, timeout=None, **kwargs):
            seen.update(method=method, url=url, headers=headers, kwargs=kwargs)
            return _Resp(204)

        monkeypatch.setattr("requests.request", fake_request)
        out = _default_http("PATCH", f"{BASE}/accounts({ACC})", "tok",
                            {"name": "x"}, {"If-Match": "*"})
        assert out == {}
        assert seen["headers"]["Authorization"] == "Bearer tok"
        assert seen["headers"]["OData-Version"] == "4.0"
        assert seen["headers"]["If-Match"] == "*"
        assert seen["kwargs"] == {"json": {"name": "x"}}

    def test_default_http_raises_mapped_errors(self, monkeypatch):
        monkeypatch.setattr("requests.request",
                            lambda *a, **k: _Resp(404, {"error": {"message": "gone"}}))
        with pytest.raises(ApiError) as e:
            _default_http("GET", f"{BASE}/accounts", "tok")
        assert e.value.details["status"] == 404

    def test_factory_wires_a_refresher(self):
        conn = dynamics365.make_dynamics365_connector(_DS())
        assert callable(conn._token_refresher)
