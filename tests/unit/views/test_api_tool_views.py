"""End-to-end wiring for the API-source tools (Phase 3 + tool wiring).

Exercises the Django view layer with a fake connector registered, proving the
whole chain — resolve datasource, enforce the account allowlist, run through
dispatch, poll — works without a live provider.
"""

import hashlib
import json

import pytest
from django.contrib.auth.models import Group, User
from django.core.cache import cache
from django.test import RequestFactory

from terno_dbi.catalog.refresh import refresh_catalog
from terno_dbi.connectors.api import registry
from terno_dbi.connectors.api.model.base import ApiConnector
from terno_dbi.connectors.api.model.types import Account, Field, QueryResult
from terno_dbi.core.models import (
    ConnectorCatalog,
    CoreOrganisation,
    DataSource,
    GroupAccountAllowlist,
    ServiceToken,
)
from terno_dbi.core.query_service import api_views


class FakeGA4(ApiConnector):
    def list_accounts(self):
        return [
            Account("111", "Client A", currency="USD"),
            Account("222", "Client B", currency="USD"),
        ]

    def list_fields(self, report_type=None):
        return [
            Field("date", "Date", "dimension", data_type="date"),
            Field("sessions", "Sessions", "metric"),
        ]

    def _run(self, spec):
        return QueryResult(list(spec.fields), [{"date": spec.date_range.start, "sessions": 7}], 1)


@pytest.fixture(autouse=True)
def registered_connector():
    # Save whatever is registered at startup (the real GA4) and restore it, so
    # this fake does not leak into other tests.
    from terno_dbi.connectors.api.registry import _REGISTRY
    previous = _REGISTRY.get("googleanalytics4")
    registry.register("googleanalytics4", lambda ds: FakeGA4(ds))
    cache.clear()
    yield
    if previous is not None:
        registry.register("googleanalytics4", previous)
    else:
        registry.unregister("googleanalytics4")
    cache.clear()


@pytest.fixture
def env(db):
    user = User.objects.create_user("apiviews", "a@x.com", "pw")
    org = CoreOrganisation.objects.create(name="Acme", subdomain="acme", owner=user)
    refresh_catalog()
    ds = DataSource.objects.create(
        display_name="GA4", type="googleanalytics4", connection_str="",
        organisation=org,
        catalog=ConnectorCatalog.objects.get(key="googleanalytics4"),
    )
    # A connected source has tokens; a non-expiring one so the refresh seam is
    # a no-op in these tests (refresh itself is covered in test_oauth_flow).
    from terno_dbi.services import secrets
    ds.connection_json = secrets.encrypt_dict({"ACCESS_TOKEN": "test-token"})
    ds.save()
    key = "dbi_query_apiviewstoken"
    token = ServiceToken.objects.create(
        name="Query", token_type=ServiceToken.TokenType.QUERY,
        key_prefix="dbi_query_",
        key_hash=hashlib.sha256(key.encode()).hexdigest(),
        is_active=True, created_by=user, organisation=org,
    )
    token.datasources.add(ds)
    return {"user": user, "org": org, "ds": ds, "token": token}


def _req(method, token, body=None):
    rf = RequestFactory()
    if method == "GET":
        request = rf.get("/x")
    else:
        request = rf.post("/x", data=json.dumps(body or {}),
                          content_type="application/json")
    request.service_token = token
    request.allowed_datasources = token.get_accessible_datasources()
    request.token_organisation = token.organisation
    return request


def _json(response):
    return json.loads(response.content)


@pytest.mark.django_db
class TestListAccounts:
    def test_unrestricted_returns_all(self, env):
        resp = api_views.api_list_accounts(_req("GET", env["token"]), "GA4")
        data = _json(resp)
        assert data["count"] == 2

    def test_allowlist_filters_what_is_visible(self, env):
        g = Group.objects.create(name="analysts")
        env["token"].groups.add(g)
        GroupAccountAllowlist.objects.create(
            group=g, data_source=env["ds"], account_id="111")

        resp = api_views.api_list_accounts(_req("GET", env["token"]), "GA4")
        data = _json(resp)
        # Only the permitted account is even visible.
        assert {a["id"] for a in data["accounts"]} == {"111"}


@pytest.mark.django_db
class TestListFields:
    def test_returns_fields(self, env):
        resp = api_views.api_list_fields(_req("GET", env["token"]), "GA4")
        data = _json(resp)
        assert {f["id"] for f in data["fields"]} == {"date", "sessions"}

    def test_surfaces_valid_report_types(self, env):
        # So the agent does not have to guess "standard" and learn from an error.
        resp = api_views.api_list_fields(_req("GET", env["token"]), "GA4")
        data = _json(resp)
        assert "Default" in data["report_types"]

    def test_filter_narrows_the_field_list(self, env):
        req = _req("GET", env["token"])
        req.GET = req.GET.copy()
        req.GET["filter"] = "session"
        resp = api_views.api_list_fields(req, "GA4")
        data = _json(resp)
        assert {f["id"] for f in data["fields"]} == {"sessions"}   # 'date' dropped

    def test_kind_metric_returns_only_metrics(self, env):
        req = _req("GET", env["token"])
        req.GET = req.GET.copy()
        req.GET["kind"] = "metric"
        resp = api_views.api_list_fields(req, "GA4")
        data = _json(resp)
        # FakeGA4 has one metric (sessions) and one dimension (date).
        assert {f["id"] for f in data["fields"]} == {"sessions"}
        assert all(f["kind"] == "metric" for f in data["fields"])

    def test_kind_and_filter_compose(self, env):
        req = _req("GET", env["token"])
        req.GET = req.GET.copy()
        req.GET["kind"] = "dimension"
        req.GET["filter"] = "date"
        resp = api_views.api_list_fields(req, "GA4")
        data = _json(resp)
        assert {f["id"] for f in data["fields"]} == {"date"}


@pytest.mark.django_db
class TestDataQuery:
    def _body(self, accounts):
        return {
            "accounts": accounts, "fields": ["date", "sessions"],
            "report_type": "Default",
            "date_range": {"start": "2026-08-01", "end": "2026-08-31"},
        }

    def test_query_runs_and_results_poll_completed(self, env):
        resp = api_views.api_data_query(
            _req("POST", env["token"], self._body(["111"])), "GA4")
        started = _json(resp)
        assert started["query_id"].startswith("q_")

        poll = api_views.api_query_results(
            _req("GET", env["token"]), started["query_id"])
        result = _json(poll)
        assert result["status"] == "completed"
        assert result["rows"][0]["sessions"] == 7

    def test_forbidden_account_fails_the_job(self, env):
        # Configure the allowlist so "222" is forbidden for the caller.
        g = Group.objects.create(name="analysts")
        env["token"].groups.add(g)
        GroupAccountAllowlist.objects.create(
            group=g, data_source=env["ds"], account_id="111")

        resp = api_views.api_data_query(
            _req("POST", env["token"], self._body(["222"])), "GA4")
        started = _json(resp)
        poll = api_views.api_query_results(
            _req("GET", env["token"]), started["query_id"])
        result = _json(poll)
        assert result["status"] == "failed"
        assert result["error"]["code"] == "ACCOUNT_FORBIDDEN"

    def test_missing_date_range_is_a_clean_error(self, env):
        resp = api_views.api_data_query(
            _req("POST", env["token"], {"accounts": ["111"], "fields": ["sessions"]}),
            "GA4")
        assert _json(resp)["success"] is False


@pytest.mark.django_db
class TestDatasourceResolution:
    def test_resolves_by_id(self, env):
        resp = api_views.api_list_accounts(_req("GET", env["token"]), str(env["ds"].id))
        assert _json(resp)["status"] == "success"

    def test_resolves_by_display_name(self, env):
        resp = api_views.api_list_accounts(_req("GET", env["token"]), "GA4")
        assert _json(resp)["status"] == "success"

    def test_resolves_by_connector_key(self, env):
        # An agent naturally passes the key it saw in list_datasources.
        resp = api_views.api_list_accounts(
            _req("GET", env["token"]), "googleanalytics4")
        assert _json(resp)["status"] == "success"

    def test_ambiguous_key_asks_for_an_id(self, env):
        # A second GA4 connection makes the key ambiguous.
        from terno_dbi.services import secrets
        ga4b = DataSource.objects.create(
            display_name="GA4 (second)", type="googleanalytics4",
            connection_str="", organisation=env["org"],
            catalog=ConnectorCatalog.objects.get(key="googleanalytics4"),
        )
        ga4b.connection_json = secrets.encrypt_dict({"ACCESS_TOKEN": "t"})
        ga4b.save()
        env["token"].datasources.add(ga4b)

        resp = api_views.api_list_accounts(
            _req("GET", env["token"]), "googleanalytics4")
        data = _json(resp)
        assert data["success"] is False
        assert "specific id" in data["error"]["message"]

    def test_unknown_identifier_points_at_list_datasources(self, env):
        resp = api_views.api_list_accounts(_req("GET", env["token"]), "nope")
        data = _json(resp)
        assert data["success"] is False
        assert "list_datasources" in data["error"]["message"]


@pytest.mark.django_db
class TestGuards:
    def test_database_source_rejects_data_query(self, env):
        # A database datasource must route to execute_query, not here.
        db_ds = DataSource.objects.create(
            display_name="PG", type="postgres",
            connection_str="postgresql://u:p@h:5432/d",
            organisation=env["org"],
            catalog=ConnectorCatalog.objects.get(key="postgres"),
        )
        env["token"].datasources.add(db_ds)
        resp = api_views.api_list_accounts(_req("GET", env["token"]), "PG")
        assert _json(resp)["success"] is False

    def test_get_today_needs_no_datasource(self, env):
        resp = api_views.api_get_today(_req("GET", env["token"]))
        assert _json(resp)["status"] == "success"
        assert "utc_date" in _json(resp)


@pytest.mark.django_db
class TestResultsAreOrgScoped:
    def test_other_org_cannot_poll(self, env):
        resp = api_views.api_data_query(
            _req("POST", env["token"], {
                "accounts": ["111"], "fields": ["date", "sessions"],
                "report_type": "Default",
                "date_range": {"start": "2026-08-01", "end": "2026-08-31"},
            }), "GA4")
        qid = _json(resp)["query_id"]

        # A token for a different org must not read the job.
        other_user = User.objects.create_user("other", "o2@x.com", "pw")
        other_org = CoreOrganisation.objects.create(
            name="Other", subdomain="other", owner=other_user)
        key = "dbi_query_othertoken"
        other_token = ServiceToken.objects.create(
            name="Other", token_type=ServiceToken.TokenType.QUERY,
            key_prefix="dbi_query_",
            key_hash=hashlib.sha256(key.encode()).hexdigest(),
            is_active=True, created_by=other_user, organisation=other_org,
        )
        poll = api_views.api_query_results(_req("GET", other_token), qid)
        assert poll.status_code == 404
