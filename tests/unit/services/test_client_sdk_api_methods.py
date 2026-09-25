"""SDK coverage for the API-source and misc client methods.

Exercises the thin HTTP wrappers (today, accounts, fields, data-query, results,
connectors, table/column updates, similar-examples) via the `responses` mock, so
their URL shape, params, and payload handling are pinned.
"""

import pytest
import responses

from terno_dbi.client import TernoDBIClient

BASE = "https://test.com"


def _client():
    return TernoDBIClient(base_url=BASE, api_key="dbi_query_key")


@responses.activate
def test_get_today_passes_timezone():
    responses.add(responses.GET, f"{BASE}/api/query/today/",
                  json={"status": "success", "today": "2026-01-01"}, status=200)
    out = _client().get_today(timezone="Asia/Kolkata")
    assert out["today"] == "2026-01-01"
    assert responses.calls[0].request.params == {"timezone": "Asia/Kolkata"}


@responses.activate
def test_get_today_without_timezone_sends_no_params():
    responses.add(responses.GET, f"{BASE}/api/query/today/",
                  json={"status": "success"}, status=200)
    _client().get_today()
    assert responses.calls[0].request.params == {}


@responses.activate
def test_list_accounts():
    responses.add(responses.GET, f"{BASE}/api/query/datasources/7/accounts/",
                  json={"status": "success", "accounts": [{"id": "1"}]}, status=200)
    out = _client().list_accounts("7")
    assert out["accounts"] == [{"id": "1"}]


@responses.activate
def test_list_fields_with_filters():
    responses.add(responses.GET, f"{BASE}/api/query/datasources/ga/fields/",
                  json={"status": "success", "fields": []}, status=200)
    _client().list_fields("ga", report_type="Default", filter="sessions", kind="metric")
    assert responses.calls[0].request.params == {
        "report_type": "Default", "filter": "sessions", "kind": "metric"}


@responses.activate
def test_list_fields_without_filters_sends_no_params():
    responses.add(responses.GET, f"{BASE}/api/query/datasources/ga/fields/",
                  json={"status": "success", "fields": []}, status=200)
    _client().list_fields("ga")
    assert responses.calls[0].request.params == {}


@responses.activate
def test_data_query_posts_payload():
    responses.add(responses.POST, f"{BASE}/api/query/datasources/ga/data-query/",
                  json={"status": "success", "query_id": "q1"}, status=200)
    out = _client().data_query("ga", {"fields": ["sessions"]})
    assert out["query_id"] == "q1"


@responses.activate
def test_get_query_results():
    responses.add(responses.GET, f"{BASE}/api/query/query-results/q1/",
                  json={"status": "completed", "rows": []}, status=200)
    assert _client().get_query_results("q1")["status"] == "completed"


@responses.activate
def test_list_connectors():
    responses.add(responses.GET, f"{BASE}/api/query/connectors/",
                  json={"status": "success", "connectors": []}, status=200)
    assert _client().list_connectors()["connectors"] == []


@responses.activate
def test_update_table_sends_only_given_fields():
    responses.add(responses.PATCH, f"{BASE}/api/admin/tables/5/",
                  json={"status": "success"}, status=200)
    _client().update_table(5, description="new desc")
    import json as _json
    assert _json.loads(responses.calls[0].request.body) == {"description": "new desc"}


@responses.activate
def test_update_column_sends_only_given_fields():
    responses.add(responses.PATCH, f"{BASE}/api/admin/columns/9/",
                  json={"status": "success"}, status=200)
    _client().update_column(9, is_hidden=True)
    import json as _json
    assert _json.loads(responses.calls[0].request.body) == {"is_hidden": True}


@responses.activate
def test_find_similar_examples_includes_optional_ids():
    responses.add(responses.POST, f"{BASE}/api/query/similar-examples/",
                  json={"status": "success", "examples": []}, status=200)
    _client().find_similar_examples("show sales", org_id=1, user_id=2)
    import json as _json
    body = _json.loads(responses.calls[0].request.body)
    assert body["org_id"] == 1 and body["user_id"] == 2 and body["query"] == "show sales"
