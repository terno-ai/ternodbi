"""Coverage for the less-exercised query_service.views endpoints:
stream_query, export_query, get_similar_examples_for_agent, add_prompt_example.

Service-layer calls (SQL transform, streaming, export, LLM, vector search) are
mocked; the tests pin the view's request handling, auth/scoping, and error paths.
"""

import hashlib
import json

import pytest
from unittest.mock import MagicMock, patch
from django.test import RequestFactory

from terno_dbi.core.models import DataSource, ServiceToken
from terno_dbi.core.query_service import views as qv


@pytest.fixture
def rf():
    return RequestFactory()


@pytest.fixture
def data(db):
    from django.contrib.auth.models import User
    from terno_dbi.core.models import CoreOrganisation, OrganisationUser

    user = User.objects.create_user("qx-user", "qx@example.com", "pw")
    org = CoreOrganisation.objects.create(name="Acme", subdomain="qx-acme", owner=user)
    OrganisationUser.objects.create(user=user, organisation=org)
    ds = DataSource.objects.create(
        display_name="qx_db", type="postgres",
        connection_str="postgresql://localhost/test", enabled=True,
        organisation=org)
    key = "dbi_query_qxtoken"
    token = ServiceToken.objects.create(
        name="Q", token_type=ServiceToken.TokenType.QUERY, key_prefix="dbi_query_",
        key_hash=hashlib.sha256(key.encode()).hexdigest(), is_active=True,
        created_by=user, organisation=org)
    token.datasources.add(ds)
    return {"user": user, "org": org, "ds": ds, "token": token}


def _post(rf, body, data, resolved=True):
    request = rf.post("/x", data=json.dumps(body), content_type="application/json")
    request.service_token = data["token"]
    request.allowed_datasources = data["token"].get_accessible_datasources()
    request.token_organisation = data["org"]
    request.user = data["user"]
    if resolved:
        request.resolved_datasource = data["ds"]
    return request


# ------------------------------ stream_query -----------------------------

@pytest.mark.django_db
def test_stream_query_missing_sql_is_400(rf, data):
    resp = qv.stream_query(_post(rf, {}, data), data["ds"].id)
    assert resp.status_code == 400


@pytest.mark.django_db
def test_stream_query_invalid_json_is_400(rf, data):
    request = rf.post("/x", data="{bad", content_type="application/json")
    request.service_token = data["token"]
    request.allowed_datasources = data["token"].get_accessible_datasources()
    request.resolved_datasource = data["ds"]
    resp = qv.stream_query(request, data["ds"].id)
    assert resp.status_code == 400


@pytest.mark.django_db
def test_stream_query_missing_datasource_is_400(rf, data):
    # No resolved_datasource and no datasource in body.
    resp = qv.stream_query(_post(rf, {"sql": "SELECT 1"}, data, resolved=False))
    assert resp.status_code == 400


@pytest.mark.django_db
@patch("terno_dbi.core.query_service.views.execute_streaming_query")
@patch("terno_dbi.core.query_service.views.generate_native_sql")
@patch("terno_dbi.core.query_service.views.prepare_mdb")
def test_stream_query_success_streams(mock_mdb, mock_gen, mock_stream, rf, data):
    mock_mdb.return_value = MagicMock()
    mock_gen.return_value = {"status": "success", "native_sql": "SELECT 1"}
    mock_stream.return_value = iter(['{"a":1}\n'])
    resp = qv.stream_query(_post(rf, {"sql": "SELECT 1", "max_rows": 5}, data), data["ds"].id)
    assert resp.status_code == 200
    assert resp["Content-Type"] == "application/x-ndjson"


@pytest.mark.django_db
@patch("terno_dbi.core.query_service.views.generate_native_sql")
@patch("terno_dbi.core.query_service.views.prepare_mdb")
def test_stream_query_transform_error_is_400(mock_mdb, mock_gen, rf, data):
    mock_mdb.return_value = MagicMock()
    mock_gen.return_value = {"status": "error", "error": "bad sql"}
    resp = qv.stream_query(_post(rf, {"sql": "SELECT x"}, data), data["ds"].id)
    assert resp.status_code == 400


# ------------------------------ export_query -----------------------------

@pytest.mark.django_db
def test_export_query_missing_sql_is_400(rf, data):
    resp = qv.export_query(_post(rf, {}, data), data["ds"].id)
    assert resp.status_code == 400


@pytest.mark.django_db
@patch("terno_dbi.core.query_service.views.export_native_sql_result")
@patch("terno_dbi.core.query_service.views.generate_native_sql")
@patch("terno_dbi.core.query_service.views.prepare_mdb")
def test_export_query_success(mock_mdb, mock_gen, mock_export, rf, data):
    from django.http import HttpResponse
    mock_mdb.return_value = MagicMock()
    mock_gen.return_value = {"status": "success", "native_sql": "SELECT 1"}
    mock_export.return_value = HttpResponse("csv", content_type="text/csv")
    resp = qv.export_query(_post(rf, {"sql": "SELECT 1"}, data), data["ds"].id)
    assert resp.status_code == 200
    mock_export.assert_called_once()


@pytest.mark.django_db
@patch("terno_dbi.core.query_service.views.generate_native_sql")
@patch("terno_dbi.core.query_service.views.prepare_mdb")
def test_export_query_transform_error_is_400(mock_mdb, mock_gen, rf, data):
    mock_mdb.return_value = MagicMock()
    mock_gen.return_value = {"status": "error"}
    resp = qv.export_query(_post(rf, {"sql": "SELECT x"}, data), data["ds"].id)
    assert resp.status_code == 400


# ---------------------- get_similar_examples_for_agent -------------------

@pytest.mark.django_db
def test_similar_examples_org_not_found_is_404(rf, data):
    resp = qv.get_similar_examples_for_agent(
        _post(rf, {"org_id": 999999, "query": "q"}, data))
    assert resp.status_code == 404


@pytest.mark.django_db
@patch("terno_dbi.core.query_service.views.find_similar_examples")
@patch("terno_dbi.core.query_service.views.LLMFactory")
def test_similar_examples_success(mock_factory, mock_find, rf, data):
    llm = MagicMock()
    llm.generate_vector.return_value = [0.1, 0.2]
    mock_factory.create_llm.return_value = llm
    mock_find.return_value = [{"key": "k", "value": "v"}]
    resp = qv.get_similar_examples_for_agent(
        _post(rf, {"query": "show sales"}, data))     # org from token
    assert resp.status_code == 200
    assert json.loads(resp.content)["examples"][0]["key"] == "k"


# --------------------------- add_prompt_example --------------------------

@pytest.mark.django_db
def test_add_prompt_example_org_not_found_is_404(rf, data):
    resp = qv.add_prompt_example(
        _post(rf, {"org_id": 999999, "key": "k", "value": "v"}, data))
    assert resp.status_code == 404


@pytest.mark.django_db
@patch("terno_dbi.core.query_service.views.sync_prompt_example")
def test_add_prompt_example_success(mock_sync, rf, data):
    resp = qv.add_prompt_example(
        _post(rf, {"key": "k", "value": "v", "user_id": data["user"].id}, data))
    assert resp.status_code == 200
    body = json.loads(resp.content)
    assert body["example"]["key"] == "k"
    mock_sync.assert_called_once()


# ---------------- body-datasource resolution & error paths ---------------

@pytest.mark.django_db
@patch("terno_dbi.core.query_service.views.execute_streaming_query")
@patch("terno_dbi.core.query_service.views.generate_native_sql")
@patch("terno_dbi.core.query_service.views.prepare_mdb")
def test_stream_query_resolves_datasource_from_body(mock_mdb, mock_gen, mock_stream, rf, data):
    mock_mdb.return_value = MagicMock()
    mock_gen.return_value = {"status": "success", "native_sql": "SELECT 1"}
    mock_stream.return_value = iter(['{"a":1}\n'])
    request = _post(rf, {"datasource": data["ds"].id, "sql": "SELECT 1"}, data,
                    resolved=False)
    resp = qv.stream_query(request)      # no path identifier -> body resolution
    assert resp.status_code == 200


@pytest.mark.django_db
def test_stream_query_access_denied_is_403(rf, data):
    from terno_dbi.core.models import CoreOrganisation
    other_org = CoreOrganisation.objects.create(
        name="Other", subdomain="qx-other", owner=data["user"])
    other = DataSource.objects.create(
        display_name="other_db", type="postgres",
        connection_str="postgresql://localhost/o", enabled=True, organisation=other_org)
    request = _post(rf, {"datasource": other.id, "sql": "SELECT 1"}, data, resolved=False)
    resp = qv.stream_query(request)
    assert resp.status_code == 403


@pytest.mark.django_db
@patch("terno_dbi.core.query_service.views.prepare_mdb", side_effect=RuntimeError("boom"))
def test_stream_query_unexpected_error_is_500(mock_mdb, rf, data):
    resp = qv.stream_query(_post(rf, {"sql": "SELECT 1"}, data), data["ds"].id)
    assert resp.status_code == 500


@pytest.mark.django_db
def test_export_query_access_denied_is_403(rf, data):
    from terno_dbi.core.models import CoreOrganisation
    other_org = CoreOrganisation.objects.create(
        name="Other2", subdomain="qx-other2", owner=data["user"])
    other = DataSource.objects.create(
        display_name="other_db2", type="postgres",
        connection_str="postgresql://localhost/o2", enabled=True, organisation=other_org)
    request = _post(rf, {"datasource": other.id, "sql": "SELECT 1"}, data, resolved=False)
    resp = qv.export_query(request)
    assert resp.status_code == 403


@pytest.mark.django_db
@patch("terno_dbi.core.query_service.views.prepare_mdb", side_effect=RuntimeError("boom"))
def test_export_query_unexpected_error_is_500(mock_mdb, rf, data):
    resp = qv.export_query(_post(rf, {"sql": "SELECT 1"}, data), data["ds"].id)
    assert resp.status_code == 500


@pytest.mark.django_db
def test_similar_examples_invalid_json_is_400(rf, data):
    request = rf.post("/x", data="{bad", content_type="application/json")
    request.service_token = data["token"]
    request.allowed_datasources = data["token"].get_accessible_datasources()
    request.token_organisation = data["org"]
    resp = qv.get_similar_examples_for_agent(request)
    assert resp.status_code == 400
