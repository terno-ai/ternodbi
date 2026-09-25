"""Coverage for save/edit/delete memory endpoints and _check_write_perms.

The memory service layer is mocked; these tests pin the views' request parsing,
org/user resolution, write-permission gate, and error mapping.
"""

import hashlib
import json

import pytest
from unittest.mock import MagicMock, patch
from django.test import RequestFactory

from terno_dbi.core.models import ServiceToken
from terno_dbi.core.query_service import views as qv


@pytest.fixture
def rf():
    return RequestFactory()


@pytest.fixture
def data(db):
    from django.contrib.auth.models import User
    from terno_dbi.core.models import CoreOrganisation

    user = User.objects.create_user("mx-user", "mx@example.com", "pw")
    org = CoreOrganisation.objects.create(name="Acme", subdomain="mx-acme", owner=user)
    token = ServiceToken.objects.create(
        name="M", token_type=ServiceToken.TokenType.QUERY, key_prefix="dbi_query_",
        key_hash=hashlib.sha256(b"mx").hexdigest(), is_active=True,
        created_by=user, created_for=user, organisation=org)
    return {"user": user, "org": org, "token": token}


def _post(rf, body, data, method="POST"):
    raw = body if isinstance(body, str) else json.dumps(body)
    request = getattr(rf, method.lower())("/x", data=raw, content_type="application/json")
    request.service_token = data["token"]
    request.allowed_datasources = data["token"].get_accessible_datasources()
    request.token_organisation = data["org"]
    return request


# ------------------------------ save_memory ------------------------------

@pytest.mark.django_db
def test_save_memory_invalid_json_is_400(rf, data):
    assert qv.save_memory(_post(rf, "{bad", data)).status_code == 400


@pytest.mark.django_db
def test_save_memory_requires_name(rf, data):
    assert qv.save_memory(_post(rf, {"content": "x"}, data)).status_code == 400


@pytest.mark.django_db
@patch("terno_dbi.core.query_service.views.memory_service")
def test_save_memory_success(mock_mem, rf, data):
    mem = MagicMock(name="m", store="user", data_source_id=None, content_hash="h")
    mem.name = "fact-1"
    mock_mem.write_memory.return_value = (mem, "created")
    resp = qv.save_memory(_post(rf, {"name": "fact-1", "content": "hi"}, data))
    assert resp.status_code == 200
    body = json.loads(resp.content)
    assert body["action"] == "created" and body["memory"]["name"] == "fact-1"


@pytest.mark.django_db
@patch("terno_dbi.core.query_service.views.memory_service")
def test_save_memory_conflict_maps_to_409(mock_mem, rf, data):
    from terno_dbi.services.memory import MemoryConflict
    mock_mem.write_memory.side_effect = MemoryConflict("stale hash")
    resp = qv.save_memory(_post(rf, {"name": "f", "content": "x",
                                     "expected_hash": "old"}, data))
    assert resp.status_code == 409


# ---------------------------- write-perm gate ----------------------------

@pytest.mark.django_db
def test_org_store_requires_admin_scope(rf, data):
    # A plain query token lacks admin:write -> org store is refused.
    resp = qv.save_memory(_post(rf, {"name": "f", "content": "x", "store": "org"}, data))
    assert resp.status_code == 403


@pytest.mark.django_db
def test_user_store_needs_a_bound_user(rf, data):
    data["token"].created_for = None      # token not bound to a user
    data["token"].save(update_fields=["created_for"])
    resp = qv.save_memory(_post(rf, {"name": "f", "content": "x"}, data))
    assert resp.status_code == 400


# ------------------------------ edit_memory ------------------------------

@pytest.mark.django_db
def test_edit_memory_requires_old_string(rf, data):
    resp = qv.edit_memory(_post(rf, {"new_string": "b"}, data), "fact-1")
    assert resp.status_code == 400


@pytest.mark.django_db
def test_edit_memory_requires_new_string(rf, data):
    resp = qv.edit_memory(_post(rf, {"old_string": "a"}, data), "fact-1")
    assert resp.status_code == 400


@pytest.mark.django_db
@patch("terno_dbi.core.query_service.views.memory_service")
def test_edit_memory_success(mock_mem, rf, data):
    mem = MagicMock(content_hash="h2")
    mem.name = "fact-1"
    mock_mem.edit_memory.return_value = mem
    resp = qv.edit_memory(
        _post(rf, {"old_string": "a", "new_string": "b"}, data), "fact-1")
    assert resp.status_code == 200
    assert json.loads(resp.content)["memory"]["content_hash"] == "h2"


@pytest.mark.django_db
@patch("terno_dbi.core.query_service.views.memory_service")
def test_edit_memory_not_found_maps_to_404(mock_mem, rf, data):
    from terno_dbi.services.memory import MemoryNotFound
    mock_mem.edit_memory.side_effect = MemoryNotFound("nope")
    resp = qv.edit_memory(
        _post(rf, {"old_string": "a", "new_string": "b"}, data), "gone")
    assert resp.status_code == 404


# ----------------------------- delete_memory -----------------------------

@pytest.mark.django_db
@patch("terno_dbi.core.query_service.views.memory_service")
def test_delete_memory_success(mock_mem, rf, data):
    mock_mem.delete_memory.return_value = 1
    resp = qv.delete_memory(_post(rf, {}, data, method="POST"), "fact-1")
    assert resp.status_code == 200
    assert json.loads(resp.content)["removed"] == 1


@pytest.mark.django_db
@patch("terno_dbi.core.query_service.views.memory_service")
def test_delete_memory_not_found_is_404(mock_mem, rf, data):
    mock_mem.delete_memory.return_value = 0
    resp = qv.delete_memory(_post(rf, {}, data, method="POST"), "gone")
    assert resp.status_code == 404


@pytest.mark.django_db
@patch("terno_dbi.core.query_service.views.memory_service")
def test_delete_memory_tolerates_bad_body(mock_mem, rf, data):
    mock_mem.delete_memory.return_value = 1
    resp = qv.delete_memory(_post(rf, "{bad", data, method="POST"), "fact-1")
    assert resp.status_code == 200      # bad body ignored, defaults applied
