"""POST /connectors/api/requests/ — users asking for a new connector."""

import json

import pytest
from django.test import RequestFactory

from terno_dbi.connectors.api import web
from terno_dbi.connectors.api.signals import connector_requested


@pytest.fixture
def org(db):
    from django.contrib.auth.models import User
    from terno_dbi.core.models import CoreOrganisation, OrganisationUser

    owner = User.objects.create_user("cr-owner", "o@example.com", "pw")
    org = CoreOrganisation.objects.create(name="Acme", subdomain="cr-acme", owner=owner)
    OrganisationUser.objects.create(user=owner, organisation=org)
    return org


@pytest.fixture
def member(db, org):
    from django.contrib.auth.models import User
    from terno_dbi.core.models import OrganisationUser

    u = User.objects.create_user("cr-member", "m@example.com", "pw")
    OrganisationUser.objects.create(user=u, organisation=org)
    return u


def _post(user, org, body):
    raw = body if isinstance(body, str) else json.dumps(body)
    request = RequestFactory().post("/connectors/api/requests/", data=raw,
                                    content_type="application/json")
    request.user = user
    request.org_id = org.id
    return request


@pytest.mark.django_db(transaction=True)
def test_member_request_is_saved_and_signal_fires(org, member):
    from terno_dbi.core.models import ConnectorRequest

    received = []

    def handler(sender, request, **kw):
        received.append(request.pk)

    connector_requested.connect(handler)
    try:
        resp = web.request_connector(_post(member, org, {
            "connector_name": "  TikTok Ads ", "use_case": "Track spend"}))
    finally:
        connector_requested.disconnect(handler)

    assert resp.status_code == 201
    row = ConnectorRequest.objects.get()
    assert row.connector_name == "TikTok Ads"
    assert row.use_case == "Track spend"
    assert row.user == member
    assert row.email_snapshot == "m@example.com"
    assert row.organisation == org
    assert row.created_at is not None
    assert received == [row.pk]


@pytest.mark.django_db
def test_use_case_is_optional(org, member):
    from terno_dbi.core.models import ConnectorRequest
    resp = web.request_connector(_post(member, org, {"connector_name": "Pinterest"}))
    assert resp.status_code == 201
    assert ConnectorRequest.objects.get().use_case == ""


@pytest.mark.django_db
@pytest.mark.parametrize("body", [{}, {"connector_name": "   "}, {"connector_name": "x" * 121},
                                  {"connector_name": "X", "use_case": "y" * 2001}])
def test_invalid_input_is_400(org, member, body):
    from terno_dbi.core.models import ConnectorRequest
    resp = web.request_connector(_post(member, org, body))
    assert resp.status_code == 400
    assert not ConnectorRequest.objects.exists()


@pytest.mark.django_db
def test_malformed_json_is_400(org, member):
    assert web.request_connector(_post(member, org, "{nope")).status_code == 400
    assert web.request_connector(_post(member, org, "[1]")).status_code == 400


@pytest.mark.django_db
def test_non_member_is_403(org):
    from django.contrib.auth.models import User
    outsider = User.objects.create_user("cr-out", "x@example.com", "pw")
    resp = web.request_connector(_post(outsider, org, {"connector_name": "X"}))
    assert resp.status_code == 403


@pytest.mark.django_db
def test_rate_limited_per_user(org, member):
    for i in range(web.management_service.REQUEST_LIMIT_PER_DAY):
        assert web.request_connector(
            _post(member, org, {"connector_name": f"C{i}"})).status_code == 201
    resp = web.request_connector(_post(member, org, {"connector_name": "One more"}))
    assert resp.status_code == 429
