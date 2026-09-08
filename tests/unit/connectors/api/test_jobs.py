"""The async query lifecycle (§6.4) and its org-scoped polling (§7)."""

import pytest
from django.contrib.auth.models import User
from django.core.cache import cache

from terno_dbi.connectors.api.model.base import ApiConnector
from terno_dbi.connectors.api.model.errors import ApiError, ErrorCode
from terno_dbi.connectors.api.pipeline.jobs import enqueue_query, get_query_results
from terno_dbi.connectors.api.model.types import (
    Account,
    DateRange,
    Field,
    QueryResult,
    QuerySpec,
)
from terno_dbi.core.models import ApiQueryJob, ConnectorCatalog, CoreOrganisation, DataSource
from terno_dbi.catalog.refresh import refresh_catalog


class _Catalog:
    key = "fake"
    report_types = [{"id": "Default", "settings": []}]
    has_report_types = True


class OkConnector(ApiConnector):
    def list_accounts(self):
        return [Account("1", "One")]

    def list_fields(self, report_type=None):
        return [Field("sessions", "Sessions", "metric")]

    def _run(self, spec):
        return QueryResult(list(spec.fields), [{"sessions": 5}], 1)


class BoomConnector(ApiConnector):
    def list_accounts(self):
        return []

    def list_fields(self, report_type=None):
        return []

    def _run(self, spec):
        raise ApiError(ErrorCode.UPSTREAM_ERROR, "provider exploded")


@pytest.fixture
def org(db):
    user = User.objects.create_user("jobuser", "j@x.com", "pw")
    return CoreOrganisation.objects.create(name="Acme", subdomain="acme", owner=user)


@pytest.fixture
def datasource(db, org):
    refresh_catalog()
    return DataSource.objects.create(
        display_name="GA4", type="googleanalytics4", connection_str="",
        organisation=org,
        catalog=ConnectorCatalog.objects.get(key="googleanalytics4"),
    )


def _spec():
    return QuerySpec(
        accounts=["1"], fields=["sessions"],
        date_range=DateRange("2020-01-01", "2020-01-31"),
        report_type="Default",
    )


@pytest.fixture(autouse=True)
def clear_cache():
    cache.clear()
    yield
    cache.clear()


@pytest.mark.django_db
class TestLifecycle:
    def test_enqueue_returns_a_query_id(self, datasource):
        out = enqueue_query(
            datasource, _spec(),
            connector_factory=lambda ds: OkConnector(ds),
            permitted_accounts=["1"],
        )
        assert out["query_id"].startswith("q_")

    def test_synchronous_executor_completes_before_polling(self, datasource, org):
        out = enqueue_query(
            datasource, _spec(),
            connector_factory=lambda ds: OkConnector(ds),
            permitted_accounts=["1"],
        )
        result = get_query_results(out["query_id"], org_id=org.id)
        assert result["status"] == ApiQueryJob.Status.COMPLETED
        assert result["success"] is True
        assert result["rows"] == [{"sessions": 5}]

    def test_provider_failure_becomes_a_failed_status_not_an_exception(
        self, datasource, org
    ):
        out = enqueue_query(
            datasource, _spec(),
            connector_factory=lambda ds: BoomConnector(ds),
            permitted_accounts=None,
        )
        result = get_query_results(out["query_id"], org_id=org.id)
        assert result["status"] == ApiQueryJob.Status.FAILED
        assert result["success"] is False
        assert result["error"]["code"] == ErrorCode.UPSTREAM_ERROR

    def test_forbidden_account_fails_the_job(self, datasource, org):
        out = enqueue_query(
            datasource, _spec(),
            connector_factory=lambda ds: OkConnector(ds),
            permitted_accounts=["999"],       # "1" is not permitted
        )
        result = get_query_results(out["query_id"], org_id=org.id)
        assert result["status"] == ApiQueryJob.Status.FAILED
        assert result["error"]["code"] == ErrorCode.ACCOUNT_FORBIDDEN


@pytest.mark.django_db
class TestPollingIsOrgScoped:
    def test_another_org_cannot_read_the_result(self, datasource, org):
        out = enqueue_query(
            datasource, _spec(),
            connector_factory=lambda ds: OkConnector(ds),
            permitted_accounts=["1"],
        )
        # A job id must not confirm the existence of another tenant's query.
        with pytest.raises(ApiError) as exc:
            get_query_results(out["query_id"], org_id=99999)
        assert "No query found" in exc.value.message

    def test_unknown_id_is_indistinguishable_from_forbidden(self, org):
        with pytest.raises(ApiError):
            get_query_results("q_doesnotexist", org_id=org.id)
