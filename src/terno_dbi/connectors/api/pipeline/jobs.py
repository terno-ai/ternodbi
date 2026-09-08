"""Manage the async query lifecycle: enqueue, execute, and poll.

`data_query` enqueues an `ApiQueryJob` and returns its ID immediately.
`get_query_results` polls the job until it reaches a terminal state. The same
contract works with synchronous or background executors.

Jobs are always accessed through their owning organisation. A mismatched
organisation returns not-found, preventing a job ID from being used to access
another tenant's results.
"""

from __future__ import annotations
import logging
import secrets
from typing import Any, Callable, Dict, Iterable, Optional
from terno_dbi.connectors.api.model.base import ApiConnector
from terno_dbi.connectors.api.pipeline.dispatch import run_query
from terno_dbi.connectors.api.model.errors import ApiError, ErrorCode
from terno_dbi.connectors.api.pipeline.executor import get_executor
from terno_dbi.connectors.api.pipeline.ratelimit import RateLimit
from terno_dbi.connectors.api.model.types import QuerySpec

logger = logging.getLogger(__name__)

# A connector factory is injected rather than imported, so this module stays
# free of any concrete connector and its provider SDK.
ConnectorFactory = Callable[[Any], ApiConnector]


def _new_job_id() -> str:
    return "q_" + secrets.token_hex(12)


def enqueue_query(
    datasource,
    spec: QuerySpec,
    *,
    connector_factory: ConnectorFactory,
    permitted_accounts: Optional[Iterable[str]],
    rate_limit: Optional[RateLimit] = None,
) -> Dict[str, Any]:
    """Create a job, submit it, and return `{query_id, status}` at once."""
    from terno_dbi.core.models import ApiQueryJob

    permitted = None if permitted_accounts is None else list(permitted_accounts)

    job = ApiQueryJob.objects.create(
        id=_new_job_id(),
        organisation=getattr(datasource, "organisation", None),
        data_source=datasource,
        status=ApiQueryJob.Status.PENDING,
        spec=spec.as_dict(),
    )

    def run() -> None:
        _execute_job(
            job.id, datasource, spec,
            connector_factory=connector_factory,
            permitted_accounts=permitted,
            rate_limit=rate_limit,
        )

    get_executor().submit(job.id, run)
    job.refresh_from_db()
    return {"query_id": job.id, "status": job.status}


def _execute_job(
    job_id: str,
    datasource,
    spec: QuerySpec,
    *,
    connector_factory: ConnectorFactory,
    permitted_accounts: Optional[list],
    rate_limit: Optional[RateLimit],
) -> None:
    """Run one job, recording its outcome. Never raises — failure is a status."""
    from terno_dbi.core.models import ApiQueryJob

    job = ApiQueryJob.objects.get(id=job_id)
    job.status = ApiQueryJob.Status.RUNNING
    job.save(update_fields=["status", "updated_at"])

    try:
        connector = connector_factory(datasource)
        payload = run_query(
            connector, spec,
            org_id=job.organisation_id,
            permitted_accounts=permitted_accounts,
            rate_limit=rate_limit,
        )
        job.result = payload
        job.status = ApiQueryJob.Status.COMPLETED
    except ApiError as exc:
        job.error = exc.to_payload()["error"]
        job.status = ApiQueryJob.Status.FAILED
    except Exception as exc:  # noqa: BLE001 - never let a provider bug hang a job
        logger.exception("Unexpected error running query job %s", job_id)
        job.error = {
            "code": ErrorCode.UPSTREAM_ERROR,
            "message": "The query failed unexpectedly.",
            "retriable": True,
        }
        job.status = ApiQueryJob.Status.FAILED
    finally:
        job.save(update_fields=["result", "error", "status", "updated_at"])


def get_query_results(query_id: str, *, org_id) -> Dict[str, Any]:
    """Poll a job, scoped to its owning organisation.

    A mismatched org is indistinguishable from a missing job on purpose: a job
    id must not confirm the existence of another tenant's query (§7).
    """
    from terno_dbi.core.models import ApiQueryJob

    job = ApiQueryJob.objects.filter(id=query_id).first()
    if job is None or job.organisation_id != org_id:
        raise ApiError(
            ErrorCode.UPSTREAM_ERROR,
            f"No query found with id {query_id!r}.",
        )

    response: Dict[str, Any] = {"query_id": job.id, "status": job.status}
    if job.status == ApiQueryJob.Status.COMPLETED:
        response.update(job.result or {})
    elif job.status == ApiQueryJob.Status.FAILED:
        response["success"] = False
        response["error"] = job.error
    else:
        response["notes"] = ["Query is still running; poll get_query_results again."]
    return response


__all__ = ["enqueue_query", "get_query_results"]
