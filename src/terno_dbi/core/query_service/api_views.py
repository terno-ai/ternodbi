"""HTTP endpoints for API-source tools.

These views resolve the caller's organisation and account allowlist, then pass
both to the connector dispatch layer. Account access comes from the token's
groups, never from the request body.

Kept separate from `views.py` so API and SQL tools remain independent while
sharing authentication and datasource resolution.
"""

import json
import logging
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods
from terno_dbi.connectors.api import registry
from terno_dbi.connectors.api.auth import rbac
from terno_dbi.connectors.api.dates import get_today
from terno_dbi.connectors.api.model.errors import ApiError, ErrorCode
from terno_dbi.connectors.api.pipeline.jobs import enqueue_query, get_query_results
from terno_dbi.connectors.api.pipeline.ratelimit import RateLimit
from terno_dbi.connectors.api.model.types import Compare, DateRange, QuerySpec
from terno_dbi.core.query_service.views import _resolve_roles
from terno_dbi.decorators import require_service_auth
from terno_dbi.services.resolver import resolve_for_caller

logger = logging.getLogger(__name__)


# Conservative default limits until per-connector limits are declared. Real
# quotas (Google Ads) come with the connector.
def _rate_limit_for(ds) -> RateLimit:
    """The connector's declared per-(org, source) rate limit, from its catalog.

    0 on an axis means no limit there; a source with no catalog, or all zeros, is
    unlimited — so the guard bites only where a connector declares a real provider
    quota (see ConnectorSpec.rate_limit_per_second / _per_day).
    """
    cat = getattr(ds, "catalog", None)
    per_second = getattr(cat, "rate_limit_per_second", 0) or None
    per_day = getattr(cat, "rate_limit_per_day", 0) or None
    return RateLimit(per_second=per_second, per_day=per_day)


def _resolve_api_datasource(request, identifier):
    """The accessible API datasource for this request, or an ApiError.

    `require_service_auth` normally resolves the URL identifier — by id, name, or
    connector key, scoped to the caller — and sets `request.resolved_datasource`.
    This uses that when present and otherwise resolves through the *same*
    `resolve_for_caller`, so the two paths cannot diverge. It then adds the one
    API-specific guard.
    """
    ds = getattr(request, "resolved_datasource", None)
    if ds is None:
        res = resolve_for_caller(identifier, request.allowed_datasources)
        if res.status == "ambiguous":
            ids = ", ".join(str(m.id) for m in res.matches)
            raise ApiError(
                ErrorCode.UPSTREAM_ERROR,
                f"Several {identifier!r} sources are connected; pass a specific "
                f"id ({ids}).",
            )
        if not res.ok:
            raise ApiError(
                ErrorCode.UPSTREAM_ERROR,
                f"No datasource {identifier!r}. Use the id or name from "
                f"list_datasources.",
            )
        ds = res.datasource

    if not ds.is_api:
        raise ApiError(
            ErrorCode.UPSTREAM_ERROR,
            f"{ds.display_name} is a database; use execute_query, not data_query.",
        )
    return ds


def _err(exc: ApiError, status=400):
    return JsonResponse(exc.to_payload(), status=status)


@require_service_auth()
@require_http_methods(["GET"])
def api_get_today(request):
    return JsonResponse({"status": "success", **get_today(request.GET.get("timezone"))})


@require_service_auth()
@require_http_methods(["GET"])
def api_list_accounts(request, datasource_identifier):
    try:
        ds = _resolve_api_datasource(request, datasource_identifier)
    except ApiError as exc:
        return _err(exc, status=404)
    try:
        connector = registry.build_connector(ds)
        accounts = connector.list_accounts()
    except ApiError as exc:
        return _err(exc)

    permitted = rbac.permitted_accounts(ds, _resolve_roles(request))
    visible = rbac.filter_accounts(accounts, permitted)
    return JsonResponse({
        "status": "success",
        "accounts": [a.as_dict() for a in visible],
        "count": len(visible),
    })


@require_service_auth()
@require_http_methods(["GET"])
def api_list_fields(request, datasource_identifier):
    try:
        ds = _resolve_api_datasource(request, datasource_identifier)
    except ApiError as exc:
        return _err(exc, status=404)
    report_type = request.GET.get("report_type")
    try:
        connector = registry.build_connector(ds)
        fields = connector.list_fields(report_type)
    except ApiError as exc:
        return _err(exc)

    # A source like GA4 returns hundreds of fields; filters keep the response
    # (and the agent's context) manageable.
    #   kind   — 'metric' or 'dimension', because "session" matches ~70
    #            attribution dimensions on top of the handful of session metrics.
    #   filter — case-insensitive, comma-separated, matched against id/name/group.
    kind = (request.GET.get("kind") or "").strip().lower()
    if kind in ("metric", "dimension"):
        fields = [f for f in fields if f.kind == kind]

    terms = [t.strip().lower() for t in (request.GET.get("filter") or "").split(",")
             if t.strip()]
    if terms:
        fields = [
            f for f in fields
            if any(t in (f.id + " " + f.name + " " + (f.group or "")).lower()
                   for t in terms)
        ]

    payload = {
        "status": "success",
        "fields": [f.as_dict() for f in fields],
        "count": len(fields),
    }
    # Surface the valid report types so the agent does not have to guess one and
    # learn it only from an error (as happened with "standard" vs "Default").
    report_types = [r.get("id") for r in (ds.catalog.report_types or [])]
    if report_types:
        payload["report_types"] = report_types
        payload["notes"] = [
            "Pass one of report_types to data_query. Fields flagged "
            "is_non_aggregatable must not be summed across rows."
        ]
    return JsonResponse(payload)


def _build_spec(body) -> QuerySpec:
    """Turn a request body into a QuerySpec, or raise ApiError."""
    date_range = body.get("date_range") or {}
    start, end = date_range.get("start"), date_range.get("end")
    if not (start and end):
        raise ApiError(
            ErrorCode.INVALID_FILTER,
            "date_range with start and end (YYYY-MM-DD) is required. Resolve "
            "relative ranges with get_today first.",
        )
    compare = None
    if body.get("compare"):
        c = body["compare"]
        compare = Compare(
            type=c.get("type", "prev_range"),
            show=c.get("show", "perc_change"),
            start=c.get("start"), end=c.get("end"),
        )
    fields = body.get("fields") or []
    if isinstance(fields, str):
        fields = [f.strip() for f in fields.split(",") if f.strip()]
    accounts = body.get("accounts") or []
    if isinstance(accounts, str):
        accounts = [a.strip() for a in accounts.split(",") if a.strip()]
    return QuerySpec(
        accounts=accounts,
        fields=fields,
        date_range=DateRange(start, end,
                             inclusive_of_today=bool(date_range.get("inclusive_of_today"))),
        report_type=body.get("report_type"),
        settings=body.get("settings") or {},
        filters=body.get("filters"),
        compare=compare,
        timezone=body.get("timezone", "UTC"),
        max_rows=int(body.get("max_rows") or 1000),
    )


@csrf_exempt
@require_service_auth()
@require_http_methods(["POST"])
def api_data_query(request, datasource_identifier):
    try:
        ds = _resolve_api_datasource(request, datasource_identifier)
        body = json.loads(request.body or "{}")
        spec = _build_spec(body)
    except ApiError as exc:
        return _err(exc)
    except (json.JSONDecodeError, ValueError) as exc:
        return _err(ApiError(ErrorCode.INVALID_FILTER, str(exc)))

    # Authorisation resolved here, from the token's groups — never the body.
    permitted = rbac.permitted_accounts(ds, _resolve_roles(request))

    try:
        result = enqueue_query(
            ds, spec,
            connector_factory=registry.build_connector,
            permitted_accounts=permitted,
            rate_limit=_rate_limit_for(ds),
        )
    except ApiError as exc:
        return _err(exc)
    return JsonResponse({"status": "success", **result})


@require_service_auth()
@require_http_methods(["GET"])
def api_query_results(request, query_id):
    org = getattr(request, "token_organisation", None)
    org_id = org.id if org else None
    try:
        result = get_query_results(query_id, org_id=org_id)
    except ApiError as exc:
        return _err(exc, status=404)
    return JsonResponse({"status": "success", **result})
