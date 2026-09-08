"""The dispatch layer: everything that wraps a single API query.

This is where the cross-cutting concerns compose, in a fixed order, *around* a
connector's `query()`. A connector stays a thin provider adapter; the policy
lives here, in one place, so it cannot be forgotten or reimplemented per source.

Order matters and is deliberate:

    1. authorise accounts   — the caller may only query permitted accounts (§7)
    2. rate limit           — reject a storm before it touches cache or provider
    3. cache lookup         — a hit skips the provider (and its quota) entirely
    4. connector.query      — validate settings, then the real request
    5. cache store          — with a TTL chosen from whether the range is closed

Token freshness is *not* a step here: the connector refreshes its own token on
each provider call (`ApiConnector.access_token`), so a cache hit costs no
refresh and no path can forget one.

Authorisation is first because nothing else — not even a cache read — should
happen for an account the caller cannot see. The full account-allowlist model is
Phase 3; the seam is here now, as an explicit `permitted_accounts` argument the
tool layer must resolve from the caller's groups. Passing `None` means
"unrestricted", which is correct only for a trusted internal caller and must
never be defaulted from request data.
"""

from __future__ import annotations
import logging
from typing import Any, Dict, Iterable, Optional
from dataclasses import replace
from terno_dbi.connectors.api.pipeline import cache as result_cache
from terno_dbi.connectors.api.model.base import ApiConnector
from terno_dbi.connectors.api.pipeline.compare import add_comparison, resolve_compare_range
from terno_dbi.connectors.api.model.errors import ApiError, ErrorCode
from terno_dbi.connectors.api.pipeline.ratelimit import RateLimit, check_rate_limit
from terno_dbi.connectors.api.model.types import Field, QuerySpec

logger = logging.getLogger(__name__)


def _apply_default_report_type(connector: ApiConnector, spec: QuerySpec) -> QuerySpec:
    """Fill in the connector's declared default report type when none was given.

    A no-op unless the query omitted `report_type` *and* the connector declared a
    `default_report_type`. GA4 declares "Default"; YouTube declares none, so a
    caller there must still choose — the policy is per-connector, never assumed.
    """
    if spec.report_type:
        return spec
    catalog = getattr(connector, "catalog", None)
    default = getattr(catalog, "default_report_type", "") or ""
    if default:
        return replace(spec, report_type=default)
    return spec


def _authorise_accounts(
    spec: QuerySpec, permitted_accounts: Optional[Iterable[str]]
) -> None:
    """Reject any requested account the caller may not see.

    `None` is unrestricted — a trusted internal caller only. An empty iterable
    means "no accounts permitted", which correctly denies everything: an empty
    allowlist must never read as "all" (§7).
    """
    if permitted_accounts is None:
        return
    permitted = set(permitted_accounts)
    forbidden = [a for a in spec.accounts if a not in permitted]
    if forbidden:
        raise ApiError(
            ErrorCode.ACCOUNT_FORBIDDEN,
            "Not permitted to query account(s): " + ", ".join(forbidden) + ".",
            details={"forbidden": forbidden},
        )


def _field_meta(connector: ApiConnector, report_type: Optional[str]) -> Dict[str, Field]:
    """`{field_id: Field}`, or empty if the source cannot report it.

    Used to classify dimensions vs metrics for comparison and to spot
    non-aggregatable metrics for the safety note. A *real* provider failure
    (`ApiError`, e.g. an expired token) propagates — the query would fail on the
    same auth anyway, and surfacing it here gives the honest error rather than a
    query that proceeds blind and fails confusingly. Only an unexpected error
    degrades to empty metadata, and it is logged with a traceback so a bug is
    never invisible.
    """
    try:
        return {f.id: f for f in connector.list_fields(report_type)}
    except ApiError:
        raise
    except Exception:   # noqa: BLE001
        logger.warning("Could not load field metadata for %s",
                       connector.key, exc_info=True)
        return {}


def _aggregation_notes(spec: QuerySpec, meta: Dict[str, Field]) -> List[str]:
    """Warn when a result contains a metric that must not be summed (§6.7)."""
    offenders = [
        meta[f].name for f in spec.fields
        if f in meta and meta[f].is_non_aggregatable
    ]
    if not offenders:
        return []
    return [
        "These metrics are pre-aggregated at the source and must not be summed "
        "or averaged across rows: " + ", ".join(offenders) + ". Each returned "
        "row is correct on its own; for a coarser total, request that grouping "
        "in the query instead."
    ]


def _check_currency(spec: QuerySpec, connector: ApiConnector, meta: Dict[str, Field]) -> None:
    """Refuse to compare/aggregate monetary metrics across mixed currencies (§6.9).

    A monetary metric summed across accounts in different currencies produces a
    number that looks authoritative and is meaningless. Only checked when it can
    actually happen: a monetary field, more than one account.
    """
    if len(spec.accounts) < 2:
        return
    monetary = [f for f in spec.fields if f in meta and meta[f].is_monetary]
    if not monetary:
        return
    # No try/except: this guards the correctness of aggregated money. If we
    # cannot list accounts to verify their currencies, we must not silently
    # proceed and risk summing mixed currencies — let the error propagate.
    wanted = set(spec.accounts)
    currencies = {
        a.currency for a in connector.list_accounts()
        if a.id in wanted and a.currency
    }
    if len(currencies) > 1:
        raise ApiError(
            ErrorCode.MIXED_CURRENCY,
            "Cannot combine monetary metrics across accounts with different "
            f"currencies ({', '.join(sorted(currencies))}). Query one currency "
            "at a time.",
            details={"currencies": sorted(currencies), "metrics": monetary},
        )


def run_query(
    connector: ApiConnector,
    spec: QuerySpec,
    *,
    org_id,
    permitted_accounts: Optional[Iterable[str]],
    rate_limit: Optional[RateLimit] = None,
    use_cache: bool = True,
) -> Dict[str, Any]:
    """Run one query through the full pipeline. Returns a result payload.

    Raises `ApiError` on any policy or provider failure; the caller (the job
    runner, or a synchronous tool) turns that into the response envelope.

    Token freshness is not handled here — the connector refreshes its own token
    on each provider call (`ApiConnector.access_token`), so a cache hit never
    triggers a refresh and no path can forget one.
    """
    source_key = connector.key

    # 0. apply the connector's declared default report type when none was given.
    # Done here, before caching, so the cache key and the provider call agree on
    # the report type. Whether a source *has* a default is the connector's own
    # policy (`catalog.default_report_type`) — the pipeline imposes none.
    spec = _apply_default_report_type(connector, spec)

    # 1. authorise — before anything else touches the account.
    _authorise_accounts(spec, permitted_accounts)

    # 2. rate limit — a storm must not even reach the cache.
    if rate_limit is not None:
        check_rate_limit(source_key, org_id, rate_limit)

    # 3. cache — a hit skips the provider and its quota.
    if use_cache:
        cached = result_cache.get_cached(source_key, org_id, spec)
        if cached is not None:
            payload = dict(cached)
            payload["cache_hit"] = True
            return payload

    meta = _field_meta(connector, spec.report_type)
    _check_currency(spec, connector, meta)

    # 4. the base request (validates settings inside connector.query).
    result = connector.query(spec)
    notes = list(result.notes) + _aggregation_notes(spec, meta)
    warnings = list(result.warnings)

    payload = result.as_dict()

    # 4a. period comparison — a second period, aligned to the first (§6.9).
    if spec.compare is not None:
        compare_range = resolve_compare_range(spec.date_range, spec.compare)
        compare_spec = replace(spec, date_range=compare_range, compare=None)
        compare_result = connector.query(compare_spec)

        plain_dims = [
            f for f in spec.fields
            if f in meta and meta[f].kind == "dimension"
            and meta[f].data_type != "date"
        ]
        date_dims = [
            f for f in spec.fields
            if f in meta and meta[f].data_type == "date"
        ]
        metrics = [
            f for f in spec.fields
            if f not in meta or meta[f].kind == "metric"
        ]
        merged, cmp_warnings = add_comparison(
            result.rows, compare_result.rows,
            plain_dimensions=plain_dims, date_dimensions=date_dims,
            metrics=metrics,
            base_start=spec.date_range.start,
            compare_start=compare_range.start,
            show=spec.compare.show,
        )
        payload["rows"] = merged
        payload["row_count"] = len(merged)
        payload["compare"] = {
            "type": spec.compare.type,
            "show": spec.compare.show,
            "range": compare_range.as_dict(),
        }
        warnings += cmp_warnings
        if spec.date_range.inclusive_of_today:
            payload["partial_period"] = True
            warnings.append(
                "The base range includes today, a partial period; the "
                "comparison is not like-for-like."
            )

    payload["notes"] = notes
    payload["warnings"] = warnings
    payload["success"] = True
    payload["cache_hit"] = False

    # 6. store, TTL chosen from whether the range can still change.
    if use_cache:
        result_cache.set_cached(source_key, org_id, spec, payload)

    return payload


__all__ = ["run_query"]
