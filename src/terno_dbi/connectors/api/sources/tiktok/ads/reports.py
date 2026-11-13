"""Running TikTok Ads reports."""

from __future__ import annotations

import csv
import io
import json
from typing import Any, Dict, List
from datetime import date

from terno_dbi.connectors.api.model.errors import (
    ApiError,
    ErrorCode,
    invalid_field,
)
from terno_dbi.connectors.api.model.types import QueryResult, QuerySpec
from terno_dbi.connectors.api.sources.tiktok.ads.smart_plus_spec import (
    SMART_PLUS_REPORT_DIMENSIONS,
)
from terno_dbi.connectors.api.sources.tiktok.ads.fields import (
    _ATTR_LEVEL,
    _ATTR_ONLY_LEVEL,
    _AUDIENCE_ATTRS,
    _AUDIENCE_METRIC_IDS,
    _AUDIENCE_ONLY,
    _DEFAULT_REPORT,
    _GROUP_OF,
    _LEVELS,
    _LEVEL_ID,
    _MAX_TOTAL_DAYS,
    _METRIC_FLAGS,
    _SP_REPORTS,
    _SP_REPORT_FILTERS,
    _TIME_SPLITS,
    _TYPED_REPORTS,
    _api_words,
    _api_words_dict,
    _fields_for,
    _parse_report_row,
)
from terno_dbi.connectors.api.sources.tiktok.ads import (
    gmv_max_spec as _GMV,
    report_rules as _RR,
)
from terno_dbi.connectors.api.sources.tiktok.ads.listings import (
    _LISTINGS,
    _filter_from_settings,
    _lookup,
)
from terno_dbi.connectors.api.sources.tiktok._api import _RAW_TEXT, _windows
from terno_dbi.connectors.api.sources._multi import gather_accounts


# Polling for async report tasks. TikTok sets no timeout; these are ours.
_ASYNC_FIRST_POLL = 2.0


_ASYNC_MAX_POLL = 20.0


_ASYNC_MAX_WAIT = 90.0


# Async reports can't combine these dimensions with these unique-user
# metrics (lists from the report/task/create/ docs).
_ASYNC_AUDIENCE_DIMS = frozenset({
    "gender", "age", "country_code", "ac", "language", "platform",
    "interest_category", "placement", "contextual_tag"})


_UV_METRICS = frozenset({
    "average_video_play_per_user", "frequency", "cost_per_1000_reached",
    "real_time_result", "real_time_result_rate", "real_time_cost_per_result",
    "result", "result_rate", "cost_per_result", "reach", "live_play_count"})


def _check_report_mode(run_async: bool, report: str, level: int,
                       dims: List[str], metrics: List[str], lifetime: bool,
                       include_deleted: bool) -> None:
    """Reject combinations TikTok documents as unsupported in this mode."""

    def refuse(msg):
        raise ApiError(ErrorCode.INVALID_FILTER, msg, retriable=False)

    adv_only = [m for m in metrics if "adv" in _METRIC_FLAGS.get(m, ())]
    if adv_only and level > 0:
        refuse(f"{', '.join(adv_only)} {'is' if len(adv_only) == 1 else 'are'}"
               f" advertiser-level only (TikTok); drop the campaign/ad group/"
               f"ad columns.")
    async_only = [m for m in metrics if "async" in _METRIC_FLAGS.get(m, ())]
    if not run_async:
        if async_only:
            refuse(f"{', '.join(async_only)} {'is' if len(async_only) == 1 else 'are'}"
                   f" only in TikTok's asynchronous reports; set the "
                   f"async_report setting to true.")
        return
    sync_only = [m for m in metrics if m in _RR.BASIC_SYNC_ONLY]
    if sync_only or "minis_id" in dims:
        refuse(f"{', '.join(sync_only or ['minis_id'])} only work in "
               f"synchronous reports (TikTok); turn async_report off.")
    if include_deleted:
        refuse("Async reports only filter by campaign/ad group/ad ids "
               "(TikTok), so include_deleted needs a synchronous report.")
    aud = [d for d in dims if d in _ASYNC_AUDIENCE_DIMS]
    uv = [m for m in metrics if m in _UV_METRICS]
    if aud and uv:
        refuse(f"Async reports can't combine UV metrics ({', '.join(uv)}) "
               f"with audience splits ({', '.join(aud)}) (TikTok).")
    if lifetime and (report == "AUDIENCE" or "country_code" in dims):
        refuse("Lifetime totals can't be split by audience or country "
               "(TikTok).")


class ReportsMixin:
    """Report methods of TikTokAdsConnector."""

    def _run(self, spec: QuerySpec) -> QueryResult:
        report_type = spec.report_type or _DEFAULT_REPORT
        catalogue = _fields_for(report_type)

        unknown = [f for f in spec.fields if f not in catalogue]
        if unknown:
            raise invalid_field(unknown[0], list(catalogue.keys()))

        if report_type in _GMV.REPORTS:
            return self._run_gmv_report(spec, report_type, catalogue)
        if report_type in _TYPED_REPORTS:
            return self._run_typed_report(spec, report_type, catalogue)
        if report_type in _SP_REPORTS:
            return self._run_sp_report(spec, report_type, catalogue)
        if report_type in _LISTINGS:
            return self._run_entities(spec, report_type)
        return self._run_report(spec, catalogue)

    def _run_sp_report(self, spec: QuerySpec, report_type: str,
                       catalogue) -> QueryResult:
        """Upgraded Smart+ creative overview or breakdown report."""
        dims = [f for f in spec.fields if catalogue[f].kind == "dimension"]
        metrics = [f for f in spec.fields if catalogue[f].kind == "metric"]
        if not dims:
            allowed = SMART_PLUS_REPORT_DIMENSIONS[_SP_REPORTS[report_type]]
            raise ApiError(
                ErrorCode.INVALID_FILTER,
                f"{report_type} needs at least one dimension: "
                f"{', '.join(allowed)}.", retriable=False)
        settings = spec.settings or {}
        filtering = {
            key: [v.strip() for v in str(settings[key]).split(",") if v.strip()]
            for key in _SP_REPORT_FILTERS if settings.get(key)}
        params: Dict[str, Any] = {
            "dimensions": json.dumps(dims),
            "start_date": spec.date_range.start,
            "end_date": spec.date_range.end,
        }
        if metrics:
            params["metrics"] = json.dumps(metrics)
        if filtering:
            params["filtering"] = json.dumps(filtering)
        requested = list(spec.fields)
        multi = len(spec.accounts) > 1

        def fetch(account):
            items = self._pages(_SP_REPORTS[report_type],
                                {**params, "advertiser_id": account},
                                spec.max_rows, page_size=100)
            return [_parse_report_row(item, requested, catalogue, account,
                                      multi=multi) for item in items]

        rows, warnings = gather_accounts(spec.accounts, fetch)
        return QueryResult(requested_field_ids=requested, rows=rows,
                           row_count=len(rows), warnings=warnings)

    def _run_report(self, spec: QuerySpec, catalogue) -> QueryResult:
        """Performance report (BASIC or AUDIENCE).

        The level comes from the most detailed attribute requested, and any
        audience column (age, gender, …) turns it into an AUDIENCE report.
        Combinations TikTok doesn't support are refused before the call.
        """
        metrics = [f for f in spec.fields if catalogue[f].kind == "metric"]
        dims = [f for f in spec.fields if catalogue[f].kind == "dimension"]
        if not (metrics or dims):
            metrics = ["spend", "impressions", "clicks"]
        settings = spec.settings or {}

        times = [d for d in dims if d in _TIME_SPLITS]
        if len(times) > 1:
            raise ApiError(
                ErrorCode.INVALID_FILTER,
                "date and hour can't be combined: TikTok splits a report by "
                "one time column. Pick one.")
        attrs = [d for d in dims if d in _ATTR_LEVEL or d in _AUDIENCE_ATTRS]
        plain = [d for d in dims if d not in _TIME_SPLITS and d not in attrs]
        audience = any(d in _AUDIENCE_ONLY for d in plain) or any(
            d in _AUDIENCE_ATTRS for d in attrs)

        if "ad_id" in dims and "ad_id_v2" in dims:
            raise ApiError(ErrorCode.INVALID_FILTER,
                           "ad_id and ad_id_v2 can't be combined (TikTok).")
        level = max((_ATTR_LEVEL.get(d, 0) for d in attrs), default=0)
        for d in attrs:
            only = _ATTR_ONLY_LEVEL.get(d)
            if only is not None and only != level:
                raise ApiError(
                    ErrorCode.INVALID_FILTER,
                    f"{d} is only available at {_LEVELS[only]} level, but "
                    f"the other columns need {_LEVELS[level]}.")
        id_dim = "ad_id_v2" if (level == 3 and "ad_id_v2" in dims) \
            else _LEVEL_ID[level]
        attr_metrics = [d for d in attrs if d != id_dim]

        if audience:
            allowed = _AUDIENCE_METRIC_IDS
            bad = [m for m in [*metrics, *attr_metrics] if m not in allowed]
            if bad:
                raise ApiError(
                    ErrorCode.INVALID_FILTER,
                    f"Audience splits ({', '.join(d for d in plain if d in _AUDIENCE_ONLY)}) "
                    f"use TikTok's AUDIENCE report, which doesn't support: "
                    f"{', '.join(bad[:10])}{'…' if len(bad) > 10 else ''}.",
                    details={"unsupported": bad})

        # Some dimensions (page_id, room_id, …) can't be grouped with an ID,
        # so drop it when no attribute needs it.
        report = "AUDIENCE" if audience else "BASIC"
        time_dims = [_TIME_SPLITS[t][0] for t in times]
        candidates = [[id_dim, *time_dims, *plain]]
        if not attrs and (time_dims or plain):
            candidates.append([*time_dims, *plain])
        api_dims: List[str] = []
        err = None
        for cand in candidates:
            err = _RR.grouping_error(report, cand)
            if err is None:
                api_dims = cand
                break
        if err is not None:
            raise ApiError(ErrorCode.INVALID_FILTER, _api_words(err),
                           retriable=False)
        bad = _RR.unsupported_metrics(report, api_dims, metrics, _GROUP_OF)
        if bad:
            raise ApiError(
                ErrorCode.INVALID_FILTER,
                "TikTok doesn't support these metrics with the chosen "
                "columns: " + "; ".join(
                    f"{m} ({why})" for m, why in list(bad.items())[:10])
                + ("…" if len(bad) > 10 else "") + ".",
                details={"unsupported": _api_words_dict(bad)},
                retriable=False)
        api_metrics = list(dict.fromkeys([*metrics, *attr_metrics])) or ["spend"]

        lifetime = str(settings.get("query_lifetime", "")).lower() == "true"
        if lifetime and times:
            raise ApiError(ErrorCode.INVALID_FILTER,
                           "Lifetime totals can't be split by date or hour.")
        run_async = str(settings.get("async_report", "")).lower() == "true"
        include_deleted = str(settings.get("include_deleted", "")).lower() \
            == "true"
        _check_report_mode(run_async, report, level, api_dims, metrics,
                           lifetime, include_deleted)
        if run_async:
            return self._run_async_report(
                spec, catalogue, report=report, level=level,
                api_dims=api_dims, api_metrics=api_metrics,
                lifetime=lifetime)
        if times:
            span = _TIME_SPLITS[times[0]][1]
        else:
            span = _MAX_TOTAL_DAYS
            days = (date.fromisoformat(spec.date_range.end)
                    - date.fromisoformat(spec.date_range.start)).days + 1
            if days > _MAX_TOTAL_DAYS and not lifetime:
                raise ApiError(
                    ErrorCode.INVALID_FILTER,
                    f"TikTok totals cover at most {_MAX_TOTAL_DAYS} days; this "
                    f"range is {days}. Add the date dimension, shorten the "
                    f"range, or set query_lifetime.")

        params: Dict[str, Any] = {
            "service_type": "AUCTION",
            "report_type": "AUDIENCE" if audience else "BASIC",
            "data_level": _LEVELS[level],
            "dimensions": json.dumps(api_dims),
            "metrics": json.dumps(api_metrics),
        }
        if include_deleted and level > 0:
            # Deleted entities are hidden unless the status filter is
            # STATUS_ALL.
            status_field = ("campaign_status", "adgroup_status",
                            "ad_status")[level - 1]
            params["filtering"] = json.dumps([{
                "field_name": status_field, "filter_type": "IN",
                "filter_value": json.dumps(["STATUS_ALL"])}])
        if lifetime:
            params["query_lifetime"] = "true"
        requested = list(spec.fields) or metrics
        multi = len(spec.accounts) > 1

        def fetch(account):
            rows: List[Dict[str, Any]] = []
            windows = ([(spec.date_range.start, spec.date_range.end)]
                       if lifetime else
                       _windows(spec.date_range.start, spec.date_range.end,
                                span))
            for start, end in windows:
                remaining = spec.max_rows - len(rows)
                if remaining <= 0:
                    break
                query = {**params, "advertiser_id": account}
                if not lifetime:
                    query.update(start_date=start, end_date=end)
                items = self._pages("report/integrated/get/", query, remaining)
                rows.extend(_parse_report_row(item, requested, catalogue,
                                              account, multi=multi)
                            for item in items)
            return rows

        rows, warnings = gather_accounts(spec.accounts, fetch)

        return QueryResult(
            requested_field_ids=requested,
            rows=rows,
            row_count=len(rows),
            warnings=warnings,
        )

    def _run_async_report(self, spec: QuerySpec, catalogue, *, report: str,
                          level: int, api_dims: List[str],
                          api_metrics: List[str],
                          lifetime: bool) -> QueryResult:
        """Run the report as an async task. Async reports have no date
        limit and no paging, so one task covers the whole range."""
        body: Dict[str, Any] = {
            "service_type": "AUCTION",
            "report_type": report,
            "data_level": _LEVELS[level],
            "dimensions": api_dims,
            "metrics": api_metrics,
            # Keep field ids as CSV headers instead of translated titles.
            "enable_report_title_translation": False,
            "output_format": "CSV_STRING",
        }
        if lifetime:
            body["query_lifetime"] = True
        else:
            body["start_date"] = spec.date_range.start
            body["end_date"] = spec.date_range.end
        requested = list(spec.fields) or list(api_metrics)
        multi = len(spec.accounts) > 1
        notes: List[str] = []

        def fetch(account):
            rows = []
            for row in self._async_task_rows(account, body):
                if len(rows) >= spec.max_rows:
                    break
                if "country_code" in requested and "country_code" not in row \
                        and "country_name" in row:
                    # Known TikTok bug: async reports return country
                    # names in place of codes.
                    row["country_code"] = row["country_name"]
                    if not notes:
                        notes.append(
                            "TikTok's async reports return country names in "
                            "place of country codes (official known issue).")
                rows.append(_parse_report_row(
                    {"dimensions": row, "metrics": row}, requested,
                    catalogue, account, multi=multi))
            return rows

        rows, warnings = gather_accounts(spec.accounts, fetch)
        return QueryResult(requested_field_ids=requested, rows=rows,
                           row_count=len(rows),
                           warnings=[*warnings, *notes])

    def _async_task_rows(self, account: str,
                         body: Dict[str, Any]) -> List[Dict[str, str]]:
        """Create a task, poll it, return its CSV rows. Cancels the task
        if it is still running after `_ASYNC_MAX_WAIT` seconds."""
        created = self._call("report/task/create/", method="POST",
                             body={**body, "advertiser_id": account},
                             write=False)
        task_id = str((created or {}).get("task_id") or "")
        if not task_id:
            raise ApiError(ErrorCode.UPSTREAM_ERROR,
                           "TikTok didn't return an async report task id.")
        ref = {"advertiser_id": account, "task_id": task_id}
        waited, delay = 0.0, _ASYNC_FIRST_POLL
        while True:
            state = self._call("report/task/check/", ref) or {}
            status = state.get("status")
            if status == "SUCCESS":
                break
            if status in ("FAILED", "CANCELED"):
                raise ApiError(
                    ErrorCode.UPSTREAM_ERROR,
                    f"TikTok async report task {task_id} {status.lower()}"
                    + (f": {state.get('message')}" if state.get("message")
                       else "."), retriable=False,
                    details={"task_id": task_id, "status": status})
            if waited >= _ASYNC_MAX_WAIT:
                try:
                    self._call("report/task/cancel/", method="POST",
                               body=ref, write=False)
                except ApiError:
                    pass
                raise ApiError(
                    ErrorCode.UPSTREAM_ERROR,
                    f"TikTok's async report was still {status or 'queued'} "
                    f"after {int(_ASYNC_MAX_WAIT)} s, so it was cancelled. "
                    f"Run it without async_report (the date column splits "
                    f"long ranges), or use a shorter range or fewer columns.",
                    retriable=True, details={"task_id": task_id})
            self._sleep(delay)
            waited += delay
            delay = min(delay * 2, _ASYNC_MAX_POLL)
        data = self._call("report/task/download/", ref)
        text = data.get(_RAW_TEXT) if isinstance(data, dict) else None
        if text is None:
            raise ApiError(ErrorCode.UPSTREAM_ERROR,
                           "TikTok's async report download had no CSV.")
        return list(csv.DictReader(io.StringIO(text.lstrip("\ufeff"))))

    def _run_gmv_report(self, spec: QuerySpec, report_type: str,
                        catalogue) -> QueryResult:
        """GMV Max report for one level (campaigns, products, creatives,
        livestreams, …)."""
        rep = _GMV.REPORTS[report_type]
        settings = spec.settings or {}
        store = str(settings.get("store_id") or "").strip()
        if not store:
            raise ApiError(
                ErrorCode.MISSING_SETTING,
                f"{report_type} needs the store_id setting: a TikTok Shop "
                f"from the GmvMaxStores report with is_gmv_max_available "
                f"true.", retriable=False, details={"setting": "store_id"})
        times = [f for f in spec.fields if f in _TIME_SPLITS]
        if len(times) > 1:
            raise ApiError(ErrorCode.INVALID_FILTER,
                           "date and hour can't be combined. Pick one.",
                           retriable=False)
        group = [f for f in spec.fields if f in _GMV.DIMENSIONS]
        # Status columns are metrics to TikTok, not attributes, so the
        # one-id rule for attributes doesn't apply to them.
        texts = [f for f in spec.fields if f in _GMV.TEXT_METRICS]
        attrs = [f for f in spec.fields if f not in group
                 and f not in _TIME_SPLITS and f not in texts
                 and catalogue[f].kind == "dimension"]
        metrics = [*texts, *(f for f in spec.fields
                             if catalogue[f].kind == "metric")]
        time_dims = [_TIME_SPLITS[t][0] for t in times]
        if not group:
            group = [t for t in rep.groupings[0].split()
                     if not t.endswith("?") and t not in ("TIME", "DAY")]
        api_dims = [*group, *time_dims]
        if not any(_RR.matches(api_dims, p) for p in rep.groupings):
            raise ApiError(
                ErrorCode.INVALID_FILTER,
                _api_words(f"{report_type} groups by: " + " | ".join(
                    _RR.describe(p) for p in rep.groupings) + "."),
                retriable=False)

        specs = [(n, *_GMV.FILTERS[n], n in rep.required)
                 for n in (*rep.required, *rep.optional)]
        filtering = _filter_from_settings(report_type, settings, specs)
        if rep.promotion:
            filtering["gmv_max_promotion_types"] = [rep.promotion]
        if attrs and not rep.campaign_level:
            ids = sum(d in _GMV.ID_DIMENSIONS for d in api_dims)
            many = any(len(filtering.get(k) or []) > 1
                       for k in ("campaign_ids", "item_group_ids",
                                 "room_ids"))
            if ids > 1 or many:
                raise ApiError(
                    ErrorCode.INVALID_FILTER,
                    f"TikTok returns attribute columns ({', '.join(attrs)}) "
                    f"below campaign level only for one id in the id "
                    f"filters and one ID grouping column.", retriable=False)

        api_metrics = list(dict.fromkeys([*attrs, *metrics])) or [
            m for m in ("cost", "orders", "gross_revenue", "roi")
            if m in rep.metrics]
        params: Dict[str, Any] = {
            "store_ids": json.dumps([store]),
            "dimensions": json.dumps(api_dims),
            "metrics": json.dumps(api_metrics),
        }
        if filtering:
            params["filtering"] = json.dumps(filtering)
        sort_field = str(settings.get("sort_field") or "").strip()
        if sort_field:
            if sort_field not in rep.sort_fields:
                raise ApiError(
                    ErrorCode.INVALID_FILTER,
                    f"sort_field must be one of "
                    f"{', '.join(rep.sort_fields) or '(none for this level)'}.",
                    retriable=False)
            params["sort_field"] = sort_field
        sort_type = str(settings.get("sort_type") or "").strip().upper()
        if sort_type:
            if sort_type not in ("ASC", "DESC"):
                raise ApiError(ErrorCode.INVALID_FILTER,
                               "sort_type must be ASC or DESC.",
                               retriable=False)
            params["sort_type"] = sort_type

        span = {"stat_time_hour": _GMV.SPAN_HOUR,
                "stat_time_day": _GMV.SPAN_DAY}.get(
                    time_dims[0] if time_dims else "", _GMV.SPAN_TOTAL)
        if not time_dims:
            days = (date.fromisoformat(spec.date_range.end)
                    - date.fromisoformat(spec.date_range.start)).days + 1
            if days > span:
                raise ApiError(
                    ErrorCode.INVALID_FILTER,
                    f"GMV Max totals cover at most {span} days; this range "
                    f"is {days}. Add the date column or shorten the range.",
                    retriable=False)
        requested = list(spec.fields) or [*api_dims, *api_metrics]
        multi = len(spec.accounts) > 1

        def fetch(account):
            rows: List[Dict[str, Any]] = []
            for start, end in _windows(spec.date_range.start,
                                       spec.date_range.end, span):
                remaining = spec.max_rows - len(rows)
                if remaining <= 0:
                    break
                items = self._pages(
                    _GMV.ENDPOINT, {**params, "advertiser_id": account,
                                    "start_date": start, "end_date": end},
                    remaining, page_size=_GMV.PAGE_SIZE)
                rows.extend(_parse_report_row(i, requested, catalogue,
                                              account, multi=multi)
                            for i in items)
            return rows

        rows, warnings = gather_accounts(spec.accounts, fetch)
        return QueryResult(requested_field_ids=requested, rows=rows,
                           row_count=len(rows), warnings=warnings)

    def _run_typed_report(self, spec: QuerySpec, report_type: str,
                          catalogue) -> QueryResult:
        """Playable ads, DSA (catalog) or Business Center report."""
        api_type, dims_allowed, _entries, leveled = _TYPED_REPORTS[report_type]
        dims = [f for f in spec.fields if f in dims_allowed]
        metrics = [f for f in spec.fields if f not in dims_allowed]
        if not dims:
            raise ApiError(
                ErrorCode.INVALID_FILTER,
                f"{report_type} needs at least one dimension: "
                f"{', '.join(dims_allowed)}.", retriable=False)
        settings = spec.settings or {}
        params: Dict[str, Any] = {
            "report_type": api_type,
            "dimensions": json.dumps(dims),
            "metrics": json.dumps(metrics or ["spend"]),
            "start_date": spec.date_range.start,
            "end_date": spec.date_range.end,
        }
        if api_type != "BC":
            params["service_type"] = "AUCTION"
        if leveled:
            level = max((("advertiser_id", "campaign_id", "adgroup_id")
                         .index(d) for d in dims
                         if d in ("advertiser_id", "campaign_id",
                                  "adgroup_id")), default=0)
            if any(d in dims for d in ("ad_id", "ad_id_v2")):
                level = 3
            params["data_level"] = _LEVELS[level]
        requested = list(spec.fields)
        multi = len(spec.accounts) > 1

        if api_type == "BC":
            err = _RR.grouping_error("BC", dims)
            bad = {} if err else _RR.unsupported_metrics("BC", dims, metrics)
            if err or bad:
                raise ApiError(
                    ErrorCode.INVALID_FILTER,
                    err or ("TikTok doesn't support these metrics with the "
                            "chosen columns: " + "; ".join(
                                f"{m} ({w})" for m, w in bad.items()) + "."),
                    details={"unsupported": bad} if bad else None,
                    retriable=False)
            bc_id = str(settings.get("bc_id") or "").strip()
            if not bc_id:
                raise ApiError(ErrorCode.MISSING_SETTING,
                               "BusinessCenter needs the bc_id setting.",
                               retriable=False)
            items = self._pages("report/integrated/get/",
                                {**params, "bc_id": bc_id}, spec.max_rows)
            rows = [_parse_report_row(i, requested, catalogue, bc_id)
                    for i in items]
            return QueryResult(requested_field_ids=requested, rows=rows,
                               row_count=len(rows))

        def fetch(account):
            items = self._pages("report/integrated/get/",
                                {**params, "advertiser_id": account},
                                spec.max_rows)
            return [_parse_report_row(i, requested, catalogue, account,
                                      multi=multi) for i in items]

        rows, warnings = gather_accounts(spec.accounts, fetch)
        return QueryResult(requested_field_ids=requested, rows=rows,
                           row_count=len(rows), warnings=warnings)

    def _run_entities(self, spec: QuerySpec, report_type) -> QueryResult:
        listing = _LISTINGS[report_type]
        fields = spec.fields or [f.id for f in listing.fields[:2]]
        multi = len(spec.accounts) > 1
        settings = spec.settings or {}
        if report_type == "Comments" and not settings.get("adgroup_id"):
            raise ApiError(
                ErrorCode.MISSING_SETTING,
                "The Comments report needs an adgroup_id setting: TikTok "
                "lists comments one ad group at a time.",
                retriable=False, details={"settings": ["adgroup_id"]})

        def fetch(account):
            params: Dict[str, Any] = {"advertiser_id": account}
            if listing.send_fields:
                params["fields"] = json.dumps(fields)
            if listing.dated and spec.date_range:
                params["start_time"] = spec.date_range.start
                params["end_time"] = spec.date_range.end
            if report_type == "Comments":
                # Comments can only be listed per ad group.
                params["search_field"] = "ADGROUP_ID"
                params["search_value"] = str(settings["adgroup_id"])
            params.update(dict(listing.static))
            for setting, param, kind in listing.setting_params:
                value = settings.get(setting)
                if value in (None, ""):
                    raise ApiError(
                        ErrorCode.MISSING_SETTING,
                        f"The {report_type} report needs the {setting} "
                        f"setting.", retriable=False,
                        details={"setting": setting})
                if kind == "ids":
                    ids = [v.strip() for v in str(value).split(",") if v.strip()]
                    params[param] = json.dumps(ids)
                else:
                    params[param] = str(value).strip()
            filtering = _filter_from_settings(
                report_type, settings, listing.filter_settings)
            if filtering:
                params["filtering"] = json.dumps(filtering)
            if listing.single:
                data = self._call(listing.path, params)
                items = [data] if isinstance(data, dict) and data else []
            elif not listing.paged:
                data = self._call(listing.path, params)
                key = next((k for k in listing.list_keys
                            if isinstance(data, dict) and k in data),
                           listing.list_keys[0])
                items = list((data or {}).get(key) or [])[:spec.max_rows]
            else:
                items = self._pages(listing.path, params, spec.max_rows,
                                    page_size=listing.page_size,
                                    list_keys=listing.list_keys)
            out = []
            for obj in items:
                record: Dict[str, Any] = {}
                if multi:
                    record["_account"] = account
                for name in fields:
                    record[name] = _lookup(obj, name)
                out.append(record)
            return out

        rows, warnings = gather_accounts(spec.accounts, fetch)

        return QueryResult(
            requested_field_ids=list(fields),
            rows=rows,
            row_count=len(rows),
            warnings=warnings,
        )
