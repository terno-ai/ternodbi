"""Report columns for each TikTok Ads report type, and row parsing."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from terno_dbi.connectors.api.model.types import Field
from terno_dbi.connectors.api.sources.tiktok.ads.smart_plus_spec import (
    SMART_PLUS_REPORT_DIMENSIONS,
    SMART_PLUS_REPORT_METRICS,
)
from terno_dbi.connectors.api.sources.tiktok.ads import (
    gmv_max_spec as _GMV,
    report_spec as _RS,
)
from terno_dbi.connectors.api.sources.tiktok.ads.listings import _LISTINGS


_LEVELS = ("AUCTION_ADVERTISER", "AUCTION_CAMPAIGN", "AUCTION_ADGROUP",
           "AUCTION_AD")


_LEVEL_ID = ("advertiser_id", "campaign_id", "adgroup_id", "ad_id")


# Lowest data level (0 advertiser … 3 ad) at which each attribute exists.
# The most detailed attribute requested decides `data_level`.
_ATTR_LEVEL: Dict[str, int] = {
    "advertiser_name": 0, "advertiser_id": 0, "currency": 0, "minis_name": 0,
    "campaign_automation_type": 1, "campaign_name": 1, "objective_type": 1,
    "split_test": 1, "campaign_budget": 1, "campaign_dedicate_type": 1,
    "app_promotion_type": 1,
    "campaign_id": 1, "adgroup_name": 2, "placement_type": 2,
    "promotion_type": 2, "opt_status": 2, "adgroup_download_url": 2,
    "profile_image": 2, "dpa_target_audience_type": 2, "budget": 2,
    "smart_target": 2, "billing_event": 2, "bid_strategy": 2, "bid": 2,
    "bid_secondary_goal": 2, "aeo_type": 2, "tt_app_id": 2, "tt_app_name": 2,
    "mobile_app_id": 2, "optimization_event": 2, "is_aco": 2,
    "adgroup_id": 2, "ad_name": 3, "ad_id_v2": 3, "ad_id": 3, "ad_text": 3,
    "call_to_action": 3, "ad_profile_image": 3, "ad_url": 3, "image_mode": 3,
    "is_smart_creative": 3,
}


# Attributes that exist at one level only.
_ATTR_ONLY_LEVEL: Dict[str, int] = {"is_aco": 2, "is_smart_creative": 3}


# Time column -> (TikTok dimension, max days per request).
_TIME_SPLITS: Dict[str, Tuple[str, int]] = {
    "date": ("stat_time_day", 30),
    "hour": ("stat_time_hour", 1),
}


# Max days per request without a time column.
_MAX_TOTAL_DAYS = 365


_BASIC_DIMS = frozenset(_RS.DIMENSIONS["BASIC"])


_AUDIENCE_DIMS = frozenset(_RS.DIMENSIONS["AUDIENCE"])


# Using any of these switches the request to an AUDIENCE report.
_AUDIENCE_ONLY = _AUDIENCE_DIMS - _BASIC_DIMS


# Not offered as plain columns: ids follow the level, time is windowed.
_SPECIAL_DIMS = frozenset({"advertiser_id", "campaign_id", "adgroup_id",
                           "ad_id", "ad_id_v2", "stat_time_day",
                           "stat_time_hour"})


def _api_words(text: str) -> str:
    """Use our column names (date, hour) in messages."""
    return text.replace("stat_time_day", "date").replace("stat_time_hour",
                                                         "hour")


def _api_words_dict(bad: Dict[str, str]) -> Dict[str, str]:
    return {m: _api_words(w) for m, w in bad.items()}


def _short_label(label: str, fid: str) -> str:
    """Shorten labels that are really descriptions."""
    if len(label) <= 60:
        return label
    first = label.split(". ")[0]
    return first if len(first) <= 60 else fid.replace("_", " ").capitalize()


def _metric_flags(fid: str, label: str) -> Tuple[bool, bool]:
    """Guess (is_monetary, is_non_aggregatable) from the id and label."""
    low = f"{fid} {label}".lower()
    money = any(w in low for w in ("cost", "spend", "cpc", "cpm", "value",
                                   "revenue")) and "roas" not in low \
        and not fid.endswith("_rate")
    ratio = any(w in low for w in ("rate", "ctr", "cpc", "cpm", "roas",
                                   "average", "avg_", "frequency", "reach",
                                   "per ", "per_", "%"))
    return money, ratio


def _metric_field(fid: str, label: str, group: str,
                  flags=()) -> Field:
    money, ratio = _metric_flags(fid, label)
    desc = f"{group}." + (" Advertiser level only." if "adv" in flags else "") \
        + (" Asynchronous reports only." if "async" in flags else "")
    return Field(fid, _short_label(label, fid), "metric", desc,
                 data_type="number", is_monetary=money,
                 is_non_aggregatable=ratio)


def _attribute_field(fid: str, label: str) -> Field:
    return Field(fid, _short_label(label, fid), "dimension",
                 "Attribute (needs an ID level; set automatically).")


_BASIC_LABEL = {fid: lab for fid, lab, _g, _f in _RS.BASIC}


_GROUP_OF = {fid: g for fid, _l, g, _f in _RS.BASIC}


_METRIC_FLAGS = {fid: fl for fid, _l, _g, fl in _RS.BASIC}


_ATTRIBUTE_IDS = frozenset(fid for fid, _l, g, _f in _RS.BASIC if g == 0)


_METRICS: List[Field] = [
    _metric_field(fid, lab, _RS.GROUPS[g], fl)
    for fid, lab, g, fl in _RS.BASIC if g != 0]


_METRIC_IDS = frozenset(f.id for f in _METRICS)


def _report_metric_ids(entries) -> frozenset:
    return frozenset(e if isinstance(e, str) else e[0] for e in entries)


_AUDIENCE_METRIC_IDS = _report_metric_ids(_RS.AUDIENCE)


# Columns only audience reports have.
_AUDIENCE_EXTRA: List[Field] = [
    (_attribute_field(e[0], e[1]) if e[2] == "Attribute metrics"
     else _metric_field(e[0], e[1], e[2]))
    for e in _RS.AUDIENCE
    if not isinstance(e, str) and e[0] not in _BASIC_LABEL]


_AUDIENCE_ATTRS = frozenset(f.id for f in _AUDIENCE_EXTRA
                            if f.kind == "dimension")


_DIM_LABELS = {
    "age": "Age", "gender": "Gender", "country_code": "Country",
    "province_id": "Region", "dma_id": "DMA", "platform": "Platform",
    "placement": "Placement", "language": "Language", "ac": "Network",
}


_REPORT_DIMENSIONS: List[Field] = [
    Field("date", "Date", "dimension", "Day the stat occurred (stat_time_day), "
          "in the advertiser's timezone.", data_type="date"),
    Field("hour", "Hour", "dimension", "Hour the stat occurred "
          "(stat_time_hour). One day per request.", data_type="datetime"),
    *[_attribute_field(fid, lab) for fid, lab, g, _f in _RS.BASIC if g == 0],
    *[f for f in _AUDIENCE_EXTRA if f.kind == "dimension"],
    *[Field(d, _DIM_LABELS.get(d, d.replace("_", " ").capitalize()),
            "dimension",
            "Audience dimension (switches to an AUDIENCE report)."
            if d in _AUDIENCE_ONLY else "Basic report dimension.")
      for d in sorted((_BASIC_DIMS | _AUDIENCE_DIMS) - _SPECIAL_DIMS)],
]


_REPORT_METRICS: List[Field] = [
    *_METRICS, *[f for f in _AUDIENCE_EXTRA if f.kind == "metric"]]


# Report -> (TikTok report_type, dimensions, metrics, takes data_level).
_TYPED_REPORTS: Dict[str, Tuple[str, Tuple[str, ...], list, bool]] = {
    "PlayableAds": ("PLAYABLE_MATERIAL", _RS.DIMENSIONS["PLAYABLE_MATERIAL"],
                    _RS.PLAYABLE, False),
    "DSA": ("CATALOG", _RS.DIMENSIONS["CATALOG"], _RS.DSA, True),
    "BusinessCenter": ("BC", _RS.DIMENSIONS["BC"], _RS.BC, False),
}


def _typed_fields(report_type: str) -> Dict[str, Field]:
    _api, dims, entries, _lvl = _TYPED_REPORTS[report_type]
    out: Dict[str, Field] = {}
    for d in dims:
        out[d] = Field(d, d.replace("_", " ").capitalize(), "dimension",
                       data_type="date" if d.startswith("stat_time") else
                       "string")
    for e in entries:
        fid = e if isinstance(e, str) else e[0]
        if fid in out:
            continue
        label = _BASIC_LABEL.get(fid, fid) if isinstance(e, str) else e[1]
        group = "" if isinstance(e, str) else e[2]
        is_attr = (fid in _ATTRIBUTE_IDS) or group == "Attribute metrics"
        out[fid] = (_attribute_field(fid, label) if is_attr
                    else _metric_field(fid, label, group or "Metrics"))
    return out


_DEFAULT_REPORT = "Performance"


# Upgraded Smart+ creative reports -> endpoint.
_SP_REPORTS: Dict[str, str] = {
    "SmartPlusCreativeOverview": "smart_plus/material_report/overview/",
    "SmartPlusCreativeBreakdown": "smart_plus/material_report/breakdown/",
}


# Text columns TikTok lists among these reports' metrics.
_SP_ATTRIBUTE_METRICS = frozenset({
    "ad_material_id", "smart_plus_creative_id", "smart_plus_creative_name",
    "adgroup_name", "campaign_name"})


_SP_METRIC_FIELDS: List[Field] = [
    Field(m, m.replace("_", " ").capitalize(), "metric",
          data_type="string" if m in _SP_ATTRIBUTE_METRICS else "number")
    for m in SMART_PLUS_REPORT_METRICS]


# Settings passed through as `filtering` ids.
_SP_REPORT_FILTERS = ("campaign_ids", "adgroup_ids", "smart_plus_ad_ids")


def _sp_report_fields(report_type: str) -> Dict[str, Field]:
    dims = SMART_PLUS_REPORT_DIMENSIONS[_SP_REPORTS[report_type]]
    out = {d: Field(d, d.replace("_", " ").capitalize(), "dimension",
                    data_type="date" if d == "stat_time_day" else "string")
           for d in dims}
    out.update({f.id: f for f in _SP_METRIC_FIELDS})
    return out


def _gmv_fields(report_type: str) -> Dict[str, Field]:
    """Columns for one GMV Max report level."""
    rep = _GMV.REPORTS[report_type]
    dims: List[str] = []
    hourly = False
    for pattern in rep.groupings:
        for tok in pattern.split():
            tok = tok.rstrip("?")
            if tok in ("TIME", "DAY"):
                hourly = hourly or tok == "TIME"
                continue
            dims.extend(d for d in tok.split("|") if d not in dims)
    out: Dict[str, Field] = {}
    if any(("TIME" in p or "DAY" in p) for p in rep.groupings):
        out["date"] = Field("date", "Date", "dimension",
                            "Day (stat_time_day). 30 days per request.",
                            data_type="date")
    if hourly:
        out["hour"] = Field("hour", "Hour", "dimension",
                            "Hour (stat_time_hour). One day per request.",
                            data_type="datetime")
    for d in dims:
        out[d] = Field(d, _GMV.DIMENSIONS[d], "dimension", "Grouping column.")
    current = report_type in _GMV.LIVE_REPORTS
    for a in (*rep.attributes, *[m for m in rep.metrics
                                 if m in _GMV.TEXT_METRICS]):
        if a not in out:
            out[a] = Field(a, _GMV.LABELS.get(a, a), "dimension",
                           "Attribute of this level.")
    for m in rep.metrics:
        if m in _GMV.TEXT_METRICS:
            continue
        label = _GMV.LABELS[m]
        if current and m in ("orders", "cost_per_order", "gross_revenue",
                             "roi"):
            label = label.replace(" (SKU)", "") + " (current shop)"
        money, ratio = _metric_flags(m, label)
        ratio = ratio or "roi" in m
        out[m] = Field(m, label, "metric", rep.label, data_type="number",
                       is_monetary=money and "roi" not in m,
                       is_non_aggregatable=ratio)
    return out


def _fields_for(report_type: Optional[str]) -> Dict[str, Field]:
    if report_type in _GMV.REPORTS:
        return _gmv_fields(report_type)
    if report_type in _TYPED_REPORTS:
        return _typed_fields(report_type)
    if report_type in _SP_REPORTS:
        return _sp_report_fields(report_type)
    if report_type in _LISTINGS:
        return {f.id: f for f in _LISTINGS[report_type].fields}
    return {f.id: f for f in (*_REPORT_DIMENSIONS, *_REPORT_METRICS)}


def _parse_report_row(item, requested_fields, catalogue, account, *,
                      multi=False) -> Dict[str, Any]:
    """Turn a `{dimensions, metrics}` row into the requested columns.

    Attributes such as names come back under `metrics`. `date` is cut from
    the `stat_time_day` timestamp.
    """
    dims = item.get("dimensions") or {}
    mets = item.get("metrics") or {}
    record: Dict[str, Any] = {}
    if multi:
        record["_account"] = account
    for name in requested_fields:
        key = _TIME_SPLITS[name][0] if name in _TIME_SPLITS else name
        raw = dims[key] if key in dims else mets.get(key)
        if name == "date" and isinstance(raw, str):
            raw = raw[:10]
        record[name] = _coerce_value(name, raw, catalogue)
    return record


def _coerce_value(field_id, raw, catalogue):
    if raw is None:
        return None
    field = catalogue.get(field_id)
    if field and field.kind == "metric":
        if raw == "-":   # not applicable
            return None
        try:
            f = float(raw)
            # Keep decimals for money; whole counts become ints.
            if field.is_monetary:
                return f
            return int(f) if f.is_integer() else f
        except (TypeError, ValueError):
            return raw
    return raw
