"""Which dimensions and metrics TikTok lets you combine in a report.

Copied from the "Dimension grouping" sections and supported-metrics tables of
the Basic, Audience and Business Center report docs. Where two pages disagree,
the combination is allowed and TikTok decides.

A pattern is a list of tokens that must match the request's dimensions
exactly. A token is a dimension id, `a|b` for either, or a class: `ID` (any
ID dimension), `ID4` (not ad_id_v2), `ID3` (advertiser, campaign or ad
group), `ID2` (advertiser or campaign), `IDX` (any but advertiser), `DAY`
(stat_time_day) or `TIME` (day or hour). A trailing `?` makes it optional.
"""

from typing import Dict, Iterable, List, Optional, Sequence, Tuple

ID_DIMS = ("advertiser_id", "campaign_id", "adgroup_id", "ad_id", "ad_id_v2")
TIME_DIMS = ("stat_time_day", "stat_time_hour")

_CLASSES: Dict[str, Tuple[str, ...]] = {
    "ID": ID_DIMS,
    "ID4": ID_DIMS[:4],
    "ID3": ID_DIMS[:3],
    "ID2": ID_DIMS[:2],
    "IDX": ID_DIMS[1:],
    "DAY": ("stat_time_day",),
    "TIME": TIME_DIMS,
}


def _choices(token: str) -> Tuple[str, ...]:
    out: List[str] = []
    for part in token.split("|"):
        out.extend(_CLASSES.get(part, (part,)))
    return tuple(out)


def matches(dims: Iterable[str], pattern: str) -> bool:
    """True if `dims` matches the pattern exactly."""
    left = list(dims)
    for raw in pattern.split():
        optional = raw.endswith("?")
        choices = _choices(raw.rstrip("?"))
        hit = next((d for d in left if d in choices), None)
        if hit is None:
            if optional:
                continue
            return False
        left.remove(hit)
    return not left


_WORDS = {"ID": "one ID level", "ID4": "one ID level (not ad_id_v2)",
          "ID3": "advertiser/campaign/ad group ID",
          "ID2": "advertiser/campaign ID", "IDX": "campaign/ad group/ad ID",
          "DAY": "date", "TIME": "date or hour",
          "stat_time_day": "date", "stat_time_hour": "hour"}


def describe(pattern: str) -> str:
    """Readable form, e.g. 'page_id + component_name (optional)'."""
    parts = []
    for raw in pattern.split():
        opt = raw.endswith("?")
        word = " or ".join(_WORDS.get(p, p) for p in raw.rstrip("?").split("|"))
        parts.append(f"{word} (optional)" if opt else word)
    return " + ".join(parts)


# ---- Basic reports -----------------------------------------------------

# Groupings allowed for each non-ID, non-time dimension. A request is valid
# if any of its dimensions has a matching pattern.
BASIC_GROUPINGS: Dict[str, Tuple[str, ...]] = {
    "country_code": ("country_code ID? DAY?",),
    "ad_type": ("ad_type ID? TIME?",),
    "custom_event_type": ("custom_event_type ID? DAY?",),
    "image_id": ("image_id ID? DAY?",),
    "video_material_id": ("video_material_id ID? DAY?",),
    "event_source_id": ("event_source_id custom_event_type?",
                        "event_source_id DAY"),
    "page_id": ("page_id component_name? DAY?",),
    "component_name": ("page_id component_name DAY?",),
    "room_id": ("room_id DAY?",),
    "post_id": ("post_id DAY?",),
    "minis_id": ("minis_id ID? DAY?",),
    "search_terms": ("search_terms ID3? TIME?",),
    "search_keyword": ("search_keyword ID3? DAY?",),
    "match_type": ("match_type ID3? DAY?",),
}
_ID_TIME = "ID? TIME?"

_SEARCH_METRICS = ("spend", "cpc", "cpm", "impressions", "clicks", "ctr")
_BASIC_COMMON = ("spend", "cpc", "cpm", "impressions", "gross_impressions",
                 "clicks", "ctr", "conversion", "cost_per_conversion",
                 "conversion_rate", "real_time_conversion",
                 "real_time_cost_per_conversion", "real_time_conversion_rate")
_VIDEO = ("video_play_actions", "video_watched_2s", "video_watched_6s",
          "average_video_play", "video_views_p25", "video_views_p50",
          "video_views_p75", "video_views_p100", "engaged_view",
          "engaged_view_15s")
_SOCIAL = ("clicks_on_music_disc", "follows", "likes", "comments", "shares",
           "profile_visits", "profile_visits_rate")
_LPV = ("total_landing_page_view", "cost_per_landing_page_view",
        "landing_page_view_rate")
_IX_VIDEO = ("ix_page_duration_avg", "ix_page_viewrate_avg", "ix_video_views",
             "ix_video_views_p25", "ix_video_views_p50", "ix_video_views_p75",
             "ix_video_views_p100", "ix_average_video_play")
_CONV_V2 = ("conversion_rate_v2", "real_time_conversion_rate_v2")
_ONSITE_SHOP = (
    "onsite_add_to_wishlist", "cost_per_onsite_add_to_wishlist",
    "onsite_add_to_wishlist_rate", "value_per_onsite_add_to_wishlist",
    "total_onsite_add_to_wishlist_value", "ix_product_click_count",
    "cost_per_ix_product_click_count", "ix_product_click_count_rate",
    "onsite_shopping_roas", "onsite_shopping", "cost_per_onsite_shopping",
    "onsite_shopping_rate", "value_per_onsite_shopping",
    "total_onsite_shopping_value", "onsite_initiate_checkout_count",
    "cost_per_onsite_initiate_checkout_count",
    "onsite_initiate_checkout_count_rate",
    "value_per_onsite_initiate_checkout_count",
    "total_onsite_initiate_checkout_count_value", "onsite_on_web_detail",
    "cost_per_onsite_on_web_detail", "onsite_on_web_detail_rate",
    "value_per_onsite_on_web_detail", "total_onsite_on_web_detail_value",
    "onsite_on_web_cart", "cost_per_onsite_on_web_cart",
    "onsite_on_web_cart_rate", "value_per_onsite_on_web_cart",
    "total_onsite_on_web_cart_value")
_ROOM_POST_CORE = ("spend", "cpc", "cpm", "impressions", "clicks", "ctr",
                   "conversion", "cost_per_conversion", "conversion_rate",
                   "conversion_rate_v2")

# Metrics each dimension supports. (TikTok's country_code table has the
# typo "conversio"; it means `conversion`.)
BASIC_DIM_METRICS: Dict[str, frozenset] = {
    "ad_type": frozenset(_BASIC_COMMON + _VIDEO + _SOCIAL + _LPV + (
        "skan_conversion", "skan_cost_per_conversion", "skan_conversion_rate",
        "skan_app_install", "skan_cost_per_app_install",
        "skan_app_install_withheld")),
    "country_code": frozenset(_BASIC_COMMON + (
        "reach", "result", "cost_per_result", "result_rate",
        "real_time_result", "real_time_cost_per_result",
        "real_time_result_rate", "frequency") + _VIDEO + _SOCIAL + (
        "live_product_clicks", "real_time_app_install",
        "real_time_app_install_cost", "app_install", "cost_per_app_install",
        "registration", "cost_per_registration", "registration_rate",
        "purchase", "cost_per_purchase", "purchase_rate", "complete_payment",
        "cost_per_complete_payment", "complete_payment_rate") + _LPV + (
        "page_browse_view", "cost_per_page_browse_view",
        "page_browse_view_rate", "button_click", "cost_per_button_click",
        "button_click_rate", "online_consult", "cost_per_online_consult",
        "online_consult_rate", "form", "cost_per_form", "form_rate",
        "download_start", "cost_per_download_start", "download_start_rate")),
    "search_terms": frozenset(_SEARCH_METRICS + (
        "conversion", "cost_per_conversion", "conversion_rate",
        "real_time_conversion", "result", "cost_per_result", "result_rate",
        "video_play_actions", "engaged_view", "engaged_view_15s")),
    "page_id": frozenset((
        "onsite_form", "onsite_download_start", "ix_page_view_count",
        "ix_button_click_count", "ix_product_click_count") + _IX_VIDEO
        + _BASIC_COMMON + _CONV_V2),
    "component_name": frozenset(_IX_VIDEO + _BASIC_COMMON + _CONV_V2),
    "room_id": frozenset(_ROOM_POST_CORE + _ONSITE_SHOP),
    "post_id": frozenset(_ROOM_POST_CORE + (
        "video_play_actions", "video_watched_2s", "video_watched_6s",
        "engaged_view", "video_views_p25", "video_views_p50",
        "video_views_p75", "video_views_p100", "average_video_play")
        + _ONSITE_SHOP),
    "image_id": frozenset(("clicks", "single_image_impressions",
                           "single_image_impression_rate",
                           "single_image_ctr")),
    "search_keyword": frozenset(_SEARCH_METRICS),
    "match_type": frozenset(_SEARCH_METRICS),
}
# Metrics these dimensions do not support.
BASIC_DIM_EXCLUDED: Dict[str, frozenset] = {
    "video_material_id": frozenset((
        "reach", "cost_per_1000_reached", "frequency", "result",
        "cost_per_result", "result_rate", "real_time_result",
        "real_time_cost_per_result", "real_time_result_rate",
        "secondary_goal_result", "cost_per_secondary_goal_result",
        "secondary_goal_result_rate", "skan_result", "skan_cost_per_result",
        "skan_result_rate", "live_views", "live_unique_views",
        "live_effective_views", "live_product_clicks", "video_views_p100")),
}

# Groupings some metrics require, by metric group (index into
# report_spec.GROUPS) or by individual metric.
_ID_DAY = ("ID DAY?",)
_ID4_DAY = ("ID4 DAY?",)
BASIC_GROUP_RULES: Dict[int, Tuple[str, ...]] = {
    11: _ID_DAY,                                    # TikTok paid metrics
    12: ("minis_id ID? DAY?", "ID DAY?"),           # Native Growth
    13: _ID4_DAY,                                   # Offline
    14: _ID4_DAY,                                   # Messaging
    15: _ID4_DAY,                                   # Shop
}
BASIC_METRIC_RULES: Dict[str, Tuple[str, ...]] = {
    **{m: _ID4_DAY for m in (
        # Purchase (App metrics)
        "purchase", "cost_per_purchase", "purchase_rate", "total_purchase",
        "cost_per_total_purchase", "value_per_total_purchase",
        "total_purchase_value", "total_active_pay_roas",
        "total_purchase_roas_day0", "total_purchase_roas_day2",
        "total_purchase_roas_day6", "total_purchase_value_day0",
        "total_purchase_value_day2", "total_purchase_value_day6",
        # Ad impression (App metrics)
        "unique_ad_impression_events", "cost_per_unique_ad_impression_event",
        "customized_ad_impression_event_rate", "ads_impression_events",
        "cost_pre_ad_impression_event", "value_per_ad_impression_event",
        "total_ad_impression_events_value", "total_ad_impression_roas",
        "ad_impression_ad_revenue_roas_day0",
        "ad_impression_ad_revenue_roas_day2",
        "ad_impression_ad_revenue_roas_day6", "ad_impression_ad_revenue_day0",
        "ad_impression_ad_revenue_day2", "ad_impression_ad_revenue_day6",
        # Destination visit (TikTok)
        "onsite_destination_visits", "cost_per_onsite_destination_visit",
        "onsite_destination_visit_rate",
        # Focused-view paid metrics
        "paid_engaged_view", "paid_engagement_engaged_view",
        "paid_engaged_view_15s", "paid_engagement_engaged_view_15s")},
    **{m: ("ID4 DAY?", "ad_type ID4? DAY?") for m in (
        # Day 7 retention (App metrics)
        "day7_retention", "cost_per_day7_retention", "day7_retention_rate",
        "total_day7_retention", "cost_per_total_day7_retention",
        "value_per_total_day7_retention", "total_day7_retention_value")},
    **{m: _ID_DAY for m in ("cost_per_consideration", "new_consideration_rate",
                            "new_consideration_size")},
}
# Metrics that only work in synchronous reports.
BASIC_SYNC_ONLY = frozenset({"billed_cost", "onsite_destination_visits",
                             "cost_per_onsite_destination_visit",
                             "onsite_destination_visit_rate"})

# ---- Audience reports --------------------------------------------------

# One audience dimension, plus optionally one ID and one time dimension.
# Exceptions: age and gender can go together; locations allow date but not
# hour; interest categories and device brand allow no time dimension;
# ad_type can't go with advertiser_id.
AUDIENCE_DEFAULT = "{d} ID? TIME?"
AUDIENCE_GROUPINGS: Dict[str, Tuple[str, ...]] = {
    "age": ("age gender? ID? TIME?",),
    "gender": ("gender age? ID? TIME?",),
    "country_code": ("country_code ID? DAY?",
                     "country_code bucket_id ID2? TIME?"),
    "province_id": ("province_id ID? DAY?",),
    "bucket_id": ("bucket_id ID2? placement|country_code? TIME?",),
    "interest_category": ("interest_category ID?",),
    "interest_category_tier2": ("interest_category_tier2 ID?",),
    "interest_category_tier3": ("interest_category_tier3 ID?",),
    "interest_category_tier4": ("interest_category_tier4 ID?",),
    "device_brand_id": ("device_brand_id ID?",),
    "ad_type": ("ad_type IDX? TIME?",),
}
_AUD_SHORT = ("spend", "cpc", "cpm", "impressions", "clicks", "conversion",
              "cost_per_conversion", "conversion_rate", "conversion_rate_v2",
              "real_time_conversion", "real_time_cost_per_conversion",
              "real_time_conversion_rate", "real_time_conversion_rate_v2",
              "video_play_actions", "video_watched_6s", "video_views_p100",
              "video_views_p75", "video_views_p50", "video_views_p25",
              "average_video_play", "engaged_view", "engaged_view_15s",
              "follows", "comments", "shares", "profile_visits_rate",
              "profile_visits", "clicks_on_music_disc",
              "total_landing_page_view")
AUDIENCE_DIM_METRICS: Dict[str, frozenset] = {
    "ad_type": frozenset(_AUD_SHORT),
    "hashtag": frozenset(_AUD_SHORT),
    "audience_tags": frozenset(_AUD_SHORT),
    "purchase_intention": frozenset(_AUD_SHORT),
    "country_code": frozenset((
        "spend", "cpc", "cpm", "impressions", "gross_impressions", "clicks",
        "ctr", "conversion", "cost_per_conversion", "conversion_rate",
        "real_time_conversion", "real_time_cost_per_conversion",
        "real_time_conversion_rate", "result", "cost_per_result",
        "result_rate", "real_time_result", "real_time_cost_per_result",
        "real_time_result_rate", "video_play_actions", "video_watched_2s",
        "video_watched_6s", "average_video_play", "video_views_p25",
        "video_views_p50", "video_views_p75", "video_views_p100",
        "profile_visits", "profile_visits_rate", "likes", "comments",
        "shares", "follows", "clicks_on_music_disc") + _LPV),
}
# Reach and frequency only work with these audience dimensions.
_REACH_RULE = ("age|gender|country_code|province_id|language ID? TIME?",
               "age gender ID? TIME?")
AUDIENCE_METRIC_RULES: Dict[str, Tuple[str, ...]] = {
    "reach": _REACH_RULE, "cost_per_1000_reached": _REACH_RULE,
    "frequency": _REACH_RULE,
}

# ---- Business Center reports ------------------------------------------

BC_GROUPINGS: Tuple[str, ...] = (
    "advertiser_id", "stat_time_day", "stat_time_week", "stat_time_month",
    "advertiser_id placement", "advertiser_id country_code")
_BC_TIME = frozenset((
    "spend", "billed_cost", "cash_spend", "voucher_spend",
    "cashback_coupon_spend", "tax_spend", "cpc", "cpm", "impressions",
    "clicks", "ctr", "conversion", "cost_per_conversion", "conversion_rate",
    "reach", "real_time_conversion", "real_time_cost_per_conversion",
    "real_time_conversion_rate", "account_type", "skan_conversion",
    "skan_cost_per_conversion", "skan_conversion_rate", "registration",
    "cost_per_registration", "registration_rate", "purchase",
    "cost_per_purchase", "purchase_rate", "value_per_total_purchase",
    "total_purchase_value", "app_install", "cost_per_app_install",
    "app_install_rate", "real_time_app_install", "real_time_app_install_cost",
    "real_time_app_install_rate", "skan_app_install",
    "skan_cost_per_app_install", "skan_registration",
    "skan_cost_per_registration", "skan_registration_rate",
    "skan_total_registration", "skan_cost_per_total_registration",
    "skan_purchase", "skan_cost_per_purchase", "skan_purchase_rate",
    "skan_total_purchase", "skan_cost_per_total_purchase",
    "skan_total_purchase_value", "video_watched_2s", "video_watched_6s",
    "video_views_p100", "video_views_p75", "video_views_p50",
    "video_views_p25"))
# Grouping by advertiser_id alone supports every BC metric.
BC_DIM_METRICS: Dict[str, frozenset] = {
    "stat_time_day": _BC_TIME, "stat_time_week": _BC_TIME,
    "stat_time_month": _BC_TIME,
    "placement": frozenset((
        "advertiser_name", "currency", "spend", "cpc", "cpm", "impressions",
        "clicks", "ctr", "conversion", "cost_per_conversion",
        "conversion_rate", "real_time_conversion",
        "real_time_cost_per_conversion", "real_time_conversion_rate")),
    "country_code": frozenset((
        "advertiser_name", "currency", "spend", "cash_spend", "voucher_spend",
        "cashback_coupon_spend", "cpc", "cpm", "impressions", "clicks", "ctr",
        "conversion", "cost_per_conversion", "conversion_rate",
        "real_time_conversion", "real_time_cost_per_conversion",
        "real_time_conversion_rate")),
}


# ---- Checks --------------------------------------------------------------

def grouping_error(report: str, dims: Sequence[str]) -> Optional[str]:
    """Return why TikTok would reject this grouping, or None."""
    dims = list(dims)
    if not dims:
        return "a report needs at least one dimension"
    if report == "BC":
        if any(matches(dims, p) for p in BC_GROUPINGS):
            return None
        return ("Business Center reports group by one of: "
                + "; ".join(" + ".join(p.split()) for p in BC_GROUPINGS))
    others = [d for d in dims if d not in ID_DIMS and d not in TIME_DIMS]
    if report == "AUDIENCE":
        pats = {d: AUDIENCE_GROUPINGS.get(d, (AUDIENCE_DEFAULT.format(d=d),))
                for d in others}
    else:
        pats = {d: BASIC_GROUPINGS[d] for d in others
                if d in BASIC_GROUPINGS}
        if len(pats) < len(others):
            # No documented rule for a dimension: only check ID/time counts.
            ids = sum(d in ID_DIMS for d in dims)
            times = sum(d in TIME_DIMS for d in dims)
            return None if ids <= 1 and times <= 1 else \
                "one ID and one time dimension at most"
    if not others:
        return None if matches(dims, _ID_TIME) else \
            "one ID and one time dimension at most"
    if any(matches(dims, p) for ps in pats.values() for p in ps):
        return None
    allowed = "; ".join(f"{d}: " + " | ".join(describe(p) for p in ps)
                        for d, ps in pats.items())
    return f"TikTok doesn't allow this grouping. Allowed: {allowed}"


def unsupported_metrics(report: str, dims: Sequence[str],
                        metrics: Sequence[str],
                        group_of: Optional[Dict[str, int]] = None
                        ) -> Dict[str, str]:
    """Return {metric: reason} for metrics TikTok rejects with `dims`."""
    dims = list(dims)
    bad: Dict[str, str] = {}
    if report == "BC":
        key = next((d for d in dims if d != "advertiser_id"), None)
        table = BC_DIM_METRICS.get(key) if key else None
        if table is not None:
            for m in metrics:
                if m not in table:
                    bad[m] = f"not supported with {key}"
        return bad

    others = [d for d in dims if d not in ID_DIMS and d not in TIME_DIMS]
    if report == "AUDIENCE":
        tables, excluded = AUDIENCE_DIM_METRICS, {}
        rules_for = AUDIENCE_METRIC_RULES.get
    else:
        tables, excluded = BASIC_DIM_METRICS, BASIC_DIM_EXCLUDED
        group_of = group_of or {}

        def rules_for(m):
            return BASIC_METRIC_RULES.get(m) or BASIC_GROUP_RULES.get(
                group_of.get(m, -1))

    for m in metrics:
        rule = rules_for(m)
        if rule and any(matches(dims, p) for p in rule):
            continue
        tabled = [d for d in others if d in tables or d in excluded]
        allowed_by_tables = all(
            (m in tables[d]) if d in tables else (m not in excluded[d])
            for d in tabled)
        if rule is None:
            if not allowed_by_tables:
                d = next(d for d in tabled if (m not in tables[d]
                         if d in tables else m in excluded[d]))
                bad[m] = f"not supported with {d}"
        elif not (tabled and len(tabled) == len(others)
                  and allowed_by_tables):
            bad[m] = "only with: " + " | ".join(describe(p) for p in rule)
    return bad
