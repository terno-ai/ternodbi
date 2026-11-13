"""Entity listings (campaigns, ads, audiences, …): endpoint, columns and
paging for each."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass

from terno_dbi.connectors.api.model.errors import ApiError, ErrorCode
from terno_dbi.connectors.api.model.types import Field
from terno_dbi.connectors.api.sources.tiktok._api import _PAGE_SIZE


@dataclass(frozen=True)
class _Listing:
    """How to fetch one kind of entity."""

    path: str
    fields: Tuple[Field, ...]
    # Keys under `data` that may hold the items; the first present wins.
    list_keys: Tuple[str, ...] = ("list",)
    page_size: int = _PAGE_SIZE
    # Send the requested columns as `fields` (only some endpoints accept it).
    send_fields: bool = False
    # Send the query's date range.
    dated: bool = False
    paged: bool = True
    # `data` is one record rather than a list.
    single: bool = False
    static: Tuple[Tuple[str, Any], ...] = ()
    # (setting, query param, "ids" | "str"); "ids" is sent as a JSON list.
    setting_params: Tuple[Tuple[str, str, str], ...] = ()
    # Settings sent inside `filtering`:
    # (setting, "ids" | "enums" | "str", max items, allowed values, required).
    filter_settings: Tuple[Tuple[str, str, Optional[int], Tuple[str, ...],
                                 bool], ...] = ()


_LISTINGS: Dict[str, _Listing] = {
    "Campaigns": _Listing("campaign/get/", (
        Field("campaign_id", "Campaign ID", "dimension"),
        Field("campaign_name", "Campaign", "dimension"),
        Field("objective_type", "Objective", "dimension"),
        Field("operation_status", "Status", "dimension",
              "ENABLE or DISABLE, as set by the advertiser."),
        Field("secondary_status", "Delivery status", "dimension",
              "Effective delivery status."),
        Field("budget_mode", "Budget mode", "dimension"),
        Field("budget", "Budget", "dimension", data_type="number",
              is_monetary=True),
        Field("budget_optimize_on", "Campaign budget optimisation", "dimension"),
        Field("special_industries", "Special ad categories", "dimension"),
        Field("create_time", "Created", "dimension", data_type="datetime"),
        Field("modify_time", "Modified", "dimension", data_type="datetime"),
    ), send_fields=True),
    "AdGroups": _Listing("adgroup/get/", (
        Field("adgroup_id", "Ad group ID", "dimension"),
        Field("adgroup_name", "Ad group", "dimension"),
        Field("campaign_id", "Campaign ID", "dimension"),
        Field("campaign_name", "Campaign", "dimension"),
        Field("operation_status", "Status", "dimension"),
        Field("secondary_status", "Delivery status", "dimension"),
        Field("promotion_type", "Promotion type", "dimension"),
        Field("placement_type", "Placement type", "dimension"),
        Field("placements", "Placements", "dimension"),
        Field("location_ids", "Locations", "dimension"),
        Field("age_groups", "Age groups", "dimension"),
        Field("gender", "Gender", "dimension"),
        Field("languages", "Languages", "dimension"),
        Field("operating_systems", "Operating systems", "dimension"),
        Field("audience_ids", "Audiences", "dimension"),
        Field("excluded_audience_ids", "Excluded audiences", "dimension"),
        Field("interest_category_ids", "Interest categories", "dimension"),
        Field("optimization_goal", "Optimization goal", "dimension"),
        Field("optimization_event", "Optimization event", "dimension"),
        Field("pixel_id", "Pixel ID", "dimension"),
        Field("billing_event", "Billing event", "dimension"),
        Field("bid_type", "Bid strategy", "dimension"),
        Field("bid_price", "Bid", "dimension", data_type="number",
              is_monetary=True),
        Field("conversion_bid_price", "Target cost per conversion",
              "dimension", data_type="number", is_monetary=True),
        Field("pacing", "Pacing", "dimension"),
        Field("budget_mode", "Budget mode", "dimension"),
        Field("budget", "Budget", "dimension", data_type="number",
              is_monetary=True),
        Field("schedule_type", "Schedule type", "dimension"),
        Field("schedule_start_time", "Start time", "dimension",
              data_type="datetime"),
        Field("schedule_end_time", "End time", "dimension",
              data_type="datetime"),
        Field("create_time", "Created", "dimension", data_type="datetime"),
        Field("modify_time", "Modified", "dimension", data_type="datetime"),
    ), send_fields=True),
    "Ads": _Listing("ad/get/", (
        Field("ad_id", "Ad ID", "dimension"),
        Field("ad_name", "Ad", "dimension"),
        Field("adgroup_id", "Ad group ID", "dimension"),
        Field("adgroup_name", "Ad group", "dimension"),
        Field("campaign_id", "Campaign ID", "dimension"),
        Field("campaign_name", "Campaign", "dimension"),
        Field("operation_status", "Status", "dimension"),
        Field("secondary_status", "Delivery status", "dimension"),
        Field("ad_format", "Ad format", "dimension"),
        Field("ad_text", "Ad text", "dimension"),
        Field("call_to_action", "Call to action", "dimension"),
        Field("landing_page_url", "Landing page", "dimension"),
        Field("identity_type", "Identity type", "dimension"),
        Field("identity_id", "Identity ID", "dimension"),
        Field("display_name", "Display name", "dimension"),
        Field("video_id", "Video ID", "dimension"),
        Field("image_ids", "Image IDs", "dimension"),
        Field("tiktok_item_id", "Spark post ID", "dimension"),
        Field("create_time", "Created", "dimension", data_type="datetime"),
        Field("modify_time", "Modified", "dimension", data_type="datetime"),
    ), send_fields=True),
    "Audiences": _Listing("dmp/custom_audience/list/", (
        Field("custom_audience_id", "Audience ID", "dimension"),
        Field("name", "Audience", "dimension"),
        Field("audience_type", "Type", "dimension"),
        Field("calculate_type", "Encryption type", "dimension"),
        Field("cover_num", "Size", "dimension", data_type="integer"),
        Field("is_valid", "Usable", "dimension"),
        Field("is_expiring", "Expiring soon", "dimension"),
        Field("expired_time", "Expires", "dimension", data_type="datetime"),
        Field("shared", "Shared", "dimension"),
        Field("is_creator", "Created by this advertiser", "dimension"),
        Field("create_time", "Created", "dimension", data_type="datetime"),
    ), page_size=100),
    "Images": _Listing("file/image/ad/search/", (
        Field("image_id", "Image ID", "dimension"),
        Field("file_name", "File name", "dimension"),
        Field("width", "Width", "dimension", data_type="integer"),
        Field("height", "Height", "dimension", data_type="integer"),
        Field("format", "Format", "dimension"),
        Field("image_url", "Image URL", "dimension"),
        Field("create_time", "Created", "dimension", data_type="datetime"),
    ), page_size=100),
    "Videos": _Listing("file/video/ad/search/", (
        Field("video_id", "Video ID", "dimension"),
        Field("file_name", "File name", "dimension"),
        Field("duration", "Duration (s)", "dimension", data_type="number"),
        Field("width", "Width", "dimension", data_type="integer"),
        Field("height", "Height", "dimension", data_type="integer"),
        Field("video_cover_url", "Cover URL", "dimension"),
        Field("preview_url", "Preview URL", "dimension"),
        Field("create_time", "Created", "dimension", data_type="datetime"),
    ), page_size=100),
    "Identities": _Listing("identity/get/", (
        Field("identity_id", "Identity ID", "dimension"),
        Field("identity_type", "Identity type", "dimension"),
        Field("display_name", "Display name", "dimension"),
        Field("username", "Username", "dimension"),
        Field("profile_image", "Profile image", "dimension"),
        Field("available_status", "Availability", "dimension"),
        Field("can_pull_video", "Can use its posts", "dimension"),
        Field("can_push_video", "Can push posts", "dimension"),
    ), list_keys=("identity_list",), page_size=100),
    "SparkPosts": _Listing("tt_video/list/", (
        Field("item_id", "Spark post ID", "dimension",
              "Pass as tiktok_item_id to create_ad."),
        Field("text", "Post text", "dimension"),
        Field("item_type", "Post type", "dimension", "VIDEO or CAROUSEL."),
        Field("status", "Post status", "dimension"),
        Field("identity_id", "Identity ID", "dimension"),
        Field("identity_type", "Identity type", "dimension"),
        Field("tiktok_name", "Creator", "dimension"),
        Field("ad_auth_status", "Authorisation status", "dimension",
              "EXPIRED, APPLYING, AUTHORIZED, NORESPONSE, REJECTED, CANCEL."),
        Field("auth_start_time", "Authorised from", "dimension",
              data_type="datetime"),
        Field("auth_end_time", "Authorised until", "dimension",
              data_type="datetime"),
    ), page_size=50),
    "Pixels": _Listing("pixel/list/", (
        Field("pixel_id", "Pixel ID", "dimension"),
        Field("pixel_name", "Pixel", "dimension"),
        Field("pixel_code", "Pixel code", "dimension"),
        Field("pixel_category", "Category", "dimension"),
        Field("pixel_setup_mode", "Setup mode", "dimension"),
        Field("partner_name", "Partner", "dimension"),
        Field("create_time", "Created", "dimension", data_type="datetime"),
    ), list_keys=("pixels",), page_size=20),
    "Comments": _Listing("comment/list/", (
        Field("comment_id", "Comment ID", "dimension"),
        Field("comment_type", "Type", "dimension", "COMMENT or REPLY."),
        Field("content", "Text", "dimension"),
        Field("comment_status", "Visibility", "dimension", "PUBLIC or HIDDEN."),
        Field("original_comment_id", "Replied-to comment ID", "dimension"),
        Field("hit_blockedword", "Hit a blocked word", "dimension"),
        Field("is_pinned", "Pinned", "dimension"),
        Field("can_delete", "Deletable", "dimension"),
        Field("ad_id", "Ad ID", "dimension"),
        Field("ad_name", "Ad", "dimension"),
        Field("adgroup_id", "Ad group ID", "dimension"),
        Field("adgroup_name", "Ad group", "dimension"),
        Field("campaign_id", "Campaign ID", "dimension"),
        Field("campaign_name", "Campaign", "dimension"),
        Field("tiktok_item_id", "Post ID", "dimension"),
        Field("identity_id", "Ad identity ID", "dimension"),
        Field("identity_type", "Ad identity type", "dimension"),
        Field("user_id", "Commenter ID", "dimension"),
        Field("user_name", "Commenter", "dimension"),
        Field("likes", "Likes", "dimension", data_type="integer"),
        Field("replies", "Replies", "dimension", data_type="integer"),
        Field("create_time", "Posted", "dimension", data_type="datetime"),
    ), list_keys=("comments",), page_size=100, dated=True),
    "AutomatedRules": _Listing("optimizer/rule/list/", (
        Field("rule_id", "Rule ID", "dimension"),
        Field("name", "Rule", "dimension"),
        Field("rule_status", "Status", "dimension"),
        Field("apply_objects", "Applies to", "dimension"),
        Field("conditions", "Conditions", "dimension"),
        Field("action", "Actions", "dimension"),
        Field("notification", "Notification", "dimension"),
        Field("rule_exec_info", "Schedule", "dimension"),
        Field("last_check_result_summary", "Last run", "dimension"),
        Field("create_datetime", "Created", "dimension", data_type="datetime"),
    ), list_keys=("rules",), page_size=10),
    # GmvMaxStores gives the `store_id` every GMV Max report needs.
    "GmvMaxStores": _Listing("gmv_max/store/list/", (
        Field("store_id", "Shop ID", "dimension"),
        Field("store_name", "Shop", "dimension"),
        Field("store_code", "Shop code", "dimension"),
        Field("store_status", "Shop status", "dimension",
              "ACTIVE, INACTIVE or NEW_CREATE."),
        Field("is_gmv_max_available", "Available for GMV Max", "dimension",
              "Only shops with true can be used in GMV Max reports."),
        Field("store_role", "Shop role", "dimension",
              "AD_PROMOTION, MANAGER or UNSET."),
        Field("targeting_region_codes", "Targetable regions", "dimension"),
        Field("thumbnail_url", "Thumbnail URL", "dimension"),
        Field("store_authorized_bc_id", "Authorised Business Center ID",
              "dimension"),
        Field("is_owner_bc", "Business Center owns the shop", "dimension"),
        Field("bc_name", "Business Center", "dimension"),
        Field("user_role", "Your Business Center role", "dimension"),
        Field("exclusive_advertiser_id", "Exclusive GMV Max ad account ID",
              "dimension"),
        Field("exclusive_advertiser_name", "Exclusive GMV Max ad account",
              "dimension"),
        Field("exclusive_advertiser_status",
              "Exclusive GMV Max ad account status", "dimension"),
    ), list_keys=("store_list",), paged=False),
    "GmvMaxCampaigns": _Listing("gmv_max/campaign/get/", (
        Field("campaign_id", "Campaign ID", "dimension"),
        Field("campaign_name", "Campaign", "dimension"),
        Field("operation_status", "Status", "dimension", "ENABLE or DISABLE."),
        Field("secondary_status", "Delivery status", "dimension"),
        Field("objective_type", "Objective", "dimension"),
        Field("roi_protection_compensation_status", "ROI protection",
              "dimension", "IN_EFFECT or NOT_ELIGIBLE."),
        Field("create_time", "Created (UTC)", "dimension"),
        Field("modify_time", "Modified (UTC)", "dimension"),
        Field("advertiser_id", "Advertiser ID", "dimension"),
    ), page_size=100, filter_settings=(
        ("gmv_max_promotion_types", "enums", None,
         ("PRODUCT_GMV_MAX", "LIVE_GMV_MAX"), True),
        ("store_ids", "ids", 10, (), False),
        ("campaign_ids", "ids", 100, (), False),
        ("campaign_name", "str", None, (), False),
        ("primary_status", "str", None,
         ("STATUS_DELIVERY_OK", "STATUS_DISABLE", "STATUS_DELETE"), False),
        ("creation_filter_start_time", "str", None, (), False),
        ("creation_filter_end_time", "str", None, (), False),
    )),
    "SmartPlusCampaigns": _Listing("smart_plus/campaign/get/", (
        Field("campaign_id", "Campaign id", "dimension"),
        Field("campaign_name", "Campaign name", "dimension"),
        Field("objective_type", "Objective type", "dimension"),
        Field("app_promotion_type", "App promotion type", "dimension"),
        Field("sales_destination", "Sales destination", "dimension"),
        Field("campaign_type", "Campaign type", "dimension"),
        Field("app_id", "App id", "dimension"),
        Field("is_search_campaign", "Is search campaign", "dimension"),
        Field("catalog_enabled", "Catalog enabled", "dimension"),
        Field("catalog_type", "Catalog type", "dimension"),
        Field("special_industries", "Special industries", "dimension"),
        Field("budget_optimize_on", "Budget optimize on", "dimension"),
        Field("budget_mode", "Budget mode", "dimension"),
        Field("budget_auto_adjust_strategy", "Budget auto adjust strategy", "dimension"),
        Field("budget", "Budget", "dimension"),
        Field("current_budget", "Current budget", "dimension"),
        Field("operation_status", "Operation status", "dimension"),
        Field("effective_status", "Effective status", "dimension"),
        Field("secondary_status", "Secondary status", "dimension"),
        Field("smart_plus_adgroup_mode", "Smart plus adgroup mode", "dimension"),
        Field("postback_window_mode", "Postback window mode", "dimension"),
        Field("po_number", "Po number", "dimension"),
        Field("is_promotional_campaign", "Is promotional campaign", "dimension"),
        Field("is_advanced_dedicated_campaign", "Is advanced dedicated campaign", "dimension"),
        Field("disable_skan_campaign", "Disable skan campaign", "dimension"),
        Field("bid_align_type", "Bid align type", "dimension"),
        Field("campaign_app_profile_page_state", "Campaign app profile page state", "dimension"),
        Field("gaming_ad_compliance_agreement", "Gaming ad compliance agreement", "dimension"),
        Field("rta_id", "Rta id", "dimension"),
        Field("rta_bid_enabled", "Rta bid enabled", "dimension"),
        Field("rta_product_selection_enabled", "Rta product selection enabled", "dimension"),
        Field("create_time", "Create time", "dimension", data_type="datetime"),
        Field("modify_time", "Modify time", "dimension", data_type="datetime"),
    ), send_fields=True),
    "SmartPlusAdGroups": _Listing("smart_plus/adgroup/get/", (
        Field("adgroup_id", "Adgroup id", "dimension"),
        Field("adgroup_name", "Adgroup name", "dimension"),
        Field("campaign_id", "Campaign id", "dimension"),
        Field("campaign_name", "Campaign name", "dimension"),
        Field("operation_status", "Operation status", "dimension"),
        Field("effective_status", "Effective status", "dimension"),
        Field("secondary_status", "Secondary status", "dimension"),
        Field("promotion_type", "Promotion type", "dimension"),
        Field("promotion_website_type", "Promotion website type", "dimension"),
        Field("promotion_target_type", "Promotion target type", "dimension"),
        Field("optimization_goal", "Optimization goal", "dimension"),
        Field("optimization_event", "Optimization event", "dimension"),
        Field("optimization_locations", "Optimization locations", "dimension"),
        Field("pixel_id", "Pixel id", "dimension"),
        Field("app_id", "App id", "dimension"),
        Field("app_config", "App config", "dimension"),
        Field("minis_id", "Minis id", "dimension"),
        Field("catalog_id", "Catalog id", "dimension"),
        Field("catalog_authorized_bc_id", "Catalog authorized bc id", "dimension"),
        Field("product_source", "Product source", "dimension"),
        Field("custom_conversion_id", "Custom conversion id", "dimension"),
        Field("deep_funnel_optimization_status", "Deep funnel optimization status", "dimension"),
        Field("deep_funnel_event_source", "Deep funnel event source", "dimension"),
        Field("deep_funnel_event_source_id", "Deep funnel event source id", "dimension"),
        Field("deep_funnel_optimization_event", "Deep funnel optimization event", "dimension"),
        Field("app_attribution_source", "App attribution source", "dimension"),
        Field("app_data_source", "App data source", "dimension"),
        Field("identity_id", "Identity id", "dimension"),
        Field("identity_type", "Identity type", "dimension"),
        Field("identity_authorized_bc_id", "Identity authorized bc id", "dimension"),
        Field("native_series_id", "Native series id", "dimension"),
        Field("drama_selling_setting", "Drama selling setting", "dimension"),
        Field("messaging_app_type", "Messaging app type", "dimension"),
        Field("zalo_id_type", "Zalo id type", "dimension"),
        Field("messaging_app_account_id", "Messaging app account id", "dimension"),
        Field("message_event_set_id", "Message event set id", "dimension"),
        Field("phone_info", "Phone info", "dimension"),
        Field("gaming_ad_compliance_agreement", "Gaming ad compliance agreement", "dimension"),
        Field("bid_type", "Bid type", "dimension"),
        Field("bid_price", "Bid price", "dimension"),
        Field("conversion_bid_price", "Conversion bid price", "dimension"),
        Field("deep_bid_type", "Deep bid type", "dimension"),
        Field("roas_bid", "Roas bid", "dimension"),
        Field("value_rule_set_id", "Value rule set id", "dimension"),
        Field("incentive_offer_type", "Incentive offer type", "dimension"),
        Field("vbo_window", "Vbo window", "dimension"),
        Field("click_attribution_window", "Click attribution window", "dimension"),
        Field("engaged_view_attribution_window", "Engaged view attribution window", "dimension"),
        Field("view_attribution_window", "View attribution window", "dimension"),
        Field("attribution_event_count", "Attribution event count", "dimension"),
        Field("billing_event", "Billing event", "dimension"),
        Field("pacing", "Pacing", "dimension"),
        Field("automated_keywords_enabled", "Automated keywords enabled", "dimension"),
        Field("search_keywords", "Search keywords", "dimension"),
        Field("budget_mode", "Budget mode", "dimension"),
        Field("budget_auto_adjust_strategy", "Budget auto adjust strategy", "dimension"),
        Field("budget", "Budget", "dimension"),
        Field("current_budget", "Current budget", "dimension"),
        Field("min_budget", "Min budget", "dimension"),
        Field("schedule_type", "Schedule type", "dimension"),
        Field("schedule_start_time", "Schedule start time", "dimension", data_type="datetime"),
        Field("schedule_end_time", "Schedule end time", "dimension", data_type="datetime"),
        Field("movie_premiere_date", "Movie premiere date", "dimension"),
        Field("dayparting", "Dayparting", "dimension"),
        Field("targeting_optimization_mode", "Targeting optimization mode", "dimension"),
        Field("suggestion_audience_enabled", "Suggestion audience enabled", "dimension"),
        Field("targeting_spec", "Targeting spec", "dimension"),
        Field("is_hfss", "Is hfss", "dimension"),
        Field("is_lhf_compliance", "Is lhf compliance", "dimension"),
        Field("placement_type", "Placement type", "dimension"),
        Field("placements", "Placements", "dimension"),
        Field("tiktok_subplacements", "Tiktok subplacements", "dimension"),
        Field("search_result_enabled", "Search result enabled", "dimension"),
        Field("organic_comment_disabled", "Organic comment disabled", "dimension"),
        Field("comment_disabled", "Comment disabled", "dimension"),
        Field("share_disabled", "Share disabled", "dimension"),
        Field("video_download_disabled", "Video download disabled", "dimension"),
        Field("skip_learning_phase", "Skip learning phase", "dimension"),
        Field("create_time", "Create time", "dimension", data_type="datetime"),
        Field("modify_time", "Modify time", "dimension", data_type="datetime"),
    ), send_fields=True),
    "SmartPlusAds": _Listing("smart_plus/ad/get/", (
        Field("smart_plus_ad_id", "Smart plus ad id", "dimension"),
        Field("ad_name", "Ad name", "dimension"),
        Field("adgroup_id", "Adgroup id", "dimension"),
        Field("adgroup_name", "Adgroup name", "dimension"),
        Field("campaign_id", "Campaign id", "dimension"),
        Field("campaign_name", "Campaign name", "dimension"),
        Field("operation_status", "Operation status", "dimension"),
        Field("effective_status", "Effective status", "dimension"),
        Field("secondary_status", "Secondary status", "dimension"),
        Field("creative_list", "Creative list", "dimension"),
        Field("playable_list", "Playable list", "dimension"),
        Field("ad_text_list", "Ad text list", "dimension"),
        Field("auto_message_list", "Auto message list", "dimension"),
        Field("call_to_action_list", "Call to action list", "dimension"),
        Field("interactive_add_on_list", "Interactive add on list", "dimension"),
        Field("page_list", "Page list", "dimension"),
        Field("landing_page_url_list", "Landing page url list", "dimension"),
        Field("custom_product_page_list", "Custom product page list", "dimension"),
        Field("deeplink_list", "Deeplink list", "dimension"),
        Field("multi_dest_list", "Multi dest list", "dimension"),
        Field("disclaimer", "Disclaimer", "dimension"),
        Field("ad_configuration", "Ad configuration", "dimension"),
        Field("create_time", "Create time", "dimension", data_type="datetime"),
        Field("modify_time", "Modify time", "dimension", data_type="datetime"),
    ), send_fields=True, page_size=100),
    "SmartPlusValueRuleSets": _Listing("smart_plus/value_rule_set/get/", (
        Field("value_rule_set_id", "Value rule set id", "dimension"),
        Field("rule_set_name", "Rule set name", "dimension"),
        Field("rules", "Rules", "dimension"),
        Field("create_time", "Create time", "dimension", data_type="datetime"),
    ), list_keys=("value_rule_sets",), paged=False,
        static=(("with_rules", "true"),)),
    "SmartPlusAdReview": _Listing("smart_plus/ad/review_info/", (
        Field("smart_plus_ad_id", "Smart plus ad id", "dimension"),
        Field("review_status", "Review status", "dimension"),
        Field("passed_locations", "Passed locations", "dimension"),
        Field("appeal_status", "Appeal status", "dimension"),
        Field("appeal_reject_reasons", "Appeal reject reasons", "dimension"),
        Field("reject_info", "Reject info", "dimension"),
    ), list_keys=("smart_plus_ad_review_infos",), paged=False,
        static=(("extra_info_setting", '{"include_reject_info": true}'),),
        setting_params=(("smart_plus_ad_ids", "smart_plus_ad_ids", "ids"),)),
    "SmartPlusMaterialReview": _Listing("smart_plus/material/review_info/", (
        Field("ad_material_id", "Ad material id", "dimension"),
        Field("smart_plus_ad_id", "Smart plus ad id", "dimension"),
        Field("review_status", "Review status", "dimension"),
        Field("passed_locations", "Passed locations", "dimension"),
        Field("reject_info", "Reject info", "dimension"),
    ), list_keys=("material_review_infos",), paged=False,
        static=(("extra_info_setting", '{"include_reject_info": true}'),),
        setting_params=(("ad_material_ids", "ad_material_ids", "ids"),)),
    "SmartPlusCopyTask": _Listing("smart_plus/campaign/copy/task/check/", (
        Field("task_status", "Task status", "dimension"),
        Field("task_info", "Task info", "dimension"),
        Field("task_result", "Task result", "dimension"),
    ), paged=False, single=True,
        setting_params=(("task_id", "task_id", "str"),)),
}


_ENTITY_ENDPOINT: Dict[str, str] = {k: v.path for k, v in _LISTINGS.items()}


_ENTITY_FIELDS: Dict[str, List[Field]] = {
    k: list(v.fields) for k, v in _LISTINGS.items()}


# Columns stored under a different key in some responses.
_ALIASES: Dict[str, Tuple[str, ...]] = {
    # Nested under exclusive_authorized_advertiser_info.
    "exclusive_advertiser_id": ("advertiser_id",),
    "exclusive_advertiser_name": ("advertiser_name",),
    "exclusive_advertiser_status": ("advertiser_status",),
    # The audience list calls it `audience_id`.
    "custom_audience_id": ("audience_id", "custom_audience_id"),
}


def _filter_from_settings(report_type: str, settings: Dict[str, Any],
                          specs) -> Dict[str, Any]:
    """Build `filtering` from report settings, rejecting values TikTok
    would refuse (too many items, unknown enum values)."""
    out: Dict[str, Any] = {}
    for name, kind, max_size, enum, required in specs:
        raw = settings.get(name)
        if raw in (None, "", []):
            if required:
                raise ApiError(
                    ErrorCode.MISSING_SETTING,
                    f"The {report_type} report needs the {name} setting"
                    + (f" (one or more of {', '.join(enum)})" if enum else "")
                    + ".", retriable=False, details={"setting": name})
            continue
        if kind == "str":
            if isinstance(raw, list):
                if len(raw) != 1:
                    raise ApiError(ErrorCode.INVALID_FILTER,
                                   f"{name} takes one value.",
                                   retriable=False)
                raw = raw[0]
            value = str(raw).strip()
            if enum and value not in enum:
                raise ApiError(ErrorCode.INVALID_FILTER,
                               f"{name} must be one of {', '.join(enum)}.",
                               retriable=False)
            out[name] = value
            continue
        values = raw if isinstance(raw, list) else \
            [v.strip() for v in str(raw).split(",")]
        values = [str(v).strip() for v in values if str(v).strip()]
        if max_size and len(values) > max_size:
            raise ApiError(ErrorCode.INVALID_FILTER,
                           f"{name} takes at most {max_size} values "
                           f"(TikTok).", retriable=False)
        bad = [v for v in values if enum and v not in enum]
        if bad:
            raise ApiError(ErrorCode.INVALID_FILTER,
                           f"{name}: {', '.join(bad)} not allowed; use "
                           f"{', '.join(enum)}.", retriable=False)
        out[name] = values
    return out


def _lookup(obj: Dict[str, Any], name: str) -> Any:
    """Read a column, also looking one level into nested objects (Spark
    posts and GMV Max stores nest some fields)."""
    keys = _ALIASES.get(name, (name,))
    for key in keys:
        if key in obj:
            return obj[key]
    for value in obj.values():
        if isinstance(value, dict):
            for key in keys:
                if key in value:
                    return value[key]
    return None
