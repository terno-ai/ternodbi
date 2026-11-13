"""Write actions: their ids, parameter schemas and the input checks the
handlers share.

Endpoints, fields and enum values follow TikTok's API reference;
tests/unit/connectors/api/fixtures/tiktok_ads_official_v1_3.json holds the
parameter lists the tests check requests against.
"""

from __future__ import annotations

import hashlib
import re
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timedelta, timezone

from terno_dbi.connectors.api.model.types import Action, ActionResult
from terno_dbi.connectors.api.model.errors import ApiError, ErrorCode


_UNVERIFIED_ENDPOINTS: frozenset = frozenset()


# Allowed values, from the API reference's enumerations appendix.
_ENUMS: Dict[str, Tuple[str, ...]] = {
    "objective_type": (
        "APP_PROMOTION", "WEB_CONVERSIONS", "APP_INSTALL", "CONVERSIONS",
        "REACH", "BRAND_CONSIDERATION", "TRAFFIC", "VIDEO_VIEWS",
        "CATALOG_SALES", "PRODUCT_SALES", "ENGAGEMENT", "LEAD_GENERATION",
        "SHOP_PURCHASES", "RF_APP_INSTALL", "RF_ENGAGEMENT", "RF_REACH",
        "TOPVIEW_REACH", "RF_TRAFFIC", "RF_VIDEO_VIEW"),
    "app_promotion_type": ("APP_INSTALL", "APP_RETARGETING",
                           "APP_PREREGISTRATION"),
    "campaign_type": ("REGULAR_CAMPAIGN", "IOS14_CAMPAIGN"),
    "special_industries": ("HOUSING", "EMPLOYMENT", "CREDIT"),
    "budget_mode": ("BUDGET_MODE_DAY", "BUDGET_MODE_DYNAMIC_DAILY_BUDGET",
                    "BUDGET_MODE_TOTAL", "BUDGET_MODE_INFINITE"),
    "pacing": ("PACING_MODE_SMOOTH", "PACING_MODE_FAST"),
    "bid_type": ("BID_TYPE_CUSTOM", "BID_TYPE_NO_BID"),
    "billing_event": ("CPC", "CPM", "CPV", "GD", "OCPM"),
    "schedule_type": ("SCHEDULE_FROM_NOW", "SCHEDULE_START_END"),
    "placement_type": ("PLACEMENT_TYPE_AUTOMATIC", "PLACEMENT_TYPE_NORMAL"),
    "placements": ("PLACEMENT_TIKTOK", "PLACEMENT_PANGLE",
                   "PLACEMENT_GLOBAL_APP_BUNDLE", "PLACEMENT_TOPBUZZ",
                   "PLACEMENT_HELO"),
    "promotion_type": (
        "APP_ANDROID", "APP_IOS", "MINI_APP", "MINI_GAME", "GAME", "WEBSITE",
        "LEAD_GENERATION", "LEAD_GEN_CLICK_TO_TT_DIRECT_MESSAGE",
        "LEAD_GEN_CLICK_TO_SOCIAL_MEDIA_APP_MESSAGE", "LEAD_GEN_CLICK_TO_CALL",
        "WEBSITE_OR_DISPLAY", "TIKTOK_SHOP", "VIDEO_SHOPPING", "LIVE_SHOPPING",
        "PSA_PRODUCT"),
    "optimization_goal": (
        "CLICK", "CONVERT", "INSTALL", "IN_APP_EVENT", "SHOW", "VIDEO_VIEW",
        "REACH", "LEAD_GENERATION", "LEADS", "WEBSITE_ENGAGEMENT",
        "CONVERSATION", "FOLLOWERS", "PROFILE_VIEWS", "PAGE_VISIT", "VALUE",
        "AUTOMATIC_VALUE_OPTIMIZATION", "GMV", "PURCHASES",
        "INITIATE_CHECKOUTS", "MT_LIVE_ROOM", "PRODUCT_CLICK_IN_LIVE",
        "ENGAGED_VIEW", "ENGAGED_VIEW_FIFTEEN", "TRAFFIC_LANDING_PAGE_VIEW",
        "CONVERSION_LEADS", "DESTINATION_VISIT", "PREFERRED_LEAD"),
    "gender": ("GENDER_UNLIMITED", "GENDER_MALE", "GENDER_FEMALE"),
    "age_groups": ("AGE_13_17", "AGE_18_24", "AGE_25_34", "AGE_35_44",
                   "AGE_45_54", "AGE_55_100"),
    "network_types": ("WIFI", "2G", "3G", "4G", "5G"),
    "operating_systems": ("ANDROID", "IOS"),
    "ad_format": ("SINGLE_IMAGE", "SINGLE_VIDEO", "LIVE_CONTENT",
                  "CAROUSEL_ADS", "CATALOG_CAROUSEL"),
    "identity_type": ("CUSTOMIZED_USER", "AUTH_CODE", "TT_USER", "BC_AUTH_TT"),
    "deeplink_type": ("NORMAL", "DEFERRED_DEEPLINK"),
    "call_to_action": (
        "APPLY_NOW", "BOOK_NOW", "CALL_NOW", "CHECK_AVAILABILITY",
        "CONTACT_US", "DOWNLOAD_NOW", "EXPERIENCE_NOW", "GET_QUOTE",
        "GET_SHOWTIMES", "GET_TICKETS_NOW", "INSTALL_NOW", "INTERESTED",
        "LEARN_MORE", "LISTEN_NOW", "ORDER_NOW", "PLAY_GAME", "PREORDER_NOW",
        "READ_MORE", "SEND_MESSAGE", "SHOP_NOW", "SIGN_UP", "SUBSCRIBE",
        "VIEW_NOW", "VIEW_PROFILE", "VISIT_STORE", "WATCH_LIVE", "WATCH_NOW",
        "JOIN_THIS_HASHTAG", "SHOOT_WITH_THIS_EFFECT",
        "VIEW_VIDEO_WITH_THIS_EFFECT"),
    "audience_type": (
        "ENGAGEMENT", "ENGAGEMENT_ORGANIC_VIDEO", "ENGAGEMENT_LIVE_VIDEO",
        "APP", "PIXEL", "LEAD_GENERATION", "BUSINESS_ACCOUNT", "TIKTOK_SHOP",
        "OFFLINE"),
}


# comment/post/ and comment/delete/ take only these identity types.
_COMMENT_IDENTITY_TYPES = ("CUSTOMIZED_USER", "TT_USER")


# Objectives for which promotion_type is not set on an ad group.
_NO_PROMOTION_TYPE_OBJECTIVES = frozenset({"REACH", "VIDEO_VIEWS",
                                           "ENGAGEMENT"})


# Reach & Frequency campaigns can't be created paused, so we don't create them.
_RF_OBJECTIVES = frozenset({"RF_APP_INSTALL", "RF_ENGAGEMENT", "RF_REACH",
                            "RF_TRAFFIC", "RF_VIDEO_VIEW"})


_LOOKALIKE_OS = ("ALL", "ANDROID", "IOS")


_LOOKALIKE_PLACEMENTS = ("TikTok", "TopBuzz & BuzzVideo", "Pangle")


# Keys every automated rule needs.
_RULE_REQUIRED = ("name", "apply_objects", "conditions", "actions",
                  "notification", "rule_exec_info")


_ID_RE = re.compile(r"^\d{1,25}$")


_TOKEN_RE = re.compile(r"^[A-Z0-9][A-Z0-9_]*$")   # e.g. 4G, AGE_18_24


_DT_RE = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$")


_HEX64_RE = re.compile(r"^[0-9a-f]{64}$")


_MAX_BATCH = 20                 # ids per status/delete call


_MAX_MEMBERS = 100_000          # identifiers per audience upload


_KIND_SCHEMA: Dict[str, Dict[str, Any]] = {
    "id": {"type": "string", "pattern": r"^\d+$"},
    "ids": {"type": "array", "items": {"type": "string", "pattern": r"^\d+$"},
            "minItems": 1},
    "text": {"type": "string", "minLength": 1},
    "texts": {"type": "array", "items": {"type": "string", "minLength": 1},
              "minItems": 1},
    "token": {"type": "string", "pattern": r"^[A-Za-z0-9][A-Za-z0-9_]*$"},
    "tokens": {"type": "array",
               "items": {"type": "string", "pattern": r"^[A-Za-z0-9][A-Za-z0-9_]*$"},
               "minItems": 1},
    "amount": {"type": "number", "exclusiveMinimum": 0},
    "int": {"type": "integer", "minimum": 1},
    "bool": {"type": "boolean"},
    "datetime": {"type": "string",
                 "pattern": r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$"},
    "url": {"type": "string", "pattern": r"^https?://"},
    "object": {"type": "object"},
}


# Parameter descriptions shown in action schemas.
_DESC: Dict[str, str] = {
    "campaign_id": "Campaign id, from the campaign_id column of a report.",
    "campaign_ids": "Several campaign ids (up to 20).",
    "adgroup_id": "Ad group id, from the adgroup_id column of a report.",
    "adgroup_ids": "Several ad group ids (up to 20).",
    "ad_id": "Ad id, from the ad_id column of a report.",
    "ad_ids": "Several ad ids (up to 20).",
    "campaign_name": "Campaign name.",
    "objective_type": "Advertising objective. Reach & Frequency (RF_*) "
                      "campaigns cannot be created paused, so they are not "
                      "created here.",
    "budget_mode": "Campaign: BUDGET_MODE_INFINITE, BUDGET_MODE_TOTAL or "
                   "BUDGET_MODE_DAY without budget optimisation; "
                   "BUDGET_MODE_TOTAL, BUDGET_MODE_DYNAMIC_DAILY_BUDGET or "
                   "BUDGET_MODE_DAY with it. Ad group: BUDGET_MODE_DAY, "
                   "BUDGET_MODE_DYNAMIC_DAILY_BUDGET or BUDGET_MODE_TOTAL "
                   "(TOTAL needs SCHEDULE_START_END). "
                   "BUDGET_MODE_DYNAMIC_DAILY_BUDGET is allowlist-only.",
    "budget": "Budget in the advertiser's currency (e.g. 50 = 50.00). TikTok "
              "enforces a per-currency range and refuses values outside it.",
    "special_industries": "Special ad categories (housing, employment, credit).",
    "budget_optimize_on": "Campaign budget optimisation: the campaign budget "
                          "is shared across its ad groups.",
    "app_promotion_type": "Required when objective_type is APP_PROMOTION.",
    "campaign_type": "Campaign type (iOS 14 dedicated or regular).",
    "adgroup_name": "Ad group name.",
    "promotion_type": "Optimization location. Required unless the campaign "
                      "objective is REACH, VIDEO_VIEWS or ENGAGEMENT.",
    "placement_type": "Automatic or selected placement.",
    "placements": "Required with PLACEMENT_TYPE_NORMAL.",
    "location_ids": "TikTok location ids to target (up to 3,000), from "
                    "TikTok's targeting search.",
    "schedule_type": "SCHEDULE_FROM_NOW (runs until paused) or "
                     "SCHEDULE_START_END.",
    "schedule_start_time": "Start, 'YYYY-MM-DD HH:MM:SS' in UTC.",
    "schedule_end_time": "End, 'YYYY-MM-DD HH:MM:SS' in UTC (needed for "
                         "SCHEDULE_START_END).",
    "dayparting": "Hours to run: 336 chars, one per half hour Mon→Sun, '1' on.",
    "optimization_goal": "What the ad group optimises for.",
    "optimization_event": "Conversion event to optimise for (with a pixel "
                          "or app).",
    "billing_event": "What you pay for.",
    "bid_type": "BID_TYPE_NO_BID (lowest cost) or BID_TYPE_CUSTOM.",
    "bid_price": "Bid per click/1,000 impressions/view, in account currency.",
    "conversion_bid_price": "Target cost per conversion (Cost Cap).",
    "deep_cpa_bid": "Target cost for the deep (secondary) event.",
    "roas_bid": "Target ROAS for value optimisation.",
    "pacing": "PACING_MODE_SMOOTH or PACING_MODE_FAST.",
    "pixel_id": "Pixel id, from the Pixels report.",
    "gender": "Gender to target.",
    "age_groups": "Age groups to target.",
    "languages": "TikTok language codes to target.",
    "operating_systems": "Operating systems to target.",
    "network_types": "Connection types to target.",
    "interest_category_ids": "Interest category ids.",
    "interest_keyword_ids": "Interest keyword ids.",
    "audience_ids": "Custom/lookalike audience ids to target (Audiences "
                    "report). Replaces the current list.",
    "excluded_audience_ids": "Audience ids to exclude. Replaces the current "
                             "list.",
    "frequency": "Frequency cap: impressions per person per "
                 "frequency_schedule days. REACH ads only.",
    "frequency_schedule": "Frequency cap window in days (REACH ads only).",
    "ad_name": "Ad name.",
    "ad_format": "Ad format; SINGLE_VIDEO when not given.",
    "identity_type": "Identity type (Identities / SparkPosts reports).",
    "identity_id": "Identity id, from the Identities or SparkPosts report.",
    "video_id": "Video id, from upload_video or the Videos report.",
    "image_ids": "Image ids, from upload_image or the Images report "
                 "(a video's cover, or the images of an image/carousel ad).",
    "ad_text": "Ad copy: up to 100 characters, no emoji. Required except "
               "for Spark Ads.",
    "call_to_action": "Call-to-action button.",
    "landing_page_url": "Destination URL.",
    "display_name": "Display name shown on the ad (1–40 characters).",
    "tiktok_item_id": "Spark Ads: the TikTok post id (SparkPosts report).",
    "deeplink": "App deep link.",
    "deeplink_type": "NORMAL (default) or DEFERRED_DEEPLINK.",
    "tracking_pixel_id": "Pixel id to attribute this ad's events to.",
    "impression_tracking_url": "Third-party impression tracking URL.",
    "click_tracking_url": "Third-party click tracking URL.",
    "card_id": "Creative portfolio id (call-to-action, display card, "
               "sticker, …).",
    "page_id": "Page id (instant page / lead form).",
    "extra": "Advanced: any further TikTok fields for this object, passed "
             "through as-is (see the TikTok API for Business docs). Cannot "
             "set ids, the advertiser or the status.",
}


_CAMPAIGN_PROPS: Dict[str, str] = {
    "campaign_name": "text", "objective_type": "token", "budget_mode": "token",
    "budget": "amount", "special_industries": "tokens",
    "budget_optimize_on": "bool", "app_promotion_type": "token",
    "campaign_type": "token",
}


_CAMPAIGN_UPDATABLE = ("campaign_name", "budget", "special_industries")


_ADGROUP_PROPS: Dict[str, str] = {
    "adgroup_name": "text", "promotion_type": "token",
    "placement_type": "token", "placements": "tokens",
    "location_ids": "ids", "budget_mode": "token", "budget": "amount",
    "schedule_type": "token", "schedule_start_time": "datetime",
    "schedule_end_time": "datetime", "dayparting": "text",
    "optimization_goal": "token", "optimization_event": "token",
    "billing_event": "token", "bid_type": "token", "bid_price": "amount",
    "conversion_bid_price": "amount", "deep_cpa_bid": "amount",
    "roas_bid": "amount", "pacing": "token", "pixel_id": "id",
    "gender": "token", "age_groups": "tokens", "languages": "texts",
    "operating_systems": "tokens", "network_types": "tokens",
    "interest_category_ids": "ids", "interest_keyword_ids": "ids",
    "audience_ids": "ids", "excluded_audience_ids": "ids",
    "frequency": "int", "frequency_schedule": "int",
}


# Placement, promotion type, optimisation goal and event, billing event,
# pixel and budget mode are fixed once an ad group exists. deep_cpa_bid can
# only be set on update.
_ADGROUP_CREATE_ONLY_EXCLUDED = frozenset({"deep_cpa_bid"})


_ADGROUP_CREATE_KEYS: Tuple[str, ...] = ()   # set below


_ADGROUP_NOT_UPDATABLE = frozenset({
    "promotion_type", "placement_type", "placements", "optimization_goal",
    "optimization_event", "billing_event", "pixel_id", "budget_mode"})


_ADGROUP_CREATE_KEYS = tuple(k for k in _ADGROUP_PROPS
                             if k not in _ADGROUP_CREATE_ONLY_EXCLUDED)


# Fields each ad group action may change.
_ADGROUP_SETTERS: Dict[str, Tuple[str, ...]] = {
    "update_ad_group": tuple(k for k in _ADGROUP_PROPS
                             if k not in _ADGROUP_NOT_UPDATABLE),
    "set_ad_group_bid": ("bid_type", "bid_price", "conversion_bid_price",
                         "deep_cpa_bid", "roas_bid"),
    "set_ad_group_schedule": ("schedule_type", "schedule_start_time",
                              "schedule_end_time", "dayparting"),
    "set_ad_group_targeting": ("location_ids", "gender", "age_groups",
                               "languages", "operating_systems",
                               "network_types", "interest_category_ids",
                               "interest_keyword_ids", "frequency",
                               "frequency_schedule"),
    "set_ad_group_audiences": ("audience_ids", "excluded_audience_ids"),
}


_AD_PROPS: Dict[str, str] = {
    "ad_name": "text", "ad_format": "token", "identity_type": "token",
    "identity_id": "text", "video_id": "text", "image_ids": "texts",
    "ad_text": "text", "call_to_action": "token", "landing_page_url": "url",
    "display_name": "text", "tiktok_item_id": "text", "deeplink": "text",
    "deeplink_type": "token", "tracking_pixel_id": "id",
    "impression_tracking_url": "url", "click_tracking_url": "url",
    "card_id": "text", "page_id": "text",
}


# Keys `extra` may not override.
_PROTECTED = frozenset({
    "advertiser_id", "operation_status", "campaign_id", "campaign_ids",
    "adgroup_id", "adgroup_ids", "ad_id", "ad_ids", "creatives",
    "custom_audience_id", "custom_audience_ids", "rule_id", "rule_ids",
    "pixel_id",
})


def _p(kind: str, key: str, desc: Optional[str] = None) -> Dict[str, Any]:
    schema = dict(_KIND_SCHEMA[kind])
    if key in _ENUMS and kind == "token":
        schema = {"type": "string", "enum": list(_ENUMS[key])}
    elif key in _ENUMS and kind == "tokens":
        schema = {"type": "array", "minItems": 1,
                  "items": {"type": "string", "enum": list(_ENUMS[key])}}
    text = desc or _DESC.get(key)
    if text:
        schema["description"] = text
    return schema


def _props_schema(props: Dict[str, str], keys) -> Dict[str, Any]:
    return {k: _p(props[k], k) for k in keys}


def _obj(properties: Dict[str, Any], required=(), *, one_of=None,
         any_of=None) -> Dict[str, Any]:
    schema: Dict[str, Any] = {"type": "object", "properties": properties,
                              "required": list(required),
                              "additionalProperties": False}
    if one_of:
        schema["oneOf"] = [{"required": [k]} for k in one_of]
    if any_of:
        schema["anyOf"] = [{"required": [k]} for k in any_of]
    return schema


def _ids_schema(single: str, plural: str) -> Dict[str, Any]:
    return _obj({single: _p("id", single), plural: _p("ids", plural)},
                one_of=(single, plural))


_EXTRA = {"extra": _p("object", "extra")}


# Status action -> (object, operation_status).
_STATUS_ACTIONS: Dict[str, Tuple[str, str]] = {
    "pause_campaign": ("campaign", "DISABLE"),
    "enable_campaign": ("campaign", "ENABLE"),
    "delete_campaign": ("campaign", "DELETE"),
    "pause_ad_group": ("adgroup", "DISABLE"),
    "enable_ad_group": ("adgroup", "ENABLE"),
    "delete_ad_group": ("adgroup", "DELETE"),
    "pause_ad": ("ad", "DISABLE"),
    "enable_ad": ("ad", "ENABLE"),
    "delete_ad": ("ad", "DELETE"),
}


_NOUN = {"campaign": "Campaign", "adgroup": "Ad group", "ad": "Ad"}


_STATUS_VERB = {"DISABLE": ("pause", "paused"), "ENABLE": ("enable", "enabled"),
                "DELETE": ("delete", "deleted")}


def _status_action(action_id: str) -> Action:
    kind, status = _STATUS_ACTIONS[action_id]
    noun = _NOUN[kind].lower()
    verb = _STATUS_VERB[status][0]
    text = {
        "DISABLE": f"Pause one or more {noun}s so they stop delivering and "
                   f"spending. Reversible with enable.",
        "ENABLE": f"Enable one or more {noun}s so they can deliver. This "
                  f"STARTS SPEND — a deliberate, separately confirmed step.",
        "DELETE": f"PERMANENTLY delete one or more {noun}s (TikTok cannot "
                  f"restore them, and their children stop). Prefer pause "
                  f"unless the user explicitly asks to delete.",
    }[status]
    return Action(action_id, f"{verb.capitalize()} {noun}", text,
                  schema=_ids_schema(f"{kind}_id", f"{kind}_ids"))


_ID_TYPES: Dict[str, str] = {
    "email": "EMAIL_SHA256", "phone": "PHONE_SHA256",
    "idfa": "IDFA_SHA256", "gaid": "GAID_SHA256",
}


_MEMBERS_SCHEMA = {
    "identifier_type": {"type": "string", "enum": sorted(_ID_TYPES),
                        "description": "What the members are: email, phone "
                                       "(with country code, e.g. "
                                       "+14155550100), idfa or gaid."},
    "members": {"type": "array", "items": {"type": "string", "minLength": 1},
                "minItems": 1, "maxItems": _MAX_MEMBERS,
                "description": "The raw identifiers. They are normalised and "
                               "SHA-256 hashed locally; raw values are never "
                               "sent. PII — confirm consent first."},
    "already_hashed": {"type": "boolean",
                       "description": "True if members are already "
                                      "SHA-256 hex of normalised values."},
}


_ACTIONS: List[Action] = [
    # -- campaigns ------------------------------------------------------
    Action(
        "create_campaign", "Create campaign",
        "Create a campaign. It is created PAUSED (DISABLE): nothing serves or "
        "spends until enable_campaign. Budget is in the advertiser's "
        "currency. Use BUDGET_MODE_INFINITE to set budgets per ad group "
        "instead. Reach & Frequency (RF_*) objectives are not supported "
        "because TikTok cannot create them paused.",
        schema=_obj({**_props_schema(_CAMPAIGN_PROPS, _CAMPAIGN_PROPS), **_EXTRA},
                    ("campaign_name", "objective_type", "budget_mode"))),
    Action(
        "update_campaign", "Update campaign",
        "Rename a campaign, change its budget or special ad categories, or "
        "set further TikTok campaign fields via extra.",
        schema=_obj({"campaign_id": _p("id", "campaign_id"),
                     **_props_schema(_CAMPAIGN_PROPS, _CAMPAIGN_UPDATABLE),
                     **_EXTRA}, ("campaign_id",))),
    Action(
        "set_campaign_budget", "Set campaign budget",
        "Set a campaign's daily or lifetime budget (whichever its budget_mode "
        "is), in the advertiser's currency. Affects spend.",
        schema=_obj({"campaign_id": _p("id", "campaign_id"),
                     "budget": _p("amount", "budget")},
                    ("campaign_id", "budget"))),
    _status_action("pause_campaign"),
    _status_action("enable_campaign"),
    _status_action("delete_campaign"),
    # -- ad groups ------------------------------------------------------
    Action(
        "create_ad_group", "Create ad group",
        "Create an ad group (targeting, placement, budget, schedule, "
        "optimisation and bidding) in a campaign. It is created PAUSED. "
        "TikTok requires budget and budget_mode even when the campaign uses "
        "budget optimisation (they are then ignored). Defaults when not "
        "given: automatic placement, runs from 10 minutes from now, "
        "lowest-cost bidding, smooth pacing.",
        schema=_obj({"campaign_id": _p("id", "campaign_id"),
                     **_props_schema(_ADGROUP_PROPS, _ADGROUP_CREATE_KEYS),
                     **_EXTRA},
                    ("campaign_id", "adgroup_name", "location_ids",
                     "optimization_goal", "billing_event", "budget_mode",
                     "budget"))),
    Action(
        "update_ad_group", "Update ad group",
        "Change an ad group's name, targeting, budget, schedule, pacing or "
        "bid. Placement, promotion type, optimisation goal/event, billing "
        "event and pixel are fixed at creation. List params replace the "
        "current list.",
        schema=_obj({"adgroup_id": _p("id", "adgroup_id"),
                     **_props_schema(_ADGROUP_PROPS,
                                     _ADGROUP_SETTERS["update_ad_group"]),
                     **_EXTRA}, ("adgroup_id",))),
    Action(
        "set_ad_group_budget", "Set ad group budget",
        "Set an ad group's daily or lifetime budget, in the advertiser's "
        "currency. Affects spend.",
        schema=_obj({"adgroup_id": _p("id", "adgroup_id"),
                     "budget": _p("amount", "budget")},
                    ("adgroup_id", "budget"))),
    Action(
        "set_ad_group_bid", "Set ad group bid",
        "Change an ad group's bidding: strategy, bid, target cost per "
        "conversion (Cost Cap), deep-event bid or target ROAS. Affects spend.",
        schema=_obj({"adgroup_id": _p("id", "adgroup_id"),
                     **_props_schema(_ADGROUP_PROPS,
                                     _ADGROUP_SETTERS["set_ad_group_bid"])},
                    ("adgroup_id",))),
    Action(
        "set_ad_group_schedule", "Set ad group schedule",
        "Change when an ad group runs: start/end time and dayparting.",
        schema=_obj({"adgroup_id": _p("id", "adgroup_id"),
                     **_props_schema(_ADGROUP_PROPS,
                                     _ADGROUP_SETTERS["set_ad_group_schedule"])},
                    ("adgroup_id",))),
    Action(
        "set_ad_group_targeting", "Set ad group targeting",
        "Change who an ad group reaches: locations, age, gender, languages, "
        "OS, network, interests and frequency cap. Each list "
        "given replaces the current one.",
        schema=_obj({"adgroup_id": _p("id", "adgroup_id"),
                     **_props_schema(_ADGROUP_PROPS,
                                     _ADGROUP_SETTERS["set_ad_group_targeting"])},
                    ("adgroup_id",))),
    Action(
        "set_ad_group_audiences", "Set ad group audiences",
        "Set the custom/lookalike audiences an ad group targets and excludes. "
        "Each list given replaces the current one, so pass the full new list.",
        schema=_obj({"adgroup_id": _p("id", "adgroup_id"),
                     **_props_schema(_ADGROUP_PROPS,
                                     _ADGROUP_SETTERS["set_ad_group_audiences"])},
                    ("adgroup_id",))),
    _status_action("pause_ad_group"),
    _status_action("enable_ad_group"),
    _status_action("delete_ad_group"),
    # -- ads ------------------------------------------------------------
    Action(
        "create_ad", "Create ad",
        "Create an ad in an ad group. SINGLE_VIDEO needs video_id (plus a "
        "cover in image_ids) or, for Spark Ads, tiktok_item_id; SINGLE_IMAGE "
        "and CAROUSEL_ADS need image_ids. The ad is created paused; it "
        "delivers only once enabled and approved by TikTok.",
        schema=_obj({"adgroup_id": _p("id", "adgroup_id"),
                     **_props_schema(_AD_PROPS, _AD_PROPS), **_EXTRA},
                    ("adgroup_id", "ad_name", "identity_type", "identity_id"))),
    Action(
        "update_ad", "Update ad",
        "Change an ad's creative: name, text, call to action, landing page, "
        "video/images, identity, tracking. Unchanged fields are carried over. "
        "An edited ad goes back through TikTok review.",
        schema=_obj({"ad_id": _p("id", "ad_id"),
                     **_props_schema(_AD_PROPS, _AD_PROPS), **_EXTRA},
                    ("ad_id",))),
    _status_action("pause_ad"),
    _status_action("enable_ad"),
    _status_action("delete_ad"),
    # -- creative assets -------------------------------------------------
    Action(
        "upload_image", "Upload image",
        "Add an image to the advertiser's creative library from a public URL "
        "(TikTok fetches it). Returns the image_id for create_ad / "
        "create_identity.",
        schema=_obj({"image_url": _p("url", "image_url", "Public image URL."),
                     "file_name": _p("text", "file_name",
                                     "Name in the library (unique).")},
                    ("image_url",)),
        destructive=False),
    Action(
        "upload_video", "Upload video",
        "Add a video to the advertiser's creative library from a public URL "
        "(TikTok fetches it). Returns the video_id for create_ad.",
        schema=_obj({"video_url": _p("url", "video_url", "Public video URL."),
                     "file_name": _p("text", "file_name",
                                     "Name in the library (unique).")},
                    ("video_url",)),
        destructive=False),
    Action(
        "create_identity", "Create custom identity",
        "Create a CUSTOMIZED_USER identity (the name and avatar shown on "
        "non-Spark ads).",
        schema=_obj({"display_name": _p("text", "display_name",
                                        "Identity name (max 100 chars)."),
                     "image_uri": _p("text", "image_uri",
                                     "Avatar image id, from upload_image.")},
                    ("display_name",)),
        destructive=False),
    Action(
        "authorize_spark_post", "Authorise Spark post",
        "Authorise a creator's TikTok post for Spark Ads using the post's "
        "authorisation code (generated by the creator in the TikTok app). The "
        "post then appears in the SparkPosts report.",
        schema=_obj({"auth_code": _p("text", "auth_code",
                                     "The post's Spark Ads authorisation "
                                     "code.")}, ("auth_code",)),
        destructive=False),
    # -- audiences --------------------------------------------------------
    Action(
        "create_customer_file_audience", "Create customer file audience",
        "Create a custom audience from a customer list (emails, phones or "
        "mobile ad ids). Identifiers are normalised and SHA-256 hashed "
        "locally before upload. This is first-party PII: confirm the user has "
        "consent before calling.",
        schema=_obj({"name": _p("text", "name", "Audience name."),
                     **_MEMBERS_SCHEMA,
                     "retention_in_days": _p("int", "retention_in_days",
                                             "Days a member stays (max 365).")},
                    ("name", "identifier_type", "members"))),
    Action(
        "add_audience_members", "Add audience members",
        "Append people to a customer file audience (hashed upload, PII — "
        "confirm consent). Members must be the same identifier type the "
        "audience was built from; TikTok refuses a change that leaves the "
        "audience under 1,000 people.",
        schema=_obj({"custom_audience_id": _p("id", "custom_audience_id",
                                              "Audience id (Audiences report)."),
                     **_MEMBERS_SCHEMA},
                    ("custom_audience_id", "identifier_type", "members"))),
    Action(
        "remove_audience_members", "Remove audience members",
        "Remove people from a customer file audience (hashed upload). TikTok "
        "refuses a change that leaves the audience under 1,000 people.",
        schema=_obj({"custom_audience_id": _p("id", "custom_audience_id",
                                              "Audience id (Audiences report)."),
                     **_MEMBERS_SCHEMA},
                    ("custom_audience_id", "identifier_type", "members"))),
    Action(
        "rename_audience", "Rename audience",
        "Rename a custom or lookalike audience.",
        schema=_obj({"custom_audience_id": _p("id", "custom_audience_id",
                                              "Audience id (Audiences report)."),
                     "name": _p("text", "name", "New name.")},
                    ("custom_audience_id", "name")),
        destructive=False),
    Action(
        "create_lookalike_audience", "Create lookalike audience",
        "Create a lookalike of an existing audience in the given locations.",
        schema=_obj({
            "name": _p("text", "name", "Audience name."),
            "source_audience_id": _p("id", "source_audience_id",
                                     "Seed audience id (Audiences report); "
                                     "at least 100 people and not itself a "
                                     "lookalike."),
            "location_ids": _p("ids", "location_ids"),
            "audience_size": {"type": "string",
                              "enum": ["NARROW", "BALANCED", "BROAD"],
                              "description": "Lookalike size."},
            "mobile_os": {"type": "string", "enum": list(_LOOKALIKE_OS),
                          "description": "Device OS: ALL, ANDROID or IOS."},
            "placements": {"type": "array", "minItems": 1,
                           "items": {"type": "string",
                                     "enum": list(_LOOKALIKE_PLACEMENTS)},
                           "description": "Apps to deliver on."},
            "include_source": _p("bool", "include_source",
                                 "Include the seed audience itself."),
            **_EXTRA},
            ("name", "source_audience_id", "location_ids", "audience_size",
             "mobile_os", "placements", "include_source")),
        destructive=False),
    Action(
        "create_rule_audience", "Create rule-based audience",
        "Create an engagement, app-activity, pixel/website-traffic or lead "
        "audience from a TikTok rule_spec (inclusion/exclusion rules over "
        "events and look-back windows).",
        schema=_obj({
            "name": _p("text", "name", "Audience name."),
            "audience_type": _p("token", "audience_type",
                                "ENGAGEMENT, APP, PIXEL (website traffic), "
                                "LEAD_GENERATION, BUSINESS_ACCOUNT, …"),
            "rule_spec": _p("object", "rule_spec",
                            "TikTok rule_spec: {inclusion_rule_set: {operator, "
                            "rules: [{event_source_ids, retention_days, "
                            "filter_set}]}, exclusion_rule_set?}."),
            **_EXTRA},
            ("name", "audience_type", "rule_spec")),
        destructive=False),
    Action(
        "delete_audience", "Delete audience",
        "PERMANENTLY delete one or more custom/lookalike audiences. Ad groups "
        "targeting them lose that targeting.",
        schema=_ids_schema("custom_audience_id", "custom_audience_ids")),
    Action(
        "share_audience", "Share audience",
        "Share audiences with other advertiser accounts (in the same "
        "Business Center).",
        schema=_obj({"custom_audience_ids": _p("ids", "custom_audience_ids",
                                               "Audience ids to share."),
                     "shared_advertiser_ids": _p("ids", "shared_advertiser_ids",
                                                 "Advertiser ids to share "
                                                 "with.")},
                    ("custom_audience_ids", "shared_advertiser_ids"))),
    # -- comments ---------------------------------------------------------
    Action(
        "reply_to_comment", "Reply to comment",
        "Publicly reply to a comment on an ad as the ad's identity. Take "
        "comment_id, ad_id and tiktok_item_id from the Comments report and "
        "identity_id/identity_type from the Ads report.",
        schema=_obj({
            "comment_id": _p("id", "comment_id", "Comment id (Comments report)."),
            "ad_id": _p("id", "ad_id"),
            "tiktok_item_id": _p("text", "tiktok_item_id",
                                 "The ad's post id (Comments report)."),
            "text": _p("text", "text", "Reply text."),
            "identity_id": _p("text", "identity_id"),
            "identity_type": {"type": "string",
                              "enum": list(_COMMENT_IDENTITY_TYPES),
                              "description": "The ad's identity type."}},
            ("comment_id", "ad_id", "tiktok_item_id", "text", "identity_id",
             "identity_type"))),
    Action(
        "hide_comment", "Hide comment",
        "Hide comments on ads from everyone but their author. Reversible with "
        "unhide_comment.",
        schema=_ids_schema("comment_id", "comment_ids"), destructive=False),
    Action(
        "unhide_comment", "Unhide comment",
        "Make hidden comments public again.",
        schema=_ids_schema("comment_id", "comment_ids"), destructive=False),
    Action(
        "delete_comment", "Delete comment",
        "PERMANENTLY delete a comment (only replies posted by the ad's own "
        "identity can be deleted). Prefer hide_comment.",
        schema=_obj({"comment_id": _p("id", "comment_id"),
                     "ad_id": _p("id", "ad_id"),
                     "tiktok_item_id": _p("text", "tiktok_item_id",
                                          "The ad's post id (Comments report)."),
                     "identity_id": _p("text", "identity_id"),
                     "identity_type": _p("token", "identity_type")},
                    ("comment_id", "ad_id", "tiktok_item_id", "identity_id",
                     "identity_type"))),  # identity per comment/delete/
    Action(
        "add_blocked_words", "Add blocked words",
        "Auto-hide future comments containing these words on the "
        "advertiser's ads.",
        schema=_obj({"words": _p("texts", "words", "Words to block.")},
                    ("words",)),
        destructive=False),
    Action(
        "remove_blocked_words", "Remove blocked words",
        "Stop auto-hiding comments containing these words.",
        schema=_obj({"words": _p("texts", "words", "Words to unblock.")},
                    ("words",)),
        destructive=False),
    # -- automated rules -------------------------------------------------
    Action(
        "create_automated_rule", "Create automated rule",
        "Create an automated rule that watches campaigns/ad groups/ads and "
        "acts on them (pause, change budget or bid, notify). `rule` is "
        "TikTok's rule object and must include name, apply_objects, "
        "conditions, actions, notification and rule_exec_info (tzone is "
        "optional). A rule can change spend on its own — confirm what it "
        "does with the user.",
        schema=_obj({"rule": _p("object", "rule", "TikTok rule object with "
                                                  "name, apply_objects, "
                                                  "conditions, actions, "
                                                  "notification, "
                                                  "rule_exec_info.")},
                    ("rule",))),
    Action(
        "update_automated_rule", "Update automated rule",
        "Replace an automated rule's definition. TikTok requires the full "
        "rule (name, apply_objects, conditions, actions, notification, "
        "rule_exec_info) on every update.",
        schema=_obj({"rule_id": _p("id", "rule_id",
                                   "Rule id (AutomatedRules report)."),
                     "rule": _p("object", "rule", "Fields to set.")},
                    ("rule_id", "rule"))),
    Action(
        "enable_automated_rule", "Turn on automated rule",
        "Turn automated rules on — they start acting on campaigns.",
        schema=_ids_schema("rule_id", "rule_ids")),
    Action(
        "disable_automated_rule", "Turn off automated rule",
        "Turn automated rules off.",
        schema=_ids_schema("rule_id", "rule_ids"), destructive=False),
    Action(
        "delete_automated_rule", "Delete automated rule",
        "PERMANENTLY delete automated rules.",
        schema=_ids_schema("rule_id", "rule_ids")),
    # -- pixels -------------------------------------------------------------
    Action(
        "create_pixel", "Create pixel",
        "Create a TikTok Pixel for website event tracking. Returns the pixel "
        "id and the code to install on the site.",
        schema=_obj({"pixel_name": _p("text", "pixel_name", "Pixel name."),
                     **_EXTRA}, ("pixel_name",)),
        destructive=False),
    Action(
        "update_pixel", "Update pixel",
        "Rename a pixel or change its settings (via extra, e.g. "
        "advanced_matching_fields). TikTok requires pixel_name on every "
        "update — pass the current name to keep it.",
        schema=_obj({"pixel_id": _p("id", "pixel_id"),
                     "pixel_name": _p("text", "pixel_name", "Pixel name "
                                      "(required by TikTok)."),
                     **_EXTRA}, ("pixel_id", "pixel_name")),
        destructive=False),
]


_ACTIONS_BY_ID: Dict[str, Action] = {a.id: a for a in _ACTIONS}


# Action -> handler method (status actions share `_set_status`).
_HANDLERS: Dict[str, str] = {
    "create_campaign": "_create_campaign",
    "update_campaign": "_update_campaign",
    "set_campaign_budget": "_set_campaign_budget",
    "create_ad_group": "_create_ad_group",
    "update_ad_group": "_set_ad_group",
    "set_ad_group_bid": "_set_ad_group",
    "set_ad_group_schedule": "_set_ad_group",
    "set_ad_group_targeting": "_set_ad_group",
    "set_ad_group_audiences": "_set_ad_group",
    "set_ad_group_budget": "_set_ad_group_budget",
    "create_ad": "_create_ad",
    "update_ad": "_update_ad",
    "upload_image": "_upload_image",
    "upload_video": "_upload_video",
    "create_identity": "_create_identity",
    "authorize_spark_post": "_authorize_spark_post",
    "create_customer_file_audience": "_create_customer_file_audience",
    "add_audience_members": "_change_audience_members",
    "remove_audience_members": "_change_audience_members",
    "rename_audience": "_rename_audience",
    "create_lookalike_audience": "_create_lookalike_audience",
    "create_rule_audience": "_create_rule_audience",
    "delete_audience": "_delete_audience",
    "share_audience": "_share_audience",
    "reply_to_comment": "_reply_to_comment",
    "hide_comment": "_set_comment_visibility",
    "unhide_comment": "_set_comment_visibility",
    "delete_comment": "_delete_comment",
    "add_blocked_words": "_blocked_words",
    "remove_blocked_words": "_blocked_words",
    "create_automated_rule": "_create_rule",
    "update_automated_rule": "_update_rule",
    "enable_automated_rule": "_set_rule_status",
    "disable_automated_rule": "_set_rule_status",
    "delete_automated_rule": "_set_rule_status",
    "create_pixel": "_create_pixel",
    "update_pixel": "_update_pixel",
}


assert set(_HANDLERS) | set(_STATUS_ACTIONS) == set(_ACTIONS_BY_ID)


# ad/update/ replaces the whole creative, so unchanged fields are resent.
_AD_CARRY = tuple(_AD_PROPS) + ("avatar_icon_web_uri", "creative_authorized")


def _bad(message: str, **details) -> ApiError:
    return ApiError(ErrorCode.INVALID_ACTION_PARAMS, message, retriable=False,
                    details=details)


def _norm_id(value: Any, key: str) -> str:
    if isinstance(value, int) and not isinstance(value, bool):
        value = str(value)
    if not isinstance(value, str) or not _ID_RE.match(value.strip()):
        raise _bad(f"'{key}' must be a numeric TikTok id, e.g. "
                   f"'1780123456789012'.", param=key, value=value)
    return value.strip()


def _coerce(key: str, value: Any, kind: str) -> Any:
    """Check one parameter and convert it to what TikTok expects."""
    if kind == "id":
        return _norm_id(value, key)
    if kind == "ids":
        if not isinstance(value, list) or not value:
            raise _bad(f"'{key}' must be a non-empty list of ids.", param=key)
        return list(dict.fromkeys(_norm_id(v, key) for v in value))
    if kind == "text":
        if not isinstance(value, str) or not value.strip():
            raise _bad(f"'{key}' must be a non-empty string.", param=key)
        return value.strip()
    if kind == "texts":
        if (not isinstance(value, list) or not value
                or not all(isinstance(v, str) and v.strip() for v in value)):
            raise _bad(f"'{key}' must be a non-empty list of strings.",
                       param=key)
        return list(dict.fromkeys(v.strip() for v in value))
    if kind == "token":
        if not isinstance(value, str) or not _TOKEN_RE.match(value.strip().upper()):
            raise _bad(f"'{key}' must be a TikTok enum value such as "
                       f"'BUDGET_MODE_DAY'.", param=key, value=value)
        token = value.strip().upper()
        allowed = _ENUMS.get(key)
        if allowed and token not in allowed:
            raise _bad(f"'{key}' must be one of TikTok's values: "
                       f"{', '.join(allowed)}.", param=key, value=value)
        return token
    if kind == "tokens":
        if not isinstance(value, list) or not value:
            raise _bad(f"'{key}' must be a non-empty list of TikTok enum "
                       f"values.", param=key)
        return list(dict.fromkeys(_coerce(key, v, "token") for v in value))
    if kind == "amount":
        if isinstance(value, bool) or not isinstance(value, (int, float)) \
                or value <= 0:
            raise _bad(f"'{key}' must be a positive amount in the "
                       f"advertiser's currency, e.g. 50 for 50.00.",
                       param=key, value=value)
        return float(value)
    if kind == "int":
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            raise _bad(f"'{key}' must be a positive whole number.", param=key)
        return value
    if kind == "bool":
        if not isinstance(value, bool):
            raise _bad(f"'{key}' must be true or false.", param=key)
        return value
    if kind == "datetime":
        if not isinstance(value, str) or not _DT_RE.match(value.strip()):
            raise _bad(f"'{key}' must be 'YYYY-MM-DD HH:MM:SS' (UTC).",
                       param=key, value=value)
        return value.strip()
    if kind == "url":
        if not isinstance(value, str) or not re.match(r"^https?://\S+$",
                                                      value.strip()):
            raise _bad(f"'{key}' must be an http(s) URL.", param=key)
        return value.strip()
    if kind == "object":
        if not isinstance(value, dict):
            raise _bad(f"'{key}' must be an object.", param=key)
        return value
    raise AssertionError(kind)   # pragma: no cover


def _pick(params: Dict[str, Any], props: Dict[str, str], keys) -> Dict[str, Any]:
    """Checked values for the `keys` present in `params`."""
    return {k: _coerce(k, params[k], props[k])
            for k in keys if params.get(k) is not None}


def _extra(params: Dict[str, Any]) -> Dict[str, Any]:
    extra = params.get("extra")
    if extra is None:
        return {}
    extra = _coerce("extra", extra, "object")
    blocked = sorted(_PROTECTED.intersection(extra))
    if blocked:
        raise _bad(f"'extra' cannot set {', '.join(blocked)}; use the "
                   f"action's own params or a status action.", keys=blocked)
    return dict(extra)


def _ids(params: Dict[str, Any], single: str, plural: str) -> List[str]:
    if params.get(single) is not None and params.get(plural) is not None:
        raise _bad(f"Give '{single}' or '{plural}', not both.")
    if params.get(single) is not None:
        return [_norm_id(params[single], single)]
    if params.get(plural) is None:
        raise _bad(f"'{single}' (or '{plural}') is required.", param=single)
    ids = _coerce(plural, params[plural], "ids")
    if len(ids) > _MAX_BATCH:
        raise _bad(f"At most {_MAX_BATCH} ids per call; got {len(ids)}.",
                   param=plural)
    return ids


def _required(params: Dict[str, Any], *keys: str) -> None:
    missing = [k for k in keys if params.get(k) is None]
    if missing:
        raise _bad(f"Missing required param(s): {', '.join(missing)}.",
                   missing=missing)


def _check_known(action: Action, params: Dict[str, Any]) -> None:
    allowed = set((action.schema or {}).get("properties") or {})
    unknown = sorted(set(params) - allowed)
    if unknown:
        raise _bad(f"Unknown param(s) for {action.id}: {', '.join(unknown)}. "
                   f"Accepted: {', '.join(sorted(allowed))}.",
                   unknown=unknown, accepted=sorted(allowed))


def _normalise_member(value: str, id_type: str) -> str:
    """Normalise an identifier the way TikTok expects before hashing."""
    raw = value.strip()
    if id_type == "email":
        email = raw.lower()
        if "@" not in email or " " in email:
            raise _bad(f"Not an email address: {value!r}.")
        return email
    if id_type == "phone":
        digits = re.sub(r"[\s\-().]", "", raw)
        if digits.startswith("00"):
            digits = "+" + digits[2:]
        if not re.match(r"^\+\d{8,15}$", digits):
            raise _bad(f"Phone numbers need a country code, e.g. "
                       f"+14155550100: {value!r}.")
        return digits
    if id_type == "idfa":
        return raw.upper()
    return raw.lower()   # gaid


def _hash_members(params: Dict[str, Any]) -> Tuple[str, List[str]]:
    """Return (calculate_type, unique SHA-256 hashes) for audience members."""
    id_type = params.get("identifier_type")
    if id_type not in _ID_TYPES:
        raise _bad(f"'identifier_type' must be one of "
                   f"{', '.join(sorted(_ID_TYPES))}.", param="identifier_type")
    members = params.get("members")
    if (not isinstance(members, list) or not members
            or not all(isinstance(m, str) and m.strip() for m in members)):
        raise _bad("'members' must be a non-empty list of strings.",
                   param="members")
    if len(members) > _MAX_MEMBERS:
        raise _bad(f"At most {_MAX_MEMBERS} members per call.", param="members")
    if params.get("already_hashed"):
        hashes = [m.strip().lower() for m in members]
        bad = next((m for m in hashes if not _HEX64_RE.match(m)), None)
        if bad is not None:
            raise _bad("already_hashed members must be 64-character SHA-256 "
                       "hex strings.", param="members")
    else:
        hashes = [hashlib.sha256(_normalise_member(m, id_type).encode("utf-8"))
                  .hexdigest() for m in members]
    return _ID_TYPES[id_type], list(dict.fromkeys(hashes))


def _check_rule(rule: Dict[str, Any]) -> None:
    """Require the keys TikTok needs on every automated rule."""
    missing = [k for k in _RULE_REQUIRED if rule.get(k) in (None, "", [], {})]
    if missing:
        raise _bad(f"The rule is missing required field(s): "
                   f"{', '.join(missing)}. A TikTok rule needs "
                   f"{', '.join(_RULE_REQUIRED)}.", missing=missing)
    if not isinstance(rule["name"], str) or not rule["name"].strip():
        raise _bad("The rule's 'name' must be a non-empty string.",
                   param="rule.name")


_EMOJI_RE = re.compile("[\U0001F000-\U0001FAFF\u2600-\u27BF\uFE0F]")


def _check_ad_texts(creative: Dict[str, Any]) -> None:
    """Ad text: at most 100 characters, no emoji. Display name: at most 40."""
    text = creative.get("ad_text")
    if isinstance(text, str):
        if len(text) > 100:
            raise _bad("'ad_text' is at most 100 characters.", param="ad_text")
        if _EMOJI_RE.search(text):
            raise _bad("'ad_text' cannot contain emoji.", param="ad_text")
    name = creative.get("display_name")
    if isinstance(name, str) and len(name) > 40:
        raise _bad("An ad's 'display_name' is 1–40 characters.",
                   param="display_name")


def _would(action: str, account: str, summary: str, *, before=None,
           request=None) -> ActionResult:
    """Result of a dry run."""
    details: Dict[str, Any] = {"dry_run": True, "applied": False}
    if request is not None:
        details["request"] = request
    return ActionResult(action=action, account=account,
                        summary=f"[dry run] {summary}", before=before,
                        details=details)


def _snap(obj: Dict[str, Any], keys) -> Dict[str, Any]:
    return {k: obj.get(k) for k in keys if k in obj}


_CAMPAIGN_SNAP = ("campaign_id", "campaign_name", "operation_status",
                  "secondary_status", "objective_type", "budget_mode", "budget",
                  "budget_optimize_on", "special_industries")


_ADGROUP_SNAP = ("adgroup_id", "adgroup_name", "campaign_id",
                 "operation_status", "secondary_status", *_ADGROUP_PROPS)


_AD_SNAP = ("ad_id", "adgroup_id", "campaign_id", "operation_status",
            "secondary_status", *_AD_PROPS)


_SNAP_KEYS = {"campaign": _CAMPAIGN_SNAP, "adgroup": _ADGROUP_SNAP,
              "ad": _AD_SNAP}


def _one_or_many(ids: List[str], per_id: Dict[str, Any]) -> Dict[str, Any]:
    """One entity as-is; several keyed by id."""
    return per_id[ids[0]] if len(ids) == 1 else dict(per_id)


def _first_id(data: Any, *keys: str) -> Optional[str]:
    """Id of the created object (responses are a dict or a list)."""
    if isinstance(data, list):
        data = data[0] if data else {}
    if not isinstance(data, dict):
        return None
    for key in keys:
        value = data.get(key)
        if isinstance(value, list):
            value = value[0] if value else None
        if value not in (None, ""):
            return str(value)
    return None


def _label(ids: List[str], noun: str, names: Dict[str, Any]) -> str:
    if len(ids) == 1:
        name = names.get(ids[0])
        return f"{noun.lower()} {name!r} ({ids[0]})" if name else \
            f"{noun.lower()} {ids[0]}"
    return f"{len(ids)} {noun.lower()}s"


def _utc_soon() -> str:
    """Start time a few minutes from now (UTC); TikTok rejects past times."""
    return (datetime.now(timezone.utc) + timedelta(minutes=10)) \
        .strftime("%Y-%m-%d %H:%M:%S")
