"""GMV Max report levels.

From the API reference pages "Run a GMV Max Campaign report" and "Metrics in
GMV Max Campaign reports". Each report type here is one metric level there,
with its allowed groupings (`report_rules` patterns), filters and metrics.
"""

from dataclasses import dataclass
from typing import Dict, Optional, Tuple

ENDPOINT = "gmv_max/report/get/"

MAX_IDS = 100            # campaign_ids / item_group_ids / room_ids
PAGE_SIZE = 1000         # page_size 1-1,000
# Max days per request: no time column / daily / hourly.
SPAN_TOTAL, SPAN_DAY, SPAN_HOUR = 365, 30, 1

DIMENSIONS = {
    "advertiser_id": "Advertiser ID", "campaign_id": "Campaign ID",
    "item_group_id": "Product SPU ID", "item_id": "TikTok post ID",
    "room_id": "Livestream room ID", "duration": "Duration",
}
# Attributes are only returned when grouping by one of these.
ID_DIMENSIONS = ("advertiser_id", "campaign_id", "item_group_id", "item_id",
                 "room_id")

LABELS: Dict[str, str] = {
    # attributes
    "operation_status": "Campaign on/off status",
    "campaign_name": "Campaign name", "schedule_type": "Schedule type",
    "schedule_start_time": "Schedule start time",
    "schedule_end_time": "Schedule end time",
    "target_roi_budget": "Target ROI budget", "bid_type": "Optimization mode",
    "max_delivery_budget": "Max delivery budget",
    "product_name": "Product name", "product_image_url": "Product image URL",
    "title": "Creative name", "tt_account_name": "TikTok account name",
    "tt_account_profile_image_url": "TikTok account profile image URL",
    "tt_account_authorization_type": "Authorization type",
    "shop_content_type": "Shop content type",
    "identity_id": "Identity ID (TikTok account ID)",
    "live_name": "LIVE name", "live_status": "Livestream status",
    "live_launched_time": "Livestream launched time",
    "live_duration": "Livestream duration",
    # string-valued delivery metrics
    "product_status": "Product status", "creative_delivery_status": "Status",
    # delivery metrics
    "cost": "Cost", "net_cost": "Net cost", "orders": "Orders (SKU)",
    "cost_per_order": "Cost per order", "gross_revenue": "Gross revenue",
    "roi": "ROI", "roas_bid": "Target ROI",
    "product_impressions": "Product ad impressions",
    "product_clicks": "Product ad clicks",
    "product_click_rate": "Product ad click rate",
    "ad_click_rate": "Ad click rate",
    "ad_conversion_rate": "Ad conversion rate",
    "ad_video_view_rate_2s": "2-second ad video view rate",
    "ad_video_view_rate_6s": "6-second ad video view rate",
    "ad_video_view_rate_p25": "25% ad video view rate",
    "ad_video_view_rate_p50": "50% ad video view rate",
    "ad_video_view_rate_p75": "75% ad video view rate",
    "ad_video_view_rate_p100": "100% ad video view rate",
    "all_shops_cost_per_order": "Cost per order (all shops)",
    "all_shops_orders": "Orders (all shops)",
    "all_shops_gross_revenue": "Gross revenue (all shops)",
    "all_shops_roi": "ROI (all shops)",
    "live_views": "LIVE views", "cost_per_live_view": "Cost per LIVE view",
    "10_second_live_views": "10-second LIVE views",
    "cost_per_10_second_live_view": "Cost per 10-second LIVE view",
    "live_follows": "LIVE follows",
}
# Metrics with text values, offered as columns.
TEXT_METRICS = frozenset({"product_status", "creative_delivery_status"})

# Filter setting -> (kind, max items, allowed values). Lists are
# comma-separated in settings.
FILTERS: Dict[str, Tuple[str, Optional[int], Tuple[str, ...]]] = {
    "gmv_max_promotion_types": ("enums", None, ("PRODUCT", "LIVE")),
    "campaign_ids": ("ids", MAX_IDS, ()),
    "campaign_name": ("str", None, ()),
    "campaign_statuses": ("enums", None, (
        "STATUS_DELIVERY_OK", "STATUS_DISABLE", "STATUS_DELETE",
        "STATUS_NOT_DELIVERY", "STATUS_ALL")),
    "item_group_ids": ("ids", MAX_IDS, ()),
    # Being deprecated by TikTok.
    "creative_types": ("enums", 1, ("ADS_AND_ORGANIC", "ORGANIC",
                                    "REMOVED")),
    # "NOT_DELIVERYING" is spelled that way by TikTok.
    "creative_delivery_statuses": ("enums", None, (
        "IN_QUEUE", "LEARNING", "DELIVERING", "NOT_DELIVERYING",
        "AUTHORIZATION_NEEDED", "EXCLUDED", "UNAVAILABLE", "REJECTED",
        "NOT_ACTIVE")),
    "search_word": ("str", None, ()),
    "room_ids": ("ids", MAX_IDS, ()),
}

PRODUCT_SORT = ("cost", "net_cost", "orders", "cost_per_order",
                "gross_revenue", "roi", "product_impressions",
                "product_clicks", "product_click_rate", "ad_click_rate",
                "ad_conversion_rate", "ad_video_view_rate_2s",
                "ad_video_view_rate_6s", "ad_video_view_rate_p25",
                "ad_video_view_rate_p50", "ad_video_view_rate_p75",
                "ad_video_view_rate_p100")
LIVE_SORT = ("cost", "net_cost", "orders", "cost_per_order", "gross_revenue",
             "roi", "live_views", "cost_per_live_view",
             "10_second_live_views", "cost_per_10_second_live_view",
             "live_follows")


@dataclass(frozen=True)
class GmvReport:
    label: str
    promotion: Optional[str]          # fixed gmv_max_promotion_types value
    groupings: Tuple[str, ...]        # allowed `dimensions` patterns
    attributes: Tuple[str, ...]       # attribute metrics of this level
    metrics: Tuple[str, ...]          # delivery metrics of this level
    required: Tuple[str, ...] = ()    # required filters
    optional: Tuple[str, ...] = ()    # optional filters
    sort_fields: Tuple[str, ...] = ()
    campaign_level: bool = False      # attributes always allowed


_CORE = ("cost", "orders", "cost_per_order", "gross_revenue", "roi")
_CREATIVE_ATTRS = ("title", "item_id", "tt_account_name",
                   "tt_account_profile_image_url",
                   "tt_account_authorization_type", "shop_content_type")
_AD_RATES = ("product_impressions", "product_clicks", "product_click_rate",
             "ad_click_rate", "ad_conversion_rate", "ad_video_view_rate_2s",
             "ad_video_view_rate_6s", "ad_video_view_rate_p25",
             "ad_video_view_rate_p50", "ad_video_view_rate_p75",
             "ad_video_view_rate_p100")
_ALL_SHOPS = ("all_shops_cost_per_order", "all_shops_orders",
              "all_shops_gross_revenue", "all_shops_roi")
_LIVE = ("live_views", "cost_per_live_view", "10_second_live_views",
         "cost_per_10_second_live_view", "live_follows")
_CAMPAIGN_FILTERS = ("campaign_ids", "campaign_name", "campaign_statuses")

REPORTS: Dict[str, GmvReport] = {
    "GmvMaxOverview": GmvReport(
        "GMV Max overview (all GMV Max campaigns)", None,
        ("advertiser_id TIME?",), (), _CORE + ("net_cost",),
        optional=("gmv_max_promotion_types",)),
    "GmvMaxProductCampaigns": GmvReport(
        "Product GMV Max — campaigns", "PRODUCT", ("campaign_id TIME?",),
        ("campaign_id", "operation_status", "campaign_name", "schedule_type",
         "schedule_start_time", "schedule_end_time", "target_roi_budget",
         "bid_type", "max_delivery_budget"),
        ("roas_bid", "cost", "net_cost", "orders", "cost_per_order",
         "gross_revenue", "roi"),
        optional=_CAMPAIGN_FILTERS, sort_fields=PRODUCT_SORT,
        campaign_level=True),
    "GmvMaxProducts": GmvReport(
        "Product GMV Max — products", None,
        ("item_group_id campaign_id? TIME?",),
        ("product_name", "item_group_id", "product_image_url", "bid_type"),
        ("product_status",) + _CORE,
        required=("campaign_ids",), sort_fields=PRODUCT_SORT),
    "GmvMaxCreatives": GmvReport(
        "Product GMV Max — creatives", None,
        # The docs list item_id with stat_time_day, but their own example
        # uses item_id alone, so both are allowed.
        ("campaign_id item_group_id item_id DAY?", "item_id DAY?"),
        _CREATIVE_ATTRS, ("creative_delivery_status",) + _CORE + _AD_RATES,
        required=("campaign_ids", "item_group_ids"),
        optional=("creative_types", "creative_delivery_statuses",
                  "search_word"), sort_fields=PRODUCT_SORT),
    "GmvMaxProductDurations": GmvReport(
        "Product GMV Max — optimization-mode durations", None,
        ("duration campaign_id? item_group_id?",), ("bid_type",), _CORE,
        required=("campaign_ids", "item_group_ids"),
        sort_fields=PRODUCT_SORT),
    "GmvMaxLiveCampaigns": GmvReport(
        "LIVE GMV Max — campaigns", "LIVE", ("campaign_id TIME?",),
        ("campaign_id", "operation_status", "campaign_name",
         "tt_account_name", "tt_account_profile_image_url", "identity_id",
         "bid_type", "schedule_type", "schedule_start_time",
         "schedule_end_time", "target_roi_budget", "max_delivery_budget"),
        ("roas_bid", "cost", "net_cost") + _CORE[1:] + _ALL_SHOPS + _LIVE,
        optional=_CAMPAIGN_FILTERS, sort_fields=LIVE_SORT,
        campaign_level=True),
    "GmvMaxLivestreams": GmvReport(
        "LIVE GMV Max — livestreams", None,
        ("room_id campaign_id? TIME?",),
        ("live_name", "live_status", "live_launched_time", "live_duration"),
        ("cost", "net_cost") + _CORE[1:] + _ALL_SHOPS + _LIVE,
        required=("campaign_ids",), sort_fields=LIVE_SORT),
    "GmvMaxLiveDurations": GmvReport(
        "LIVE GMV Max — optimization-mode durations", None,
        ("duration campaign_id? room_id?",), ("bid_type",),
        _CORE + _ALL_SHOPS, required=("campaign_ids", "room_ids"),
        sort_fields=LIVE_SORT),
}
# LIVE reports also have all-shop totals, so the plain metrics are
# labelled "(current shop)".
LIVE_REPORTS = frozenset({"GmvMaxLiveCampaigns", "GmvMaxLivestreams",
                          "GmvMaxLiveDurations"})
