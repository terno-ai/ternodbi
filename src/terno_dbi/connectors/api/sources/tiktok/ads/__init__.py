"""TikTok Ads connector.

`connector` holds the class; reports, write actions and Smart+ actions live in
`reports`, `writes` and `smart_plus`. The `*_spec` and `report_rules` modules
are data copied from TikTok's API reference.
"""

from terno_dbi.connectors.api.sources.tiktok.ads.connector import (
    TikTokAdsConnector, make_tiktok_ads_connector,
)

__all__ = ["TikTokAdsConnector", "make_tiktok_ads_connector"]
