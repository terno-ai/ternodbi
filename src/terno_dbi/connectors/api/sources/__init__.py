"""Sources layer: the concrete provider connectors.

One module per platform (ga4, and meta / google_ads / youtube as they land),
each an `ApiConnector` registered with the dispatch registry. Depends on
`model`, `pipeline` and `auth`.
"""

from terno_dbi.connectors.api.sources.ga4 import GA4Connector, make_ga4_connector
from terno_dbi.connectors.api.sources.gsc import GSCConnector, make_gsc_connector
from terno_dbi.connectors.api.sources.google_ads import (
    GoogleAdsConnector, make_google_ads_connector,
)
from terno_dbi.connectors.api.sources.meta_ads import (
    MetaAdsConnector, make_meta_ads_connector,
)
from terno_dbi.connectors.api.sources.youtube import (
    YouTubeConnector, make_youtube_connector,
)

__all__ = [
    "GA4Connector", "make_ga4_connector",
    "GSCConnector", "make_gsc_connector",
    "GoogleAdsConnector", "make_google_ads_connector",
    "MetaAdsConnector", "make_meta_ads_connector",
    "YouTubeConnector", "make_youtube_connector",
]
