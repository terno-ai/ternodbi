"""Sources layer: the concrete provider connectors.

One module per platform (ga4, and meta / google_ads / youtube as they land),
each an `ApiConnector` registered with the dispatch registry. Depends on
`model`, `pipeline` and `auth`.
"""

from terno_dbi.connectors.api.sources.ga4 import GA4Connector, make_ga4_connector

__all__ = ["GA4Connector", "make_ga4_connector"]
