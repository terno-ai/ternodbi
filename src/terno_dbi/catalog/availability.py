"""Which connectors a deployment offers.

A catalog row's `enabled` flag is the database-owned switch. On top of it, a
deployment can withhold connectors through configuration — for example, offer
a connector on a staging server while production hides it until the provider
approves the app — without touching the database or the code:

    TERNO_DISABLED_CONNECTORS=hubspot,meta_ads

Read from the Django setting of that name, falling back to the environment
variable; a comma-separated string or a list of connector keys. A withheld
connector is not offered anywhere — the connectors list, the agent's catalogue,
the MCP tools — and cannot be connected. Sources an organisation connected
before keep working.
"""

import logging
import os
from functools import lru_cache
from typing import FrozenSet, Iterable

from django.conf import settings

logger = logging.getLogger(__name__)

SETTING = "TERNO_DISABLED_CONNECTORS"


def disabled_connector_keys() -> FrozenSet[str]:
    """Connector keys this deployment withholds."""
    raw = getattr(settings, SETTING, None)
    if raw is None:
        raw = os.environ.get(SETTING, "")
    if isinstance(raw, str):
        raw = raw.split(",")
    return _parse(tuple(raw))


@lru_cache(maxsize=8)
def _parse(raw: Iterable[str]) -> FrozenSet[str]:
    from terno_dbi.catalog.declarations import declared_keys

    keys = frozenset(k.strip() for k in raw if k and k.strip())
    unknown = keys - set(declared_keys())
    if unknown:
        # A typo here silently offers the connector it meant to hide.
        logger.warning("%s names unknown connectors: %s", SETTING, ", ".join(sorted(unknown)))
    return keys


def offered_connectors():
    """Catalog rows this deployment offers: enabled, and not withheld."""
    from terno_dbi.core.models import ConnectorCatalog

    return ConnectorCatalog.objects.filter(enabled=True).exclude(
        key__in=disabled_connector_keys()
    )
