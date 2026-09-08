"""Sync declared connectors into `ConnectorCatalog`.

`declarations.py` is the source of truth; the catalog stores deployment-specific
settings such as `enabled`, ordering, and display overrides.

Refreshes preserve all database-owned settings. Connectors removed from code
are disabled rather than deleted because existing `DataSource` rows may still
reference them.
"""

import logging
from typing import Dict
from django.db import transaction
from terno_dbi.catalog.declarations import DECLARED_CONNECTORS, declared_keys

logger = logging.getLogger(__name__)


@transaction.atomic
def refresh_catalog() -> Dict[str, int]:
    """Upsert every declared connector. Idempotent.

    Runs post-migrate and at startup. Returns counts for logging and tests.
    """
    from terno_dbi.core.models import ConnectorCatalog

    created = updated = 0

    for spec in DECLARED_CONNECTORS:
        row = ConnectorCatalog.objects.filter(key=spec.key).first()
        if row is None:
            ConnectorCatalog.objects.create(
                key=spec.key,
                enabled=spec.default_enabled,
                **spec.code_owned_fields(),
            )
            created += 1
            continue

        fields = spec.code_owned_fields()
        changed = [
            name for name, value in fields.items()
            if getattr(row, name) != value
        ]
        if changed:
            for name, value in fields.items():
                setattr(row, name, value)
            row.save(update_fields=changed)
            updated += 1

    # Anything no longer declared is disabled, not removed. It stays visible in
    # admin and any DataSource pointing at it keeps resolving.
    retired = (
        ConnectorCatalog.objects
        .exclude(key__in=declared_keys())
        .filter(enabled=True)
        .update(enabled=False)
    )

    if created or updated or retired:
        logger.info(
            "Connector catalog refreshed: %d created, %d updated, %d retired",
            created, updated, retired,
        )

    return {"created": created, "updated": updated, "retired": retired}


__all__ = ["refresh_catalog"]
