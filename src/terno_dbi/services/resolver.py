"""
This module provides a unified way to resolve datasources using either
their numeric ID or display name, enabling the hybrid lookup pattern.
"""

import logging
from typing import Union
from django.http import Http404
from terno_dbi.core.models import DataSource

logger = logging.getLogger(__name__)


def resolve_datasource(identifier: Union[int, str], enabled_only: bool = True) -> DataSource:
    """
    Resolve a datasource by ID (int) or display_name (str).
    """
    qs = DataSource.objects.all()
    if enabled_only:
        qs = qs.filter(enabled=True)

    try:
        ds_id = int(identifier)
        try:
            ds = qs.get(id=ds_id)
            logger.debug("Datasource resolved by ID: %s -> '%s'", ds_id, ds.display_name)
            return ds
        except DataSource.DoesNotExist:
            logger.warning("Datasource not found by ID: %s", ds_id)
            raise Http404(f"DataSource with ID {ds_id} not found")
    except (ValueError, TypeError):
        pass

    try:
        ds = qs.get(display_name=identifier)
        logger.debug("Datasource resolved by name: '%s' -> id=%d", identifier, ds.id)
        return ds
    except DataSource.DoesNotExist:
        logger.warning("Datasource not found by name: '%s'", identifier)
        raise Http404(f"DataSource '{identifier}' not found")
    except DataSource.MultipleObjectsReturned:
        logger.warning("Multiple datasources found with name: '%s'", identifier)
        raise Http404(f"Multiple datasources found with name '{identifier}'. Please use ID instead.")


def get_datasource_id(identifier: Union[int, str], enabled_only: bool = True) -> int:
    ds = resolve_datasource(identifier, enabled_only=enabled_only)
    return ds.id
