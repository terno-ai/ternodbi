"""
This module provides a unified way to resolve datasources using either
their numeric ID or display name, enabling the hybrid lookup pattern.
"""

import logging
from dataclasses import dataclass, field
from typing import List, Optional, Union
from django.http import Http404
from terno_dbi.core.models import DataSource

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Resolution:
    """The outcome of resolving an identifier for a specific caller.

    A small result object rather than exceptions or a bare datasource, so the
    HTTP layer (decorator) and the tool layer (view) can map the *same* outcomes
    to their own responses without duplicating the resolution logic.
    """

    status: str                                  # ok | forbidden | ambiguous | not_found
    datasource: Optional[DataSource] = None      # set only when status == "ok"
    matches: List[DataSource] = field(default_factory=list)  # for "ambiguous"

    @property
    def ok(self) -> bool:
        return self.status == "ok"


def resolve_for_caller(identifier: Union[int, str], allowed_datasources) -> Resolution:
    """Resolve a datasource for a caller within `allowed_datasources`.

    Resolution first tries the datasource ID or display name, then falls back to
    the connector key within the caller's allowed set. Connector keys are not
    globally unique because each organisation can have its own datasource.

    Only `Http404` from the ID/name lookup is treated as not found. Other errors
    propagate so database failures and unexpected bugs are not hidden as missing
    datasources.
    """
    resolved_but_forbidden = False
    try:
        candidate = resolve_datasource(identifier)
    except Http404:
        candidate = None

    if candidate is not None:
        if allowed_datasources.filter(id=candidate.id).exists():
            return Resolution("ok", candidate)
        resolved_but_forbidden = True

    key_matches = list(allowed_datasources.filter(catalog__key=identifier))
    if len(key_matches) == 1:
        return Resolution("ok", key_matches[0])
    if len(key_matches) > 1:
        return Resolution("ambiguous", matches=key_matches)

    return Resolution("forbidden" if resolved_but_forbidden else "not_found")


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
