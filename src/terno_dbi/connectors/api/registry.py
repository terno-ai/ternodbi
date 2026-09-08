"""Create the `ApiConnector` for a datasource.

The factory maps each `catalog.key` to its concrete connector, keeping provider
imports out of the tool layer. New connectors register here as they are added.

If a datasource has no registered connector, return a clear error instead of
failing unexpectedly; the source may be cataloged but not implemented yet.
"""

from __future__ import annotations
from typing import Callable, Dict
from terno_dbi.connectors.api.model.base import ApiConnector
from terno_dbi.connectors.api.model.errors import ApiError, ErrorCode

# key -> factory(datasource) -> ApiConnector
_REGISTRY: Dict[str, Callable[[object], ApiConnector]] = {}


def register(key: str, factory: Callable[[object], ApiConnector]) -> None:
    _REGISTRY[key] = factory


def unregister(key: str) -> None:
    _REGISTRY.pop(key, None)


def is_supported(key: str) -> bool:
    return key in _REGISTRY


def build_connector(data_source) -> ApiConnector:
    key = data_source.catalog.key if data_source.catalog_id else data_source.type
    factory = _REGISTRY.get(key)
    if factory is None:
        raise ApiError(
            ErrorCode.UPSTREAM_ERROR,
            f"{key} is listed but not yet queryable in this version.",
        )
    return factory(data_source)


__all__ = ["build_connector", "is_supported", "register", "unregister"]
