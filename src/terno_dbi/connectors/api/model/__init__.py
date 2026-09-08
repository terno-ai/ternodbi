"""Model layer: the value types, the connector contract, and their validation.

Self-contained by design — depends on nothing else under `connectors.api`, so
the other layers (pipeline, auth, sources) may all build on it without cycles.
"""

from terno_dbi.connectors.api.model.base import ApiConnector
from terno_dbi.connectors.api.model.errors import (
    ApiError,
    ErrorCode,
    invalid_field,
    invalid_setting,
    missing_setting,
)
from terno_dbi.connectors.api.model.settings_validation import validate_settings
from terno_dbi.connectors.api.model.types import (
    Account,
    Compare,
    CompareShow,
    CompareType,
    DateRange,
    Field,
    FieldKind,
    QueryResult,
    QuerySpec,
)

__all__ = [
    "Account",
    "ApiConnector",
    "ApiError",
    "Compare",
    "CompareShow",
    "CompareType",
    "DateRange",
    "ErrorCode",
    "Field",
    "FieldKind",
    "QueryResult",
    "QuerySpec",
    "invalid_field",
    "invalid_setting",
    "missing_setting",
    "validate_settings",
]
