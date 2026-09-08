"""The connector catalog: what TernoDBI can offer, declared in code.

`ConnectorCatalog` rows are a projection of `DECLARED_CONNECTORS`; see
`spec.py` for the code-owned / database-owned split.
"""

from terno_dbi.catalog.declarations import (
    DECLARED_CONNECTORS,
    TYPE_ALIASES,
    canonical_key,
    declared_keys,
    get_spec,
)
from terno_dbi.catalog.spec import (
    AuthType,
    ConnectorSpec,
    Family,
    FormField,
    ReportSetting,
    ReportType,
)

__all__ = [
    "AuthType",
    "ConnectorSpec",
    "DECLARED_CONNECTORS",
    "Family",
    "TYPE_ALIASES",
    "canonical_key",
    "FormField",
    "ReportSetting",
    "ReportType",
    "declared_keys",
    "get_spec",
]
