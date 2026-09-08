"""
Defines the capabilities and default configuration of a connector.

`ConnectorSpec` is the source of truth for connector metadata and capabilities.
Code-owned fields are refreshed into `ConnectorCatalog`, while deployment-specific
settings remain managed by the database.
"""

from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional


class Family:
    """Selects how the connector is executed; separate from its auth type."""
    DATABASE = "database"
    API = "api"


class AuthType:
    """Selects the connector's connection flow; separate from its family."""

    OAUTH = "oauth"
    MANUAL = "manual"


@dataclass(frozen=True)
class FormField:
    """Defines an input field for a manual connector's credentials form."""

    name: str
    type: str = "string"          # string | integer | password | textarea
    required: bool = True
    sensitive: bool = False
    label: str = ""
    help_text: str = ""
    placeholder: str = ""

    def as_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ReportSetting:
    """Defines a required setting needed to run a report type."""

    setting_id: str
    type: str = "text"
    label: str = ""
    help_text: str = ""
    required: bool = True

    def as_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ReportType:
    """A named shape of report an API source can return."""

    id: str
    label: str
    is_date_range_required: bool = True
    settings: List[ReportSetting] = field(default_factory=list)

    def as_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "label": self.label,
            "is_date_range_required": self.is_date_range_required,
            "settings": [s.as_dict() for s in self.settings],
        }


@dataclass(frozen=True)
class ConnectorSpec:
    key: str
    display_name: str
    family: str
    auth_type: str

    provider: str = ""
    category: str = ""
    description: str = ""
    icon_url: str = ""
    scopes_label: str = ""

    # Manual connectors only; defines the credentials form.
    # API fields are fetched at runtime because they can vary by account or property.
    form_fields: List[FormField] = field(default_factory=list)

    # api connectors only
    has_account_list: bool = False
    has_fields: bool = False
    is_date_range_required: bool = False
    report_types: List[ReportType] = field(default_factory=list)

    default_report_type: Optional[str] = None

    account_label_singular: str = "Account"
    account_label_plural: str = "Accounts"

    # Only consulted when the catalog row is first created. Afterwards
    # `enabled` belongs to the database and a refresh leaves it alone.
    default_enabled: bool = True

    def __post_init__(self) -> None:
        if self.family not in (Family.DATABASE, Family.API):
            raise ValueError(f"{self.key}: unknown family {self.family!r}")
        if self.auth_type not in (AuthType.OAUTH, AuthType.MANUAL):
            raise ValueError(f"{self.key}: unknown auth_type {self.auth_type!r}")
        if self.auth_type == AuthType.MANUAL and not self.form_fields:
            raise ValueError(
                f"{self.key}: a manual connector needs form_fields, or the user "
                f"has no way to supply credentials"
            )
        if self.report_types and not self.has_fields:
            raise ValueError(
                f"{self.key}: declares report types but has_fields is False; "
                f"an agent could select a report type and then have no fields "
                f"to request"
            )
        if self.default_report_type is not None:
            ids = {r.id for r in self.report_types}
            if self.default_report_type not in ids:
                raise ValueError(
                    f"{self.key}: default_report_type "
                    f"{self.default_report_type!r} is not one of its declared "
                    f"report types {sorted(ids)}"
                )

    @property
    def has_report_types(self) -> bool:
        return bool(self.report_types)

    def code_owned_fields(self) -> Dict[str, Any]:
        """Exactly the columns a refresh writes.

        `enabled`, `sort_order`, `most_popular`, and name/description overrides
        are deliberately absent — those belong to the database.
        """
        return {
            "display_name": self.display_name,
            "family": self.family,
            "auth_type": self.auth_type,
            "provider": self.provider,
            "category": self.category,
            "description": self.description,
            "icon_url": self.icon_url,
            "scopes_label": self.scopes_label,
            "fields_spec": [f.as_dict() for f in self.form_fields],
            "has_account_list": self.has_account_list,
            "has_fields": self.has_fields,
            "has_report_types": self.has_report_types,
            "is_date_range_required": self.is_date_range_required,
            "report_types": [r.as_dict() for r in self.report_types],
            "default_report_type": self.default_report_type or "",
            "account_label_singular": self.account_label_singular,
            "account_label_plural": self.account_label_plural,
        }


__all__ = [
    "AuthType",
    "ConnectorSpec",
    "Family",
    "FormField",
    "ReportSetting",
    "ReportType",
]
