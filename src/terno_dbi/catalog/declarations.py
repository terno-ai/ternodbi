"""Catalog of connectors available in TernoDBI.

A `ConnectorCatalog` entry represents a connector TernoDBI supports, while a
`DataSource` represents one that an organisation has connected.

Unconnected connectors remain in the catalog so agents can show what is
available and guide users through connecting it.

Add new connectors as `ConnectorSpec` entries and run `refresh_catalog()`.
Catalog rows should not be created manually.
"""

from typing import Dict, List

from terno_dbi.catalog.spec import (
    AuthType,
    ConnectorSpec,
    Family,
    FormField,
    ReportSetting,
    ReportType,
)

# --------------------------------------------------------------------------
# Shared form fragments
# --------------------------------------------------------------------------


def _host_port(default_port: int) -> List[FormField]:
    return [
        FormField("host", label="Host", placeholder="db.example.com"),
        FormField("port", type="integer", label="Port",
                  placeholder=str(default_port)),
    ]


def _user_password() -> List[FormField]:
    return [
        FormField("user", label="Username"),
        FormField("password", type="password", sensitive=True, label="Password"),
    ]


# --------------------------------------------------------------------------
# Databases — family=database
# --------------------------------------------------------------------------

_DATABASES: List[ConnectorSpec] = [
    ConnectorSpec(
        key="postgres",
        display_name="PostgreSQL",
        provider="PostgreSQL",
        category="Database",
        family=Family.DATABASE,
        auth_type=AuthType.MANUAL,
        description="Query a PostgreSQL database with full schema, column and "
                    "row-level access control.",
        form_fields=[
            *_host_port(5432),
            *_user_password(),
            FormField("database", label="Database"),
            FormField("schema", required=False, label="Schema",
                      help_text="Defaults to 'public' when left blank."),
        ],
    ),
    ConnectorSpec(
        key="mysql",
        display_name="MySQL",
        provider="MySQL",
        category="Database",
        family=Family.DATABASE,
        auth_type=AuthType.MANUAL,
        description="Query a MySQL or MariaDB database.",
        form_fields=[
            *_host_port(3306),
            *_user_password(),
            FormField("database", label="Database"),
        ],
    ),
    ConnectorSpec(
        key="oracle",
        display_name="Oracle",
        provider="Oracle",
        category="Database",
        family=Family.DATABASE,
        auth_type=AuthType.MANUAL,
        description="Query an Oracle database.",
        form_fields=[
            *_host_port(1521),
            *_user_password(),
            FormField("service_name", label="Service name"),
        ],
    ),
    ConnectorSpec(
        key="snowflake",
        display_name="Snowflake",
        provider="Snowflake",
        category="Warehouse",
        family=Family.DATABASE,
        auth_type=AuthType.MANUAL,
        description="Query a Snowflake warehouse.",
        form_fields=[
            FormField("account", label="Account identifier",
                      placeholder="xy12345.eu-central-1"),
            FormField("user", label="Username"),
            FormField("password", type="password", required=False,
                      sensitive=True, label="Password",
                      help_text="Supply either a password or an RSA key."),
            FormField("rsa_key", type="textarea", required=False,
                      sensitive=True, label="RSA private key"),
            FormField("rsa_key_password", type="password", required=False,
                      sensitive=True, label="RSA key passphrase"),
            FormField("warehouse", label="Warehouse"),
            FormField("database", label="Database"),
            FormField("schema", label="Schema"),
        ],
    ),
    ConnectorSpec(
        key="databricks",
        display_name="Databricks",
        provider="Databricks",
        category="Warehouse",
        family=Family.DATABASE,
        auth_type=AuthType.MANUAL,
        description="Query a Databricks SQL warehouse.",
        form_fields=[
            FormField("host", label="Workspace host",
                      placeholder="dbc-1234.cloud.databricks.com"),
            FormField("http_path", label="HTTP path",
                      placeholder="/sql/1.0/endpoints/1234"),
            FormField("token", type="password", sensitive=True,
                      label="Access token"),
            FormField("catalog", required=False, label="Catalog"),
            FormField("schema", required=False, label="Schema"),
        ],
    ),
    ConnectorSpec(
        key="bigquery",
        display_name="BigQuery",
        provider="Google",
        category="Warehouse",
        family=Family.DATABASE,
        auth_type=AuthType.MANUAL,
        description="Query Google BigQuery datasets.",
        form_fields=[
            FormField("project_id", label="Project ID"),
            FormField("dataset_id", label="Dataset ID"),
            FormField("service_account_json", type="textarea", sensitive=True,
                      label="Service account key (JSON)",
                      help_text="Paste the full contents of the service "
                                "account JSON key file."),
        ],
    ),
    ConnectorSpec(
        key="generic",
        display_name="Generic (SQLite)",
        provider="",
        category="Database",
        family=Family.DATABASE,
        auth_type=AuthType.MANUAL,
        description="A local SQLite file. Intended for administration and "
                    "testing rather than production use.",
        form_fields=[
            FormField("path", label="Database file path"),
        ],
        default_enabled=False,
    ),
]


# --------------------------------------------------------------------------
# Marketing / analytics APIs — family=api
# --------------------------------------------------------------------------

_APIS: List[ConnectorSpec] = [
    ConnectorSpec(
        key="googleanalytics4",
        display_name="Google Analytics 4",
        provider="Google",
        category="Analytics",
        family=Family.API,
        auth_type=AuthType.OAUTH,
        description="Sessions, users, traffic sources, events and ecommerce "
                    "from GA4 properties.",
        scopes_label="Google Analytics read-only access",
        has_account_list=True,
        has_fields=True,
        is_date_range_required=True,
        account_label_singular="Property",
        account_label_plural="Properties",
        report_types=[
            ReportType("Default", "Standard report"),
            ReportType("CohortDaily", "Daily cohort"),
            ReportType("CohortWeekly", "Weekly cohort"),
            ReportType("CohortMonthly", "Monthly cohort"),
        ],
        default_report_type="Default",   # GA4 has an obvious default
        default_enabled=True,
    ),
    ConnectorSpec(
        key="youtube",
        display_name="YouTube",
        provider="Google",
        category="Social media",
        family=Family.API,
        auth_type=AuthType.OAUTH,
        description="Channel and video performance, demographics, traffic "
                    "sources and ad performance for YouTube channels.",
        scopes_label="YouTube Analytics read-only access",
        has_account_list=True,
        has_fields=True,
        is_date_range_required=True,
        account_label_singular="Channel",
        account_label_plural="Channels",
        report_types=[
            ReportType("ChannelTotals", "Channel overview"),
            ReportType("LatestVideos", "Videos performance"),
            ReportType(
                "VideoTotals", "Single video performance",
                settings=[
                    ReportSetting(
                        "video_id", label="Video ID",
                        help_text="The ID after 'v=' in the video URL. For "
                                  "https://www.youtube.com/watch?v=YVWI6nwtiGo&t=4s "
                                  "the ID is 'YVWI6nwtiGo&t=4s'.",
                    ),
                ],
            ),
            ReportType("Geo", "Geographies"),
            ReportType("Demographic", "Demographics"),
            ReportType("Device", "Devices"),
            ReportType("TrafficSources", "Traffic sources"),
        ],
        default_enabled=False,   # OAuth verification pending
    ),
    ConnectorSpec(
        key="meta_ads",
        display_name="Meta Ads",
        provider="Meta",
        category="Advertising",
        family=Family.API,
        auth_type=AuthType.OAUTH,
        description="Campaigns, ad sets, ads and insights from Meta "
                    "(Facebook and Instagram) ad accounts.",
        scopes_label="Meta ads read access",
        has_account_list=True,
        has_fields=True,
        is_date_range_required=True,
        account_label_singular="Ad account",
        account_label_plural="Ad accounts",
        report_types=[
            ReportType("Insights", "Ad insights"),
            ReportType("Campaigns", "Campaigns"),
            ReportType("AdSets", "Ad sets"),
            ReportType("Ads", "Ads"),
        ],
        default_enabled=False,   # Business Verification + App Review pending
    ),
    ConnectorSpec(
        key="google_ads",
        display_name="Google Ads",
        provider="Google",
        category="Advertising",
        family=Family.API,
        auth_type=AuthType.OAUTH,
        description="Campaign, ad group, keyword and search-term performance "
                    "from Google Ads accounts.",
        scopes_label="Google Ads read access",
        has_account_list=True,
        has_fields=True,
        is_date_range_required=True,
        account_label_singular="Ad account",
        account_label_plural="Ad accounts",
        report_types=[
            ReportType("Campaign", "Campaign performance"),
            ReportType("AdGroup", "Ad group performance"),
            ReportType("Keyword", "Keyword performance"),
            ReportType("SearchTerm", "Search terms"),
        ],
        default_enabled=False,   # Developer token pending — longest lead item
    ),
]


DECLARED_CONNECTORS: List[ConnectorSpec] = [*_DATABASES, *_APIS]


# Map `DataSource.type` aliases like `sqlite` and `postgresql` to canonical
# catalog keys instead of creating duplicate connector entries.
TYPE_ALIASES: Dict[str, str] = {
    "sqlite": "generic",        # both resolve to SQLiteConnector
    "postgresql": "postgres",
}


def canonical_key(db_type: str) -> str:
    """The catalog key for a `DataSource.type` value."""
    key = (db_type or "").strip().lower()
    return TYPE_ALIASES.get(key, key)


def _check_unique(specs: List[ConnectorSpec]) -> None:
    seen: Dict[str, ConnectorSpec] = {}
    for spec in specs:
        if spec.key in seen:
            raise ValueError(f"Duplicate connector key {spec.key!r}")
        seen[spec.key] = spec


_check_unique(DECLARED_CONNECTORS)


def declared_keys() -> List[str]:
    return [spec.key for spec in DECLARED_CONNECTORS]


def get_spec(key: str) -> ConnectorSpec | None:
    for spec in DECLARED_CONNECTORS:
        if spec.key == key:
            return spec
    return None


__all__ = [
    "DECLARED_CONNECTORS",
    "TYPE_ALIASES",
    "canonical_key",
    "declared_keys",
    "get_spec",
]
