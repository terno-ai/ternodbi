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
            ReportType("Realtime", "Realtime report (last ~30 minutes)",
                       is_date_range_required=False),
            ReportType(
                "Funnel", "Funnel exploration",
                settings=[
                    ReportSetting(
                        "funnel_steps", type="json", label="Funnel steps",
                        help_text="A JSON array of at least two "
                                  "{name, event_name} objects, one per step, "
                                  "in order (e.g. [{\"name\": \"View\", "
                                  "\"event_name\": \"page_view\"}, "
                                  "{\"name\": \"Purchase\", "
                                  "\"event_name\": \"purchase\"}]).",
                    ),
                ],
            ),
        ],
        default_report_type="Default",   # GA4 has an obvious default
        # Conservative guard on the shared Google OAuth app's quota. The GA4 Data
        # API bills in tokens, not requests, so this is a coarse safety net, not a
        # mirror of the real quota — tune against production usage. Per (org,
        # property): ~50 report calls/sec, ~10k/day.
        rate_limit_per_second=50,
        rate_limit_per_day=500000,
        default_enabled=True,
    ),
    ConnectorSpec(
        key="google_search_console",
        display_name="Google Search Console",
        provider="Google",
        category="Analytics",
        family=Family.API,
        auth_type=AuthType.OAUTH,
        description="Search analytics — clicks, impressions, CTR and average "
                    "position by query, page, country and device for verified "
                    "sites.",
        scopes_label="Google Search Console read-only access",
        has_account_list=True,
        has_fields=True,
        is_date_range_required=True,
        account_label_singular="Site",
        account_label_plural="Sites",
        report_types=[
            ReportType("SearchAnalytics", "Search analytics"),
        ],
        default_report_type="SearchAnalytics",
        rate_limit_per_second=50,
        rate_limit_per_day=50000,
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
        scopes_label="YouTube Analytics (incl. revenue), channel and "
                     "memberships read-only access",
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
            ReportType("Revenue", "Revenue (monetized channels)"),
            ReportType("Members", "Channel members", is_date_range_required=False),
        ],
        default_report_type="ChannelTotals",
        rate_limit_per_second=50,
        rate_limit_per_day=50000,
        default_enabled=True,
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
        default_report_type="Insights",
        rate_limit_per_second=50,
        rate_limit_per_day=100000,
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
        default_report_type="Campaign",
        rate_limit_per_second=50,
        rate_limit_per_day=50000,
        default_enabled=True,   # Developer token configured
    ),
    ConnectorSpec(
        key="linkedin_ads",
        display_name="LinkedIn Ads",
        provider="LinkedIn",
        category="Advertising",
        family=Family.API,
        auth_type=AuthType.OAUTH,
        description="Campaign, campaign group and creative performance from "
                    "LinkedIn sponsored ad accounts — spend, impressions, "
                    "clicks, engagements, leads and conversions.",
        scopes_label="LinkedIn ads and ads reporting read access",
        has_account_list=True,
        has_fields=True,
        is_date_range_required=True,
        account_label_singular="Ad account",
        account_label_plural="Ad accounts",
        report_types=[
            ReportType("Campaign", "Campaign performance"),
            ReportType("CampaignGroup", "Campaign group performance"),
            ReportType("Creative", "Creative performance"),
            ReportType("Account", "Account totals"),
        ],
        default_report_type="Campaign",
        # LinkedIn applies a daily application quota rather than a published
        # per-second rate; keep the per-second guard loose and the daily one
        # real, so a runaway agent is stopped before the app-wide quota is.
        rate_limit_per_second=25,
        rate_limit_per_day=50000,
        default_enabled=False,   # LinkedIn Marketing API access pending
    ),
    ConnectorSpec(
        key="google_drive",
        display_name="Google Drive",
        provider="Google",
        category="Productivity",
        family=Family.API,
        auth_type=AuthType.OAUTH,
        description="File and folder metadata from My Drive and shared drives "
                    "— names, owners, types, sizes and modification times.",
        scopes_label="Google Drive metadata read-only access (file names and "
                     "properties, never file contents)",
        has_account_list=True,
        has_fields=True,
        # The date range filters `modifiedTime`, so a query returns only files
        # touched inside it. The report types below declare their own exception
        # for when that flag is honoured per report type.
        is_date_range_required=True,
        account_label_singular="Drive",
        account_label_plural="Drives",
        report_types=[
            ReportType(
                "Files", "Files", is_date_range_required=False,
                settings=[
                    ReportSetting(
                        "folder_id", label="Folder ID", required=False,
                        help_text="Restrict to the direct children of one "
                                  "folder. The ID is the last path segment of "
                                  "the folder's Drive URL.",
                    ),
                    ReportSetting(
                        "name_contains", label="Name contains", required=False,
                        help_text="Match files whose name contains this text.",
                    ),
                    ReportSetting(
                        "mime_type", label="MIME type", required=False,
                        help_text="Restrict to one type, e.g. "
                                  "'application/pdf' or "
                                  "'application/vnd.google-apps.document'.",
                    ),
                ],
            ),
            ReportType(
                "Folders", "Folders", is_date_range_required=False,
                settings=[
                    ReportSetting(
                        "folder_id", label="Parent folder ID", required=False,
                        help_text="Restrict to sub-folders of this folder.",
                    ),
                    ReportSetting(
                        "name_contains", label="Name contains", required=False,
                    ),
                ],
            ),
            ReportType(
                "SharedWithMe", "Shared with me", is_date_range_required=False,
                settings=[
                    ReportSetting(
                        "name_contains", label="Name contains", required=False,
                    ),
                    ReportSetting(
                        "mime_type", label="MIME type", required=False,
                    ),
                ],
            ),
            ReportType(
                "Trashed", "Trash", is_date_range_required=False,
                settings=[
                    ReportSetting(
                        "name_contains", label="Name contains", required=False,
                    ),
                ],
            ),
        ],
        default_report_type="Files",
        # Drive's default user quota is ~200 requests/second; stay well under it
        # so a fan-out across shared drives cannot exhaust the shared OAuth app.
        rate_limit_per_second=20,
        rate_limit_per_day=100000,
        default_enabled=True,
    ),
    ConnectorSpec(
        key="google_sheets",
        display_name="Google Sheets",
        provider="Google",
        category="Productivity",
        family=Family.API,
        auth_type=AuthType.OAUTH,
        description="Read rows from Google Sheets spreadsheets as tabular "
                    "data, using each sheet's header row as its columns.",
        scopes_label="Google Sheets read-only access, plus Drive file metadata "
                     "to find your spreadsheets",
        has_account_list=True,
        has_fields=True,
        # A spreadsheet is current state, not a time series; the connector has
        # no date dimension and ignores the range. Declared True to match the
        # other sources until the per-report flags below are honoured.
        is_date_range_required=True,
        account_label_singular="Spreadsheet",
        account_label_plural="Spreadsheets",
        report_types=[
            ReportType(
                "Values", "Sheet rows", is_date_range_required=False,
                settings=[
                    ReportSetting(
                        "sheet_name", label="Tab name", required=False,
                        help_text="Which tab to read. Defaults to the first "
                                  "visible tab; run the 'Tabs' report to see "
                                  "the names.",
                    ),
                    ReportSetting(
                        "header_row", label="Header row", required=False,
                        help_text="Row number holding the column names. "
                                  "Defaults to 1.",
                    ),
                    ReportSetting(
                        "range", label="Cell range", required=False,
                        help_text="An A1 range to read instead of the whole "
                                  "tab, e.g. 'B2:F500'.",
                    ),
                ],
            ),
            ReportType("Tabs", "Sheet tabs", is_date_range_required=False),
        ],
        default_report_type="Values",
        # The Sheets read quota is 60 requests/minute/user — far tighter than
        # the other Google APIs, so this limit is real rather than a safety net.
        rate_limit_per_second=1,
        rate_limit_per_day=50000,
        default_enabled=True,
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
