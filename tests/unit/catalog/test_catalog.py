"""Phase 0 acceptance: the connector catalog.

The catalog is a *projection* of `terno_dbi.catalog.declarations`, so the
behaviour worth pinning is the ownership split: a refresh must overwrite
provider facts and must never touch the columns a deployment owns.
"""

import pytest

from terno_dbi.catalog import (
    AuthType,
    ConnectorSpec,
    DECLARED_CONNECTORS,
    Family,
    FormField,
    ReportSetting,
    ReportType,
    canonical_key,
    declared_keys,
)
from terno_dbi.catalog.refresh import refresh_catalog
from terno_dbi.core.models import ConnectorCatalog, DataSource


# ---------------------------------------------------------------------------
# Declarations — pure, no database
# ---------------------------------------------------------------------------


class TestDeclarations:
    def test_keys_are_unique(self):
        keys = declared_keys()
        assert len(keys) == len(set(keys))

    def test_every_spec_declares_a_known_family_and_auth_type(self):
        for spec in DECLARED_CONNECTORS:
            assert spec.family in (Family.DATABASE, Family.API)
            assert spec.auth_type in (AuthType.OAUTH, AuthType.MANUAL)

    def test_manual_connector_must_describe_its_form(self):
        # Without form fields a user has no way to supply credentials, so this
        # is a construction error rather than a runtime surprise.
        with pytest.raises(ValueError, match="form_fields"):
            ConnectorSpec(
                key="x", display_name="X",
                family=Family.DATABASE, auth_type=AuthType.MANUAL,
            )

    def test_report_types_require_fields(self):
        # Selecting a report type and then having no fields to request is a
        # dead end for an agent.
        with pytest.raises(ValueError, match="has_fields"):
            ConnectorSpec(
                key="x", display_name="X",
                family=Family.API, auth_type=AuthType.OAUTH,
                report_types=[ReportType("R", "R")],
            )

    def test_default_report_type_must_be_a_declared_one(self):
        with pytest.raises(ValueError, match="default_report_type"):
            ConnectorSpec(
                key="x", display_name="X",
                family=Family.API, auth_type=AuthType.OAUTH,
                has_fields=True,
                report_types=[ReportType("A", "A")],
                default_report_type="NotDeclared",
            )

    def test_default_report_type_is_optional(self):
        # A source may legitimately have no obvious default (the choice matters).
        ConnectorSpec(
            key="x", display_name="X",
            family=Family.API, auth_type=AuthType.OAUTH,
            has_fields=True,
            report_types=[ReportType("A", "A"), ReportType("B", "B")],
        )   # default_report_type unset — must not raise

    def test_rate_limits_default_to_unlimited_and_are_projected(self):
        spec = ConnectorSpec(
            key="x", display_name="X",
            family=Family.API, auth_type=AuthType.OAUTH,
        )
        owned = spec.code_owned_fields()
        # Absent = 0 = no limit on that axis.
        assert owned["rate_limit_per_second"] == 0
        assert owned["rate_limit_per_day"] == 0

        spec = ConnectorSpec(
            key="y", display_name="Y",
            family=Family.API, auth_type=AuthType.OAUTH,
            rate_limit_per_second=10, rate_limit_per_day=10000,
        )
        owned = spec.code_owned_fields()
        assert owned["rate_limit_per_second"] == 10
        assert owned["rate_limit_per_day"] == 10000

    def test_negative_rate_limit_is_rejected(self):
        with pytest.raises(ValueError, match="rate_limit_per_second"):
            ConnectorSpec(
                key="x", display_name="X",
                family=Family.API, auth_type=AuthType.OAUTH,
                rate_limit_per_second=-1,
            )

    def test_code_owned_fields_exclude_deployment_columns(self):
        fields = DECLARED_CONNECTORS[0].code_owned_fields()
        for owned_by_deployment in (
            "enabled", "sort_order", "most_popular",
            "display_name_override", "description_override",
        ):
            assert owned_by_deployment not in fields

    def test_sensitive_form_fields_are_marked(self):
        # The web form relies on this flag to mask input, and the plan relies on
        # it to keep secrets out of tool arguments.
        for spec in DECLARED_CONNECTORS:
            for field in spec.form_fields:
                if field.name in ("password", "token", "rsa_key",
                                  "service_account_json", "rsa_key_password"):
                    assert field.sensitive, f"{spec.key}.{field.name}"

    @pytest.mark.parametrize("db_type,expected", [
        ("sqlite", "generic"),        # ConnectorFactory alias
        ("postgresql", "postgres"),   # ConnectorFactory alias
        ("postgres", "postgres"),
        ("BigQuery", "bigquery"),     # case-insensitive
        ("unknown", "unknown"),       # passed through, not guessed at
    ])
    def test_canonical_key_maps_factory_aliases(self, db_type, expected):
        assert canonical_key(db_type) == expected


# ---------------------------------------------------------------------------
# refresh_catalog
# ---------------------------------------------------------------------------


@pytest.mark.django_db
class TestRefreshCatalog:
    def test_seeds_every_declared_connector(self):
        refresh_catalog()
        assert set(
            ConnectorCatalog.objects.values_list("key", flat=True)
        ) >= set(declared_keys())

    def test_is_idempotent(self):
        refresh_catalog()
        second = refresh_catalog()
        assert second == {"created": 0, "updated": 0, "retired": 0}

    def test_preserves_deployment_owned_columns(self):
        refresh_catalog()
        row = ConnectorCatalog.objects.get(key="googleanalytics4")
        row.enabled = False
        row.sort_order = 5
        row.most_popular = True
        row.display_name_override = "GA4 (ours)"
        row.description_override = "Our wording."
        row.save()

        refresh_catalog()
        row.refresh_from_db()

        assert row.enabled is False
        assert row.sort_order == 5
        assert row.most_popular is True
        assert row.name == "GA4 (ours)"
        assert row.summary == "Our wording."

    def test_restores_code_owned_columns(self):
        refresh_catalog()
        row = ConnectorCatalog.objects.get(key="googleanalytics4")
        row.description = "tampered"
        row.has_account_list = False
        row.save(update_fields=["description", "has_account_list"])

        refresh_catalog()
        row.refresh_from_db()

        assert row.description != "tampered"
        assert row.has_account_list is True

    def test_default_enabled_applies_only_at_creation(self):
        refresh_catalog()
        # meta_ads is declared default_enabled=False (approval pending), but an
        # operator who enables it must not have that undone by a deploy.
        row = ConnectorCatalog.objects.get(key="meta_ads")
        assert row.enabled is False
        row.enabled = True
        row.save()

        refresh_catalog()
        row.refresh_from_db()
        assert row.enabled is True

    def test_undeclared_connector_is_disabled_not_deleted(self):
        refresh_catalog()
        stale = ConnectorCatalog.objects.create(
            key="retired_source", display_name="Retired",
            family=Family.DATABASE, auth_type=AuthType.MANUAL, enabled=True,
        )
        # A DataSource may still point at it, and the FK is PROTECT — deleting
        # would either fail mid-deploy or orphan the customer's configuration.
        DataSource.objects.create(
            display_name="Legacy", type="generic",
            connection_str="sqlite:///legacy.db", catalog=stale,
        )

        result = refresh_catalog()

        stale.refresh_from_db()
        assert stale.enabled is False
        assert result["retired"] == 1
        assert ConnectorCatalog.objects.filter(key="retired_source").exists()


# ---------------------------------------------------------------------------
# Model behaviour
# ---------------------------------------------------------------------------


@pytest.mark.django_db
class TestConnectorCatalogModel:
    def test_required_settings_for_report_type(self):
        refresh_catalog()
        youtube = ConnectorCatalog.objects.get(key="youtube")
        # VideoTotals cannot run without a video id — a setting, not a filter.
        assert [
            s["setting_id"] for s in youtube.required_settings("VideoTotals")
        ] == ["video_id"]
        assert youtube.required_settings("Geo") == []
        assert youtube.required_settings("NoSuchReport") == []

    def test_family_and_auth_type_are_independent_axes(self):
        refresh_catalog()
        # Branching execution on auth_type would route BigQuery to the API
        # strategy and break it.
        bigquery = ConnectorCatalog.objects.get(key="bigquery")
        assert bigquery.family == Family.DATABASE
        assert bigquery.is_api is False

        ga4 = ConnectorCatalog.objects.get(key="googleanalytics4")
        assert ga4.family == Family.API
        assert ga4.auth_type == AuthType.OAUTH


@pytest.mark.django_db
class TestDataSourceCatalogLink:
    def test_family_defaults_to_database_without_a_catalog(self):
        # Rows predating the catalog were all databases by definition.
        ds = DataSource.objects.create(
            display_name="Legacy", type="postgres",
            connection_str="postgresql://u:p@h:5432/d",
        )
        assert ds.family == Family.DATABASE
        assert ds.is_api is False

    def test_database_datasource_requires_a_connection_string(self):
        from django.core.exceptions import ValidationError

        refresh_catalog()
        ds = DataSource(
            display_name="Broken", type="postgres", connection_str="   ",
            catalog=ConnectorCatalog.objects.get(key="postgres"),
        )
        with pytest.raises(ValidationError):
            ds.full_clean(exclude=["organisation"])

    def test_api_datasource_needs_no_connection_string(self):
        refresh_catalog()
        ds = DataSource(
            display_name="Our GA4", type="googleanalytics4", connection_str="",
            catalog=ConnectorCatalog.objects.get(key="googleanalytics4"),
        )
        ds.clean()   # must not raise

    def test_api_datasource_rejected_when_source_is_disabled(self):
        from django.core.exceptions import ValidationError

        refresh_catalog()
        catalog = ConnectorCatalog.objects.get(key="meta_ads")
        assert catalog.enabled is False   # approval pending
        ds = DataSource(
            display_name="Meta", type="meta_ads",
            connection_str="", catalog=catalog,
        )
        with pytest.raises(ValidationError, match="not enabled"):
            ds.clean()

    @pytest.mark.parametrize("status,expected", [
        (DataSource.AuthStatus.CONNECTED, False),
        (DataSource.AuthStatus.EXPIRED, True),
        (DataSource.AuthStatus.NOT_AUTHENTICATED, True),
    ])
    def test_needs_reconnect(self, status, expected):
        ds = DataSource(
            display_name="X", type="postgres",
            connection_str="postgresql://u:p@h:5432/d", auth_status=status,
        )
        assert ds.needs_reconnect is expected
