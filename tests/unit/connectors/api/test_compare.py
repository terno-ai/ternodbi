"""Period comparison (§6.9): date math and row alignment, one test per rule."""

import pytest

from terno_dbi.connectors.api.pipeline.compare import add_comparison, resolve_compare_range
from terno_dbi.connectors.api.model.errors import ApiError
from terno_dbi.connectors.api.model.types import Compare, DateRange


class TestResolveRange:
    def test_prev_range_is_the_immediately_preceding_window(self):
        base = DateRange("2026-08-01", "2026-08-31")   # 31 days
        prev = resolve_compare_range(base, Compare("prev_range"))
        assert prev.end == "2026-07-31"                # day before base start
        assert prev.start == "2026-07-01"              # same span

    def test_prev_year_shifts_by_a_year(self):
        base = DateRange("2026-03-01", "2026-03-31")
        prev = resolve_compare_range(base, Compare("prev_year"))
        assert (prev.start, prev.end) == ("2025-03-01", "2025-03-31")

    def test_prev_year_maps_leap_day_to_28_feb(self):
        base = DateRange("2024-02-29", "2024-02-29")   # 2024 is a leap year
        prev = resolve_compare_range(base, Compare("prev_year"))
        assert prev.start == "2023-02-28"

    def test_prev_year_weekday_preserves_weekday(self):
        # 2026-08-03 is a Monday; 364 days earlier is also a Monday.
        base = DateRange("2026-08-03", "2026-08-09")
        prev = resolve_compare_range(base, Compare("prev_year_weekday"))
        assert prev.start == "2025-08-04"              # Monday
        import datetime
        assert datetime.date.fromisoformat(prev.start).weekday() == 0

    def test_custom_uses_supplied_dates(self):
        base = DateRange("2026-08-01", "2026-08-31")
        prev = resolve_compare_range(
            base, Compare("custom", start="2025-01-01", end="2025-01-31"))
        assert (prev.start, prev.end) == ("2025-01-01", "2025-01-31")

    def test_custom_without_dates_is_an_error(self):
        with pytest.raises(ApiError):
            resolve_compare_range(DateRange("2026-08-01", "2026-08-31"),
                                  Compare("custom"))


class TestAlignment:
    def test_metrics_only_single_row(self):
        base = [{"sessions": 100}]
        compare = [{"sessions": 80}]
        rows, warnings = add_comparison(
            base, compare,
            plain_dimensions=[], date_dimensions=[], metrics=["sessions"],
            base_start="2026-08-01", compare_start="2026-07-01",
            show="perc_change",
        )
        assert rows[0]["sessions"] == 100
        assert rows[0]["sessions__compare"] == 80
        assert rows[0]["sessions__delta"] == pytest.approx(25.0)  # +25%
        assert warnings == []

    def test_plain_dimension_join(self):
        base = [{"country": "US", "clicks": 10}, {"country": "CA", "clicks": 5}]
        compare = [{"country": "CA", "clicks": 4}, {"country": "US", "clicks": 8}]
        rows, _ = add_comparison(
            base, compare,
            plain_dimensions=["country"], date_dimensions=[], metrics=["clicks"],
            base_start="2026-08-01", compare_start="2026-07-01",
            show="abs_change",
        )
        by_country = {r["country"]: r for r in rows}
        assert by_country["US"]["clicks__delta"] == 2   # 10 - 8
        assert by_country["CA"]["clicks__delta"] == 1   # 5 - 4

    def test_date_dimension_aligns_by_offset_not_absolute(self):
        # base day 1 should compare against compare day 1, despite different dates.
        base = [{"date": "2026-08-01", "sessions": 30}]
        compare = [{"date": "2026-07-01", "sessions": 20}]
        rows, _ = add_comparison(
            base, compare,
            plain_dimensions=[], date_dimensions=["date"], metrics=["sessions"],
            base_start="2026-08-01", compare_start="2026-07-01",
            show="value",
        )
        assert len(rows) == 1
        assert rows[0]["sessions__compare"] == 20

    def test_row_only_in_base_has_null_compare(self):
        base = [{"country": "US", "clicks": 10}, {"country": "MX", "clicks": 3}]
        compare = [{"country": "US", "clicks": 8}]
        rows, _ = add_comparison(
            base, compare,
            plain_dimensions=["country"], date_dimensions=[], metrics=["clicks"],
            base_start="2026-08-01", compare_start="2026-07-01",
        )
        mx = next(r for r in rows if r["country"] == "MX")
        assert mx["clicks__compare"] is None
        assert mx["clicks__delta"] is None      # not a fake -100%

    def test_row_only_in_compare_appears_with_null_base(self):
        base = [{"country": "US", "clicks": 10}]
        compare = [{"country": "US", "clicks": 8}, {"country": "FR", "clicks": 2}]
        rows, warnings = add_comparison(
            base, compare,
            plain_dimensions=["country"], date_dimensions=[], metrics=["clicks"],
            base_start="2026-08-01", compare_start="2026-07-01",
        )
        fr = next(r for r in rows if r["country"] == "FR")
        assert fr["clicks__compare"] == 2
        assert fr["clicks__delta"] is None
        assert warnings   # a warning about compare-only rows

    def test_zero_base_gives_null_percentage_not_infinity(self):
        rows, _ = add_comparison(
            [{"x": 5}], [{"x": 0}],
            plain_dimensions=[], date_dimensions=[], metrics=["x"],
            base_start="2026-08-01", compare_start="2026-07-01",
            show="perc_change",
        )
        assert rows[0]["x__delta"] is None
