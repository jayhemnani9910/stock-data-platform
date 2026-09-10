"""Tests for scripts/populate_dim_date.py — generate_date_records().

The record-building loop used to be inlined in the DB-writing function, so
there was nothing pure to import and the test carried its own copy. Flipping
the weekend rule to `dow >= 4` in production left the suite green.
"""

from datetime import date

import populate_dim_date
from populate_dim_date import END_DATE, START_DATE, generate_date_records


class TestGenerateDateRecords:
    def test_single_day(self):
        records = generate_date_records(date(2024, 3, 15), date(2024, 3, 15))
        assert len(records) == 1
        d, year, quarter, month, day, dow, is_weekend = records[0]
        assert d == date(2024, 3, 15)
        assert (year, quarter, month, day) == (2024, 1, 3, 15)
        assert dow == 4 and is_weekend is False

    def test_weekend_detection(self):
        # 2024-01-06 Sat, 01-07 Sun, 01-08 Mon
        records = generate_date_records(date(2024, 1, 6), date(2024, 1, 8))
        assert [r[6] for r in records] == [True, True, False]

    def test_friday_is_not_a_weekend(self):
        """Pins the boundary. `dow >= 4` would make Friday a weekend."""
        records = generate_date_records(date(2024, 1, 5), date(2024, 1, 5))
        assert records[0][5] == 4
        assert records[0][6] is False

    def test_quarter_boundaries(self):
        for month, expected in ((1, 1), (3, 1), (4, 2), (6, 2), (7, 3), (9, 3), (10, 4), (12, 4)):
            records = generate_date_records(date(2024, month, 1), date(2024, month, 1))
            assert records[0][2] == expected, f"month {month}"

    def test_date_range_length(self):
        records = generate_date_records(date(2024, 1, 1), date(2024, 1, 31))
        assert len(records) == 31

    def test_leap_year(self):
        records = generate_date_records(date(2024, 2, 1), date(2024, 2, 29))
        assert len(records) == 29
        assert records[-1][0] == date(2024, 2, 29)

    def test_empty_when_end_precedes_start(self):
        assert generate_date_records(date(2024, 1, 2), date(2024, 1, 1)) == []

    def test_column_order_matches_the_insert(self):
        """The tuple is fed positionally into the dim_date INSERT."""
        (row,) = generate_date_records(date(2024, 3, 15), date(2024, 3, 15))
        assert len(row) == 7


class TestConfiguredRange:
    """The range has to cover every date any loader can produce. It used to
    start at 1990 while FRED macro series start in 1947, which left 53% of
    fact_macro_data with no dimension row to join to."""

    def test_covers_the_oldest_fred_observation(self):
        assert START_DATE <= date(1947, 1, 1)

    def test_reaches_well_past_today(self):
        assert END_DATE >= date(2050, 1, 1)

    def test_generate_dates_uses_the_configured_range(self, monkeypatch):
        captured = {}

        def fake_batch_insert(conn, sql, records, **kwargs):
            captured["records"] = records
            captured["sql"] = sql

        class _Conn:
            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

        monkeypatch.setattr(populate_dim_date, "batch_insert", fake_batch_insert)
        monkeypatch.setattr(populate_dim_date, "get_db_connection", lambda: _Conn())
        populate_dim_date.generate_dates()

        assert captured["records"][0][0] == START_DATE
        assert captured["records"][-1][0] == END_DATE
        assert "ON CONFLICT (date) DO NOTHING" in captured["sql"]
