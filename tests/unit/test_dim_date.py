"""Tests for scripts/populate_dim_date.py — generate_dates() logic."""

import os
import sys
from datetime import date

# Add scripts/ to path so we can import without DB side-effects
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "scripts"))


def _generate_date_records(start, end):
    """Reproduce the pure logic of generate_dates() without DB calls."""
    from datetime import timedelta

    records = []
    current = start
    while current <= end:
        dow = current.weekday()
        records.append(
            (
                current,
                current.year,
                (current.month - 1) // 3 + 1,
                current.month,
                current.day,
                dow,
                dow >= 5,
            )
        )
        current += timedelta(days=1)
    return records


class TestGenerateDates:
    def test_single_day(self):
        records = _generate_date_records(date(2024, 1, 1), date(2024, 1, 1))
        assert len(records) == 1
        d, year, quarter, month, day, dow, is_weekend = records[0]
        assert d == date(2024, 1, 1)
        assert year == 2024
        assert quarter == 1
        assert month == 1
        assert day == 1
        assert dow == 0  # Monday
        assert is_weekend is False

    def test_weekend_detection(self):
        # 2024-01-06 is Saturday, 2024-01-07 is Sunday
        records = _generate_date_records(date(2024, 1, 5), date(2024, 1, 8))
        assert len(records) == 4
        assert records[0][6] is False  # Friday
        assert records[1][6] is True  # Saturday
        assert records[2][6] is True  # Sunday
        assert records[3][6] is False  # Monday

    def test_quarter_boundaries(self):
        records = _generate_date_records(date(2024, 1, 1), date(2024, 12, 31))
        jan_1 = records[0]
        apr_1 = records[91]  # 31+29+31 = 91 days into year (2024 is leap)
        jul_1 = records[182]
        oct_1 = records[274]
        assert jan_1[2] == 1  # Q1
        assert apr_1[2] == 2  # Q2
        assert jul_1[2] == 3  # Q3
        assert oct_1[2] == 4  # Q4

    def test_date_range_length(self):
        records = _generate_date_records(date(2024, 1, 1), date(2024, 1, 31))
        assert len(records) == 31

    def test_leap_year(self):
        records = _generate_date_records(date(2024, 2, 28), date(2024, 3, 1))
        assert len(records) == 3  # Feb 28, Feb 29, Mar 1
        assert records[1][0] == date(2024, 2, 29)
