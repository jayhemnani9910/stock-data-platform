"""Tests for scripts/populate_sec_financials.py — _parse_period() and _dedupe()."""

from datetime import date

from populate_sec_financials import INSTANT, _dedupe, _parse_period


class TestParsePeriod:
    """A filing reports one line item over several windows that share an end
    date. Keeping only the end date collapsed them, so the loader stored the
    year-to-date figure as if it were the quarter. Both dates come back now."""

    def test_duration_returns_both_dates(self):
        assert _parse_period("duration_2026-03-29_2026-06-27") == (
            date(2026, 3, 29),
            date(2026, 6, 27),
            None,
        )

    def test_quarter_and_ytd_do_not_collapse(self):
        """The regression. Apple's Q3 FY2026 10-Q carries both of these."""
        quarter = _parse_period("duration_2026-03-29_2026-06-27")
        nine_months = _parse_period("duration_2025-09-28_2026-06-27")
        assert quarter[1] == nine_months[1], "same end date, as in the filing"
        assert quarter != nine_months, "but they must not be the same key"

    def test_instant_start_equals_end(self):
        assert _parse_period("instant_2025-09-27") == (
            date(2025, 9, 27),
            date(2025, 9, 27),
            INSTANT,
        )

    def test_instant_is_typed_as_instant(self):
        assert _parse_period("instant_2025-09-27")[2] == INSTANT

    def test_duration_type_is_left_to_the_filing(self):
        """period_type comes from the filing's own metadata, not a day count,
        because 52/53-week fiscal calendars make day counts unreliable."""
        assert _parse_period("duration_2024-09-29_2025-09-27")[2] is None

    def test_unparseable_key_returns_none(self):
        assert _parse_period("no_date_here") is None

    def test_empty_string_returns_none(self):
        assert _parse_period("") is None

    def test_bare_date_is_rejected(self):
        """The old regex matched anything ending in a date, so a key it did not
        understand was still keyed on its trailing date. Strict now: a key that
        is not a period is dropped rather than guessed at."""
        assert _parse_period("2024-01-15") is None

    def test_unknown_prefix_is_rejected(self):
        assert _parse_period("prefix_2023-01-01_2024-06-30") is None

    def test_returns_dates_not_strings(self):
        start, end, _ = _parse_period("duration_2024-01-01_2024-03-31")
        assert isinstance(start, date) and isinstance(end, date)


def _row(period_start, period_end, value, line_item="Net sales"):
    return (
        1,
        "income",
        line_item,
        period_start,
        period_end,
        "Quarterly",
        2026,
        "Q3",
        None,
        None,
        date(2026, 7, 31),
        "10-Q",
        value,
    )


class TestDedupe:
    """execute_values sends the batch as one INSERT, and Postgres rejects a
    statement that hits the conflict target twice. The dedup key has to match
    the table's primary key."""

    def test_keeps_rows_that_differ_only_by_period_start(self):
        rows = [
            _row(date(2026, 3, 29), date(2026, 6, 27), 109_417_000_000),
            _row(date(2025, 9, 28), date(2026, 6, 27), 364_357_000_000),
        ]
        assert len(_dedupe(rows)) == 2

    def test_collapses_a_genuine_duplicate(self):
        rows = [
            _row(date(2026, 3, 29), date(2026, 6, 27), 1.0),
            _row(date(2026, 3, 29), date(2026, 6, 27), 2.0),
        ]
        deduped = _dedupe(rows)
        assert len(deduped) == 1
        assert deduped[0][-1] == 2.0, "the later row wins"

    def test_keeps_distinct_line_items(self):
        rows = [
            _row(date(2026, 3, 29), date(2026, 6, 27), 1.0, "Net sales"),
            _row(date(2026, 3, 29), date(2026, 6, 27), 2.0, "Net income"),
        ]
        assert len(_dedupe(rows)) == 2

    def test_empty_input(self):
        assert _dedupe([]) == []
