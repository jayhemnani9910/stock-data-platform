"""Tests for scripts/populate_stock_price_intraday.py — _to_rows()."""

from datetime import date
from zoneinfo import ZoneInfo

import pandas as pd
import pytest
from db_utils import UPSERT_STOCK_PRICE_INTRADAY_SQL

pytest.importorskip("yfinance", reason="yfinance not installed")

from populate_stock_price_intraday import BAR_INTERVAL, PERIOD, _to_rows

ET = ZoneInfo("America/New_York")


def _frame(rows):
    """rows: (iso timestamp in ET, open, high, low, close, volume)."""
    idx = pd.DatetimeIndex([pd.Timestamp(t, tz=ET) for t, *_ in rows], name="Datetime")
    return pd.DataFrame(
        {
            "Open": [r[1] for r in rows],
            "High": [r[2] for r in rows],
            "Low": [r[3] for r in rows],
            "Close": [r[4] for r in rows],
            "Volume": [r[5] for r in rows],
        },
        index=idx,
    )


SESSION = _frame(
    [
        ("2026-09-09 09:30:00", 315.93, 318.13, 314.32, 314.75, 7809919),
        ("2026-09-09 15:30:00", 315.55, 315.94, 314.88, 315.41, 4968709),
    ]
)


class TestToRows:
    def test_one_row_per_bar(self):
        assert len(_to_rows(SESSION, 1)) == 2

    def test_column_order_matches_the_insert(self):
        """The tuple is fed positionally into UPSERT_STOCK_PRICE_INTRADAY_SQL."""
        (ts, ck, interval, trade_date, o, h, low, c, v) = _to_rows(SESSION, 7)[0]
        assert ck == 7
        assert interval == BAR_INTERVAL
        assert trade_date == date(2026, 9, 9)
        assert (o, h, low, c, v) == (315.93, 318.13, 314.32, 314.75, 7809919)
        assert ts.tzinfo is not None, "timestamps must stay tz-aware"

    def test_trade_date_is_the_eastern_session_date(self):
        """A 09:30 ET bar is 13:30 UTC. Taking the UTC date would still be the
        9th here, but an after-hours bar would roll to the next day."""
        late = _frame([("2026-09-09 19:30:00", 1.0, 2.0, 0.5, 1.5, 100)])
        assert _to_rows(late, 1)[0][3] == date(2026, 9, 9)

    def test_zero_volume_bars_are_dropped(self):
        """Same rule as the daily ETL: a printed price with nothing traded."""
        mixed = _frame(
            [
                ("2026-09-09 09:30:00", 1.0, 2.0, 0.5, 1.5, 100),
                ("2026-09-09 10:30:00", 1.5, 1.5, 1.5, 1.5, 0),
            ]
        )
        rows = _to_rows(mixed, 1)
        assert len(rows) == 1
        assert rows[0][8] == 100

    def test_volume_is_an_int(self):
        assert isinstance(_to_rows(SESSION, 1)[0][8], int)

    def test_empty_frame(self):
        assert _to_rows(_frame([]), 1) == []

    def test_bar_interval_is_labelled_on_every_row(self):
        """Without it, a later 1m load would be indistinguishable from these."""
        assert {r[2] for r in _to_rows(SESSION, 1)} == {BAR_INTERVAL}

    def test_a_different_interval_can_be_labelled(self):
        rows = _to_rows(SESSION, 1, bar_interval="1m")
        assert {r[2] for r in rows} == {"1m"}


class TestExtractionWindow:
    """Yahoo counts the intraday limit in trading days, so period="730d"
    reaches ~1,065 calendar days. Every larger period returns nothing, and
    "max" returns only 730 calendar days -- less. This is the most obtainable."""

    def test_period_is_the_maximum_that_works(self):
        assert PERIOD == "730d"

    def test_interval_is_hourly(self):
        assert BAR_INTERVAL == "1h"


class TestIntradayTemplate:
    def test_conflict_target_includes_the_interval(self):
        """Hourly and minute bars share a timestamp; only bar_interval
        separates them. Leaving it out would make them overwrite each other."""
        assert "ON CONFLICT (ts, company_key, bar_interval)" in UPSERT_STOCK_PRICE_INTRADAY_SQL

    def test_targets_the_intraday_table_not_the_daily_one(self):
        assert "INSERT INTO fact_stock_price_intraday" in UPSERT_STOCK_PRICE_INTRADAY_SQL
        assert "fact_stock_price_daily" not in UPSERT_STOCK_PRICE_INTRADAY_SQL

    def test_carries_the_session_date(self):
        assert "trade_date" in UPSERT_STOCK_PRICE_INTRADAY_SQL
