"""Tests for live_from_kafka.py — _load_tickers(), _is_market_open(), _day_bar_from_intraday().

These used to run against local copies, so the real MARKET_OPEN could be moved
to 03:30 without a single failure. They import the module now.
"""

from datetime import datetime
from datetime import time as dtime

import pandas as pd
import pytest

pytest.importorskip("kafka", reason="kafka-python not installed")
pytest.importorskip("yfinance", reason="yfinance not installed")

import live_from_kafka as lfk


class TestLoadTickers:
    def test_single_ticker(self, monkeypatch):
        monkeypatch.setenv("STOCK_TICKERS", "AAPL")
        assert lfk._load_tickers() == ["AAPL"]

    def test_multiple_tickers(self, monkeypatch):
        monkeypatch.setenv("STOCK_TICKERS", "AAPL,MSFT,GOOG")
        assert lfk._load_tickers() == ["AAPL", "MSFT", "GOOG"]

    def test_strips_whitespace(self, monkeypatch):
        monkeypatch.setenv("STOCK_TICKERS", " AAPL , MSFT ")
        assert lfk._load_tickers() == ["AAPL", "MSFT"]

    def test_skips_empty_entries(self, monkeypatch):
        monkeypatch.setenv("STOCK_TICKERS", "AAPL,,MSFT,")
        assert lfk._load_tickers() == ["AAPL", "MSFT"]

    def test_empty_string_yields_nothing(self, monkeypatch):
        """An explicitly empty var is not the same as an unset one."""
        monkeypatch.setenv("STOCK_TICKERS", "")
        assert lfk._load_tickers() == []

    def test_unset_falls_back_to_aapl(self, monkeypatch):
        """The real default. The old test was named test_empty_string_default
        and asserted [], which is what its local copy did — the production
        fallback to AAPL was never covered."""
        monkeypatch.delenv("STOCK_TICKERS", raising=False)
        assert lfk._load_tickers() == ["AAPL"]


class TestIsMarketOpen:
    def _at(self, y, m, d, hh, mm):
        return datetime(y, m, d, hh, mm, tzinfo=lfk.ET)

    def test_weekday_during_market_hours(self):
        assert lfk._is_market_open(self._at(2024, 1, 3, 12, 0)) is True

    def test_weekday_before_market(self):
        assert lfk._is_market_open(self._at(2024, 1, 1, 8, 0)) is False

    def test_weekday_after_market(self):
        assert lfk._is_market_open(self._at(2024, 1, 3, 17, 0)) is False

    def test_saturday(self):
        assert lfk._is_market_open(self._at(2024, 1, 6, 12, 0)) is False

    def test_sunday(self):
        assert lfk._is_market_open(self._at(2024, 1, 7, 12, 0)) is False

    def test_market_open_exact(self):
        assert lfk._is_market_open(self._at(2024, 1, 3, 9, 30)) is True

    def test_market_close_exact(self):
        assert lfk._is_market_open(self._at(2024, 1, 3, 16, 0)) is True

    def test_one_minute_before_open(self):
        assert lfk._is_market_open(self._at(2024, 1, 3, 9, 29)) is False

    def test_one_minute_after_close(self):
        assert lfk._is_market_open(self._at(2024, 1, 3, 16, 1)) is False

    def test_constants_are_the_regular_session(self):
        """Pins the real constants. Moving them silently was possible before."""
        assert lfk.MARKET_OPEN == dtime(9, 30)
        assert lfk.MARKET_CLOSE == dtime(16, 0)


def _frame(rows):
    """rows: list of (minute, open, high, low, close, volume)."""
    idx = pd.to_datetime([f"2026-09-09 {t}" for t, *_ in rows])
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


class TestDayBarFromIntraday:
    """The payload lands in fact_stock_price_daily, so it has to describe the
    day. Publishing .tail(1) instead meant 97.1% of the messages in the topic
    carried volume 0 and a zero-width range."""

    def test_open_is_the_first_bar(self):
        bar = _frame([("09:30", 100, 101, 99, 100.5, 500), ("09:31", 100.5, 103, 100, 102, 700)])
        assert lfk._day_bar_from_intraday(bar)["open"] == 100

    def test_close_is_the_last_bar(self):
        bar = _frame([("09:30", 100, 101, 99, 100.5, 500), ("09:31", 100.5, 103, 100, 102, 700)])
        assert lfk._day_bar_from_intraday(bar)["close"] == 102

    def test_high_and_low_span_the_whole_day(self):
        bar = _frame(
            [
                ("09:30", 100, 101, 99, 100.5, 500),
                ("09:31", 100.5, 108, 97, 102, 700),
                ("09:32", 102, 103, 101, 102.5, 300),
            ]
        )
        result = lfk._day_bar_from_intraday(bar)
        assert result["high"] == 108
        assert result["low"] == 97

    def test_volume_is_the_days_total(self):
        bar = _frame([("09:30", 100, 101, 99, 100.5, 500), ("09:31", 100.5, 103, 100, 102, 700)])
        assert lfk._day_bar_from_intraday(bar)["volume"] == 1200

    def test_trailing_partial_bar_does_not_become_the_whole_day(self):
        """The regression: yfinance's last intraday row is an in-progress
        minute with volume 0 and open == high == low == close. Taking it alone
        gave a daily row with a real volume of 35 million recorded as 0."""
        bar = _frame(
            [
                ("09:30", 100, 110, 95, 105, 40_000_000),
                ("09:31", 105, 105, 105, 105, 0),
            ]
        )
        result = lfk._day_bar_from_intraday(bar)
        assert result["volume"] == 40_000_000
        assert result["high"] == 110
        assert result["low"] == 95
        assert result["open"] == 100

    def test_date_comes_from_the_frame(self):
        bar = _frame([("09:30", 100, 101, 99, 100.5, 500)])
        assert lfk._day_bar_from_intraday(bar)["date"] == "2026-09-09"

    def test_volume_is_an_int(self):
        bar = _frame([("09:30", 100, 101, 99, 100.5, 500)])
        assert isinstance(lfk._day_bar_from_intraday(bar)["volume"], int)

    def test_empty_frame_is_dropped(self):
        assert lfk._day_bar_from_intraday(_frame([])) is None

    def test_none_is_dropped(self):
        assert lfk._day_bar_from_intraday(None) is None

    def test_zero_volume_day_is_dropped(self):
        """Pre-open, every bar is empty. Publishing that would create a daily
        row with no trades in it."""
        bar = _frame([("09:30", 100, 100, 100, 100, 0), ("09:31", 100, 100, 100, 100, 0)])
        assert lfk._day_bar_from_intraday(bar) is None
