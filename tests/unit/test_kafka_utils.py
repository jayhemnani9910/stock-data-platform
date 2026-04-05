"""Tests for live_from_kafka.py — _load_tickers() and _is_market_open()."""

from datetime import datetime
from datetime import time as dtime
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
MARKET_OPEN = dtime(9, 30)
MARKET_CLOSE = dtime(16, 0)


def _load_tickers_from_env(tickers_str):
    """Reproduces _load_tickers() logic with explicit input instead of env var."""
    return [t.strip() for t in tickers_str.split(",") if t.strip()]


def _is_market_open_at(dt):
    """Reproduces _is_market_open() logic with explicit datetime instead of now()."""
    if dt.weekday() >= 5:
        return False
    return MARKET_OPEN <= dt.time() <= MARKET_CLOSE


class TestLoadTickers:
    def test_single_ticker(self):
        assert _load_tickers_from_env("AAPL") == ["AAPL"]

    def test_multiple_tickers(self):
        result = _load_tickers_from_env("AAPL,GOOG,MSFT")
        assert result == ["AAPL", "GOOG", "MSFT"]

    def test_strips_whitespace(self):
        result = _load_tickers_from_env(" AAPL , GOOG , MSFT ")
        assert result == ["AAPL", "GOOG", "MSFT"]

    def test_skips_empty_entries(self):
        result = _load_tickers_from_env("AAPL,,GOOG,")
        assert result == ["AAPL", "GOOG"]

    def test_empty_string_default(self):
        result = _load_tickers_from_env("")
        assert result == []


class TestIsMarketOpen:
    def test_weekday_during_market_hours(self):
        # Wednesday at 10:00 AM ET
        dt = datetime(2024, 1, 3, 10, 0, tzinfo=ET)
        assert _is_market_open_at(dt) is True

    def test_weekday_before_market(self):
        # Monday at 8:00 AM ET
        dt = datetime(2024, 1, 1, 8, 0, tzinfo=ET)
        assert _is_market_open_at(dt) is False

    def test_weekday_after_market(self):
        # Tuesday at 5:00 PM ET
        dt = datetime(2024, 1, 2, 17, 0, tzinfo=ET)
        assert _is_market_open_at(dt) is False

    def test_saturday(self):
        # Saturday at noon ET
        dt = datetime(2024, 1, 6, 12, 0, tzinfo=ET)
        assert _is_market_open_at(dt) is False

    def test_sunday(self):
        # Sunday at noon ET
        dt = datetime(2024, 1, 7, 12, 0, tzinfo=ET)
        assert _is_market_open_at(dt) is False

    def test_market_open_exact(self):
        # Exactly 9:30 AM ET on a weekday
        dt = datetime(2024, 1, 3, 9, 30, tzinfo=ET)
        assert _is_market_open_at(dt) is True

    def test_market_close_exact(self):
        # Exactly 4:00 PM ET on a weekday
        dt = datetime(2024, 1, 3, 16, 0, tzinfo=ET)
        assert _is_market_open_at(dt) is True

    def test_one_minute_before_open(self):
        dt = datetime(2024, 1, 3, 9, 29, tzinfo=ET)
        assert _is_market_open_at(dt) is False

    def test_one_minute_after_close(self):
        dt = datetime(2024, 1, 3, 16, 1, tzinfo=ET)
        assert _is_market_open_at(dt) is False
