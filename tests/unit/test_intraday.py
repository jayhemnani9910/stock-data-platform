"""Tests for scripts/populate_stock_price_intraday.py."""

from datetime import UTC, date, datetime
from zoneinfo import ZoneInfo

import pandas as pd
import populate_stock_price_intraday as psi
import pytest
from db_utils import UPSERT_STOCK_PRICE_INTRADAY_SQL

ET = ZoneInfo("America/New_York")


def _bar(et_clock, o, h, low, c, v, day="2026-09-09"):
    """One Alpaca-shaped 30-minute bar; t is UTC like the API returns."""
    t = pd.Timestamp(f"{day} {et_clock}", tz=ET).tz_convert("UTC")
    return {"t": t.strftime("%Y-%m-%dT%H:%M:%SZ"), "o": o, "h": h, "l": low, "c": c, "v": v}


# The real AAPL 2026-09-09 session, 30-minute bars from Alpaca, plus the
# pre-market and after-hours bars the fold must discard.
SESSION = [
    _bar("08:30", 315.10, 315.40, 315.00, 315.20, 99_000),  # pre-market: dropped
    _bar("09:00", 315.20, 317.90, 315.10, 315.93, 40_000),  # pre-market: dropped
    _bar("09:30", 315.93, 318.13, 314.33, 316.00, 4_000_000),
    _bar("10:00", 316.00, 316.50, 314.30, 314.75, 3_231_960),
    _bar("10:30", 314.71, 314.72, 312.11, 312.90, 2_700_000),
    _bar("11:00", 312.90, 313.20, 312.20, 312.50, 2_609_096),
    _bar("15:30", 315.56, 315.94, 314.88, 315.42, 4_972_953),
    _bar("16:00", 315.42, 316.00, 315.30, 315.94, 7_157_345),  # after-hours: dropped
]


class TestFoldToSessionHours:
    """Alpaca's native 1Hour bars start on the clock and include pre- and
    post-market trading. 30-minute bars are folded into :30-anchored
    regular-session hours instead, the way Yahoo cuts them."""

    def test_hours_are_anchored_at_half_past(self):
        hours = psi._fold_to_session_hours(SESSION)
        assert [t.strftime("%H:%M") for t in hours.index] == ["09:30", "10:30", "15:30"]

    def test_extended_hours_are_dropped(self):
        """The 08:30, 09:00 and 16:00 bars are outside the session."""
        hours = psi._fold_to_session_hours(SESSION)
        assert hours["Volume"].sum() == 4_000_000 + 3_231_960 + 2_700_000 + 2_609_096 + 4_972_953

    def test_open_is_the_first_half_hour_and_close_the_second(self):
        first = psi._fold_to_session_hours(SESSION).iloc[0]
        assert first["Open"] == 315.93
        assert first["Close"] == 314.75

    def test_high_and_low_span_both_half_hours(self):
        first = psi._fold_to_session_hours(SESSION).iloc[0]
        assert first["High"] == 318.13
        assert first["Low"] == 314.30

    def test_volume_is_summed(self):
        first = psi._fold_to_session_hours(SESSION).iloc[0]
        assert first["Volume"] == 4_000_000 + 3_231_960

    def test_the_last_hour_is_a_single_half_hour_like_yahoos(self):
        last = psi._fold_to_session_hours(SESSION).iloc[-1]
        assert last["Close"] == 315.42 and last["Volume"] == 4_972_953

    def test_the_premarket_opening_bell_blend_cannot_leak_in(self):
        """Alpaca's native 09:00 hour mixes pre-market with the open. Folding
        30-minute bars must never let a pre-market price set the 09:30 open."""
        first = psi._fold_to_session_hours(SESSION).iloc[0]
        assert first["Open"] != 315.20

    def test_index_is_eastern(self):
        hours = psi._fold_to_session_hours(SESSION)
        assert str(hours.index.tz) == "America/New_York"

    def test_works_across_the_dst_change(self):
        """Filtering on Eastern wall-clock time, not a UTC offset, keeps the
        session right on both sides of a daylight-saving switch."""
        winter = [_bar("09:30", 1, 2, 0.5, 1.5, 100, day="2026-01-15")]
        summer = [_bar("09:30", 1, 2, 0.5, 1.5, 100, day="2026-07-15")]
        for bars in (winter, summer):
            assert psi._fold_to_session_hours(bars).index[0].strftime("%H:%M") == "09:30"

    def test_empty_input(self):
        assert psi._fold_to_session_hours([]).empty

    def test_only_extended_hours(self):
        assert psi._fold_to_session_hours([SESSION[0], SESSION[-1]]).empty


class TestToRows:
    def test_column_order_matches_the_insert(self):
        hours = psi._fold_to_session_hours(SESSION)
        (ts, ck, interval, trade_date, o, h, low, c, v, source) = psi._to_rows(hours, 7, "alpaca")[0]
        assert (ck, interval, trade_date, source) == (7, "1h", date(2026, 9, 9), "alpaca")
        assert (o, h, low, c, v) == (315.93, 318.13, 314.30, 314.75, 7_231_960)
        assert ts.tzinfo is not None

    def test_every_row_is_labelled_with_its_source(self):
        hours = psi._fold_to_session_hours(SESSION)
        assert {r[9] for r in psi._to_rows(hours, 1, "alpaca")} == {"alpaca"}

    def test_zero_volume_hours_are_dropped(self):
        hours = psi._fold_to_session_hours([_bar("09:30", 1, 1, 1, 1, 0)])
        assert psi._to_rows(hours, 1, "alpaca") == []

    def test_trade_date_is_the_eastern_session_date(self):
        late = psi._fold_to_session_hours([_bar("15:30", 1, 2, 0.5, 1.5, 100)])
        assert psi._to_rows(late, 1, "alpaca")[0][3] == date(2026, 9, 9)


class _Resp:
    def __init__(self, body, status=200):
        self._body, self.status_code, self.text = body, status, str(body)

    def json(self):
        return self._body


class _Session:
    """Records requests and replays canned pages."""

    def __init__(self, pages):
        self.pages, self.calls = list(pages), []

    def get(self, url, headers=None, params=None, timeout=None):
        self.calls.append({"url": url, "headers": dict(headers), "params": dict(params)})
        return self.pages.pop(0)


NOW = datetime(2026, 9, 11, 12, 0, tzinfo=UTC)


class TestFetchAlpacaBars:
    def test_follows_every_page(self):
        s = _Session([_Resp({"bars": [SESSION[2]], "next_page_token": "p2"}), _Resp({"bars": [SESSION[3]]})])
        bars = psi._fetch_alpaca_bars("AAPL", ("k", "s"), now=NOW, session=s)
        assert len(bars) == 2 and len(s.calls) == 2
        assert s.calls[1]["params"]["page_token"] == "p2"

    def test_never_asks_for_the_last_fifteen_minutes(self):
        """The free tier answers 403 for SIP data less than 15 minutes old."""
        s = _Session([_Resp({"bars": []})])
        psi._fetch_alpaca_bars("AAPL", ("k", "s"), now=NOW, session=s)
        end = datetime.strptime(s.calls[0]["params"]["end"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
        assert NOW - end >= psi.ALPACA_END_LAG
        assert psi.ALPACA_END_LAG.total_seconds() > 15 * 60

    def test_asks_for_the_right_shape(self):
        s = _Session([_Resp({"bars": []})])
        psi._fetch_alpaca_bars("AAPL", ("k", "s"), now=NOW, session=s)
        p = s.calls[0]["params"]
        assert p["timeframe"] == "30Min", "native 1Hour bars are cut on the clock"
        assert p["feed"] == "sip", "IEX alone is ~2.5% of US volume"
        assert p["adjustment"] == "all", "must match the daily series' adjustment"
        assert p["start"].startswith("2016-01-01")

    def test_sends_the_credentials_as_headers_not_params(self):
        s = _Session([_Resp({"bars": []})])
        psi._fetch_alpaca_bars("AAPL", ("KEY", "SECRET"), now=NOW, session=s)
        assert s.calls[0]["headers"] == {"APCA-API-KEY-ID": "KEY", "APCA-API-SECRET-KEY": "SECRET"}
        assert "KEY" not in str(s.calls[0]["params"]) and "SECRET" not in str(s.calls[0]["params"])

    def test_an_error_status_raises_instead_of_returning_nothing(self):
        """A silent empty result would look like a ticker with no history."""
        s = _Session([_Resp({"message": "forbidden"}, status=403)])
        with pytest.raises(RuntimeError, match="403"):
            psi._fetch_alpaca_bars("AAPL", ("k", "s"), now=NOW, session=s)


class TestCredentials:
    def test_uses_alpaca_when_both_keys_are_set(self, monkeypatch):
        monkeypatch.setenv("ALPACA_API_KEY", "k")
        monkeypatch.setenv("ALPACA_API_SECRET", "s")
        assert psi._alpaca_credentials() == ("k", "s")

    @pytest.mark.parametrize("key,secret", [("", "s"), ("k", ""), ("", ""), ("  ", "s")])
    def test_missing_or_blank_keys_mean_no_credentials(self, monkeypatch, key, secret):
        monkeypatch.setenv("ALPACA_API_KEY", key)
        monkeypatch.setenv("ALPACA_API_SECRET", secret)
        assert psi._alpaca_credentials() is None


class TestNoFallback:
    """Yahoo's hourly prices are not dividend-adjusted, so they disagree with
    the daily table for every dividend payer -- JPM by 5.4% in 2023. Without
    keys the refresh must write nothing, rather than write that."""

    def test_without_keys_it_writes_nothing_and_says_so(self, monkeypatch):
        monkeypatch.delenv("ALPACA_API_KEY", raising=False)
        monkeypatch.delenv("ALPACA_API_SECRET", raising=False)

        def must_not_run(*a, **k):
            raise AssertionError("nothing may be fetched or written without keys")

        monkeypatch.setattr(psi, "get_db_connection", must_not_run)
        monkeypatch.setattr(psi, "_fetch_alpaca_bars", must_not_run)
        assert psi.populate_stock_price_intraday() is False

    def test_the_loader_no_longer_imports_yahoo(self):
        assert not hasattr(psi, "yf"), "the Yahoo hourly path is gone for a reason"
        assert not hasattr(psi, "YAHOO_PERIOD")


class TestTemplates:
    def test_conflict_target_includes_the_interval(self):
        assert "ON CONFLICT (ts, company_key, bar_interval)" in UPSERT_STOCK_PRICE_INTRADAY_SQL

    def test_source_is_written_and_updated(self):
        assert "source" in UPSERT_STOCK_PRICE_INTRADAY_SQL
        assert "source = EXCLUDED.source" in UPSERT_STOCK_PRICE_INTRADAY_SQL

    def test_targets_the_intraday_table_not_the_daily_one(self):
        assert "INSERT INTO fact_stock_price_intraday" in UPSERT_STOCK_PRICE_INTRADAY_SQL
        assert "fact_stock_price_daily" not in UPSERT_STOCK_PRICE_INTRADAY_SQL

    def test_replacement_only_touches_one_ticker_and_one_interval(self):
        sql = " ".join(psi.DELETE_OTHER_SOURCES_SQL.split())
        assert "company_key = %s" in sql and "bar_interval = %s" in sql and "source <> %s" in sql
