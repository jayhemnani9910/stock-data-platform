import os
from datetime import UTC, date, datetime, time, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from db_utils import (
    UPSERT_STOCK_PRICE_INTRADAY_SQL,
    batch_insert,
    get_company_key,
    get_db_connection,
)

TICKERS_FILE = os.environ.get("TICKERS_FILE", "/opt/airflow/dags/tickers.txt")

# After a successful Alpaca load, the ticker's bars from any other provider go.
# A volume first loaded by the old Yahoo hourly loader still holds those rows,
# and they are wrong for every dividend payer (see "No fallback" below); Alpaca
# covers their whole window, so this replaces rather than loses anything.
DELETE_OTHER_SOURCES_SQL = """
    DELETE FROM fact_stock_price_intraday
    WHERE company_key = %s AND bar_interval = %s AND source <> %s
"""

ET = ZoneInfo("America/New_York")
BAR_INTERVAL = "1h"
SESSION_OPEN = time(9, 30)
SESSION_CLOSE = time(16, 0)

# --- Alpaca: the primary source ------------------------------------------------
#
# A free paper-trading account serves bars back to 2016-01-01 -- about 10.7
# years, against the ~2.9 Yahoo allows at 1h. Historical SIP data (every US
# exchange, not just IEX) is free provided the requested window ends at least
# 15 minutes ago; ask for anything newer and it answers 403 "subscription does
# not permit querying recent SIP data". ALPACA_END_LAG keeps clear of that.
ALPACA_URL = "https://data.alpaca.markets/v2/stocks/{symbol}/bars"
ALPACA_START = "2016-01-01T00:00:00Z"
ALPACA_END_LAG = timedelta(minutes=20)
ALPACA_PAGE = 10000

# Alpaca's native 1Hour bars are the wrong shape for this table: they start on
# the clock (10:00, 11:00, ...) and include pre- and post-market trading, and
# the 09:00 bar blends pre-market trades with the opening bell. 30-minute bars
# split cleanly at 09:30, so they are fetched instead and folded into
# :30-anchored regular-session hours -- 09:30, 10:30, ... 15:30 -- which is how
# Yahoo cuts them. Checked against Yahoo on AAPL 2026-09-09: closes agree to the
# cent, highs and lows match, volumes within ~1%.
ALPACA_TIMEFRAME = "30Min"

# --- No fallback -----------------------------------------------------------------
#
# Yahoo used to fill in when the keys were missing, and must not again. Its
# hourly prices are split-adjusted but NOT dividend-adjusted, unlike its daily
# ones -- so for every dividend payer the hourly bars disagreed with
# fact_stock_price_daily, and by more the further back they went: in 2023 AAPL
# was 1.2% high, MSFT 2.1%, DIS 2.8%, JPM 5.4%. Only AMZN, NFLX and TSLA, which
# pay nothing, lined up. Writing that on top of dividend-adjusted Alpaca
# history would also put a fake step at the seam. Without keys the refresh is
# skipped and the stored bars stay exactly as they are.


def _alpaca_credentials():
    key = os.environ.get("ALPACA_API_KEY", "").strip()
    secret = os.environ.get("ALPACA_API_SECRET", "").strip()
    return (key, secret) if key and secret else None


def _fetch_alpaca_bars(symbol, credentials, now=None, session=requests):
    """Every 30-minute bar for one symbol from ALPACA_START to a safe end.

    adjustment=all so prices are split- and dividend-adjusted like the daily
    series; reloading the whole window each run keeps one adjustment basis
    across it, which is the lesson from the daily ETL's step at 2026-03-12.
    """
    key, secret = credentials
    now = now or datetime.now(UTC)
    params = {
        "timeframe": ALPACA_TIMEFRAME,
        "start": ALPACA_START,
        "end": (now - ALPACA_END_LAG).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "adjustment": "all",
        "feed": "sip",
        "limit": ALPACA_PAGE,
    }
    headers = {"APCA-API-KEY-ID": key, "APCA-API-SECRET-KEY": secret}
    bars, token = [], None
    while True:
        if token:
            params["page_token"] = token
        r = session.get(ALPACA_URL.format(symbol=symbol), headers=headers, params=params, timeout=60)
        if r.status_code != 200:
            raise RuntimeError(f"Alpaca {symbol}: HTTP {r.status_code}: {r.text[:200]}")
        body = r.json()
        bars.extend(body.get("bars") or [])
        token = body.get("next_page_token")
        if not token:
            return bars


def _fold_to_session_hours(bars):
    """Fold 30-minute bars into :30-anchored regular-session hours.

    Drops everything outside 09:30-16:00 ET, then groups 09:30+10:00 into the
    09:30 hour, 10:30+11:00 into 10:30, and so on. The last hour, 15:30, is a
    single half-hour -- the same as Yahoo's. Early-close sessions simply end
    sooner. Returns a frame indexed by the hour's start time in ET.
    """
    if not bars:
        return pd.DataFrame(columns=["Open", "High", "Low", "Close", "Volume"])
    df = pd.DataFrame(bars)
    df["t"] = pd.to_datetime(df["t"], utc=True).dt.tz_convert(ET)
    clock = df["t"].dt.time
    df = df[(clock >= SESSION_OPEN) & (clock < SESSION_CLOSE)].sort_values("t")
    if df.empty:
        return pd.DataFrame(columns=["Open", "High", "Low", "Close", "Volume"])
    # Shift back half an hour, floor to the hour, shift forward again: 09:30
    # and 10:00 both land on 09:30; 10:30 and 11:00 on 10:30.
    df["hour"] = (df["t"] - pd.Timedelta(minutes=30)).dt.floor("h") + pd.Timedelta(minutes=30)
    return df.groupby("hour").agg(
        Open=("o", "first"),
        High=("h", "max"),
        Low=("l", "min"),
        Close=("c", "last"),
        Volume=("v", "sum"),
    )


def _to_rows(history, company_key, source, bar_interval=BAR_INTERVAL):
    """Turn an hourly frame (indexed by tz-aware start time) into upsert rows.

    trade_date is the session's calendar date in US Eastern, which is what
    dim_date is keyed on. Bars with no volume are dropped, matching the daily
    ETL: a printed price with nothing traded behind it.
    """
    rows = []
    for ts, bar in history.iterrows():
        volume = int(bar["Volume"])
        if volume <= 0:
            continue
        moment = ts.to_pydatetime()
        rows.append(
            (
                moment,
                company_key,
                bar_interval,
                moment.astimezone(ET).date(),
                float(bar["Open"]),
                float(bar["High"]),
                float(bar["Low"]),
                float(bar["Close"]),
                volume,
                source,
            )
        )
    return rows


def populate_stock_price_intraday():
    """Reload every hourly bar from Alpaca. Returns False if it did nothing.

    False means the keys are missing, and the caller should surface that as a
    skip rather than a success -- a green run that wrote nothing is how stale
    data goes unnoticed.
    """
    credentials = _alpaca_credentials()
    if not credentials:
        print(
            "WARNING: ALPACA_API_KEY / ALPACA_API_SECRET not set -- hourly refresh skipped. "
            "Stored bars are untouched. Add the free paper-trading keys to .env to resume."
        )
        return False

    with open(TICKERS_FILE) as f:
        tickers = [line.strip() for line in f if line.strip()]

    total = 0
    with get_db_connection() as conn:
        for ticker in tickers:
            company_key = get_company_key(conn, ticker)
            if not company_key:
                print(f"Skipping {ticker}: not in dim_company")
                continue
            try:
                history, source = _fold_to_session_hours(_fetch_alpaca_bars(ticker, credentials)), "alpaca"
                if history.empty:
                    print(f"  {ticker}: no intraday data returned")
                    continue
                rows = _to_rows(history, company_key, source)
                if rows:
                    # Per ticker, not one batch at the end: a failure late in
                    # the loop should not discard the tickers that worked.
                    batch_insert(conn, UPSERT_STOCK_PRICE_INTRADAY_SQL, rows)
                    total += len(rows)
                    if source == "alpaca":
                        with conn.cursor() as cur:
                            cur.execute(DELETE_OTHER_SOURCES_SQL, (company_key, BAR_INTERVAL, source))
                            if cur.rowcount:
                                print(f"  {ticker}: replaced {cur.rowcount} bars from other sources")
                        conn.commit()
                first = min(r[3] for r in rows) if rows else date.today()
                print(f"  {ticker}: {len(rows)} {BAR_INTERVAL} bars from {source}, since {first}")
            except Exception as e:
                print(f"Error fetching intraday for {ticker}: {e}")

    print(f"Intraday prices updated: {total} {BAR_INTERVAL} bars across {len(tickers)} tickers")
    return True
