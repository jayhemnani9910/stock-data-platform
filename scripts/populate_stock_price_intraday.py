import os
from zoneinfo import ZoneInfo

import yfinance as yf
from db_utils import (
    UPSERT_STOCK_PRICE_INTRADAY_SQL,
    batch_insert,
    get_company_key,
    get_db_connection,
)

TICKERS_FILE = os.environ.get("TICKERS_FILE", "/opt/airflow/dags/tickers.txt")

ET = ZoneInfo("America/New_York")

# Yahoo counts the intraday limit in trading days, not calendar days, so
# period="730d" reaches back about 1,065 calendar days -- roughly 2.9 years,
# half a year more than "2 years" suggests. Every larger period ("1000d",
# "5y", "10y") returns nothing at all, and period="max" is worse still: it
# yields only 730 *calendar* days. Asking for an older window explicitly does
# not work either; Yahoo rejects it with "The requested range must be within
# the last 730 days". This value is the most history obtainable.
BAR_INTERVAL = "1h"
PERIOD = "730d"


def _to_rows(history, company_key, bar_interval=BAR_INTERVAL):
    """Turn a yfinance intraday frame into upsert rows.

    Timestamps arrive tz-aware in US Eastern; trade_date is that session's
    calendar date in the same zone, which is what dim_date is keyed on.

    Bars with no volume are dropped, matching the daily ETL. They are the
    same artifact: a printed price with nothing traded behind it.
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
            )
        )
    return rows


def populate_stock_price_intraday():
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
                history = yf.Ticker(ticker).history(period=PERIOD, interval=BAR_INTERVAL)
                if history.empty:
                    print(f"  {ticker}: no intraday data returned")
                    continue
                rows = _to_rows(history, company_key)
                if rows:
                    # Per ticker, not one batch at the end: ten tickers of
                    # hourly bars is ~50k rows, and a single failure late in
                    # the loop should not discard the nine that worked.
                    batch_insert(conn, UPSERT_STOCK_PRICE_INTRADAY_SQL, rows)
                    total += len(rows)
                print(f"  {ticker}: {len(rows)} {BAR_INTERVAL} bars")
            except Exception as e:
                print(f"Error fetching intraday for {ticker}: {e}")

    print(f"Intraday prices updated: {total} {BAR_INTERVAL} bars across {len(tickers)} tickers")
