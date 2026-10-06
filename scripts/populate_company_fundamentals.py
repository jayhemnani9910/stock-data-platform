import os
from datetime import date

import yfinance as yf
from db_utils import (
    UPSERT_FUNDAMENTALS_SQL,
    batch_insert,
    get_company_key,
    get_db_connection,
)

TICKERS_FILE = os.environ.get("TICKERS_FILE", "/opt/airflow/dags/tickers.txt")

INFO_FIELDS = [
    "marketCap",
    "trailingPE",
    "forwardPE",
    "priceToBook",
    "dividendRate",
    "dividendYield",
    "beta",
    "fiftyTwoWeekHigh",
    "fiftyTwoWeekLow",
    "fullTimeEmployees",
    "longBusinessSummary",
]


def populate_company_fundamentals():
    with open(TICKERS_FILE) as f:
        tickers = [line.strip() for line in f if line.strip()]

    today = date.today()
    rows = []

    with get_db_connection() as conn:
        for ticker in tickers:
            company_key = get_company_key(conn, ticker)
            if not company_key:
                print(f"Skipping {ticker}: not in dim_company")
                continue
            try:
                info = yf.Ticker(ticker).info
                # A rate-limited .info comes back empty. Writing it would make
                # an all-NULL row the newest, and hide the last good one.
                if info.get("marketCap") is None:
                    print(f"Skipping {ticker}: no fundamentals returned")
                    continue
                rows.append((today, company_key, *(info.get(field) for field in INFO_FIELDS)))
            except Exception as e:
                print(f"Error fetching info for {ticker}: {e}")

        if rows:
            batch_insert(conn, UPSERT_FUNDAMENTALS_SQL, rows)

    print(f"Company fundamentals updated: {len(rows)} tickers for {today}")
