from datetime import date

import yfinance as yf
from db_utils import (
    UPSERT_FUNDAMENTALS_SQL,
    batch_insert,
    get_company_key,
    get_db_connection,
)
from tickers import load_tickers

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
    tickers = load_tickers()

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
                rows.append(
                    (
                        today,
                        company_key,
                        info.get("marketCap"),
                        info.get("trailingPE"),
                        info.get("forwardPE"),
                        info.get("priceToBook"),
                        info.get("dividendRate"),
                        info.get("dividendYield"),
                        info.get("beta"),
                        info.get("fiftyTwoWeekHigh"),
                        info.get("fiftyTwoWeekLow"),
                        info.get("fullTimeEmployees"),
                        info.get("longBusinessSummary"),
                    )
                )
            except Exception as e:
                print(f"Error fetching info for {ticker}: {e}")

        if rows:
            batch_insert(conn, UPSERT_FUNDAMENTALS_SQL, rows)

    print(f"Company fundamentals updated: {len(rows)} tickers for {today}")
