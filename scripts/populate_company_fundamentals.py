import os
from datetime import date
import yfinance as yf
from db_utils import (
    get_db_connection,
    get_company_key,
    batch_insert,
    UPSERT_FUNDAMENTALS_SQL,
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
    with open(TICKERS_FILE, "r") as f:
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
