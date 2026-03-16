import os
import pandas as pd
import yfinance as yf
from db_utils import get_db_connection, get_company_key, batch_insert, UPSERT_EARNINGS_SQL

TICKERS_FILE = os.environ.get("TICKERS_FILE", "/opt/airflow/dags/tickers.txt")


def populate_earnings():
    with open(TICKERS_FILE, 'r') as f:
        tickers = [line.strip() for line in f if line.strip()]

    rows = []

    with get_db_connection() as conn:
        for ticker in tickers:
            company_key = get_company_key(conn, ticker)
            if not company_key:
                print(f"Skipping {ticker}: not in dim_company")
                continue
            try:
                df = yf.Ticker(ticker).get_earnings_dates(limit=12)
                if df is None or df.empty:
                    continue
                for report_date, row in df.iterrows():
                    eps_actual = row.get('Reported EPS')
                    if pd.isna(eps_actual):
                        continue
                    rows.append((
                        report_date.date(),
                        company_key,
                        row.get('EPS Estimate') if not pd.isna(row.get('EPS Estimate')) else None,
                        float(eps_actual),
                        row.get('Surprise(%)') if not pd.isna(row.get('Surprise(%)')) else None,
                    ))
            except Exception as e:
                print(f"Error fetching earnings for {ticker}: {e}")

        if rows:
            batch_insert(conn, UPSERT_EARNINGS_SQL, rows)

    print(f"Earnings updated: {len(rows)} records across {len(tickers)} tickers")
