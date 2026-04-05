import os
from db_utils import get_db_connection, batch_insert

TICKERS_FILE = os.environ.get("TICKERS_FILE", "/opt/airflow/dags/tickers.txt")

COMPANY_METADATA = {
    "AAPL": ("Apple Inc.", "Technology", "Consumer Electronics", "NASDAQ"),
    "AMZN": ("Amazon.com Inc.", "Consumer Cyclical", "Internet Retail", "NASDAQ"),
    "DIS": (
        "The Walt Disney Company",
        "Communication Services",
        "Entertainment",
        "NYSE",
    ),
    "GOOG": ("Alphabet Inc.", "Technology", "Internet Content", "NASDAQ"),
    "JPM": ("JPMorgan Chase & Co.", "Finance", "Banking", "NYSE"),
    "META": ("Meta Platforms", "Technology", "Social Media", "NASDAQ"),
    "MSFT": ("Microsoft Corporation", "Technology", "Software", "NASDAQ"),
    "NFLX": ("Netflix Inc.", "Communication Services", "Entertainment", "NASDAQ"),
    "NVDA": ("NVIDIA Corporation", "Technology", "Semiconductors", "NASDAQ"),
    "TSLA": ("Tesla Inc.", "Consumer Cyclical", "Auto Manufacturers", "NASDAQ"),
}


def populate_dim_company():
    with open(TICKERS_FILE, "r") as f:
        tickers = [line.strip() for line in f if line.strip()]

    companies = []
    for ticker in tickers:
        meta = COMPANY_METADATA.get(ticker)
        if not meta:
            print(f"Warning: no metadata for {ticker}, skipping")
            continue
        companies.append((ticker, *meta))

    with get_db_connection() as conn:
        batch_insert(
            conn,
            "INSERT INTO dim_company (ticker, company_name, sector, industry, exchange) "
            "VALUES %s ON CONFLICT (ticker) DO NOTHING",
            companies,
        )

    print(f"dim_company populated with {len(companies)} companies.")
