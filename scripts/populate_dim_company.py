import os

from db_utils import batch_insert, get_db_connection

TICKERS_FILE = os.environ.get("TICKERS_FILE", "/opt/airflow/dags/tickers.txt")

# Correcting metadata in place keeps company_key stable. A true SCD Type 2 version
# bump would mint a new company_key, and since every loader resolves a ticker through
# get_company_key (which filters is_current), the ticker's existing facts would stay
# on the retired key while new rows landed on the new one, splitting its history.
UPSERT_COMPANY_SQL = """
    INSERT INTO dim_company (ticker, company_name, sector, industry, exchange)
    VALUES %s
    ON CONFLICT (ticker) WHERE is_current DO UPDATE
    SET company_name = EXCLUDED.company_name,
        sector = EXCLUDED.sector,
        industry = EXCLUDED.industry,
        exchange = EXCLUDED.exchange
"""

COMPANY_METADATA = {
    "AAPL": ("Apple Inc.", "Technology", "Consumer Electronics", "NASDAQ"),
    "AMZN": ("Amazon.com Inc.", "Consumer Cyclical", "Internet Retail", "NASDAQ"),
    "DIS": (
        "The Walt Disney Company",
        "Communication Services",
        "Entertainment",
        "NYSE",
    ),
    "GOOG": (
        "Alphabet Inc.",
        "Communication Services",
        "Internet Content & Information",
        "NASDAQ",
    ),
    "JPM": ("JPMorgan Chase & Co.", "Financial Services", "Banks - Diversified", "NYSE"),
    "META": (
        "Meta Platforms",
        "Communication Services",
        "Internet Content & Information",
        "NASDAQ",
    ),
    "MSFT": ("Microsoft Corporation", "Technology", "Software", "NASDAQ"),
    "NFLX": ("Netflix Inc.", "Communication Services", "Entertainment", "NASDAQ"),
    "NVDA": ("NVIDIA Corporation", "Technology", "Semiconductors", "NASDAQ"),
    "TSLA": ("Tesla Inc.", "Consumer Cyclical", "Auto Manufacturers", "NASDAQ"),
}


def populate_dim_company():
    with open(TICKERS_FILE) as f:
        tickers = [line.strip() for line in f if line.strip()]

    companies = []
    for ticker in tickers:
        meta = COMPANY_METADATA.get(ticker)
        if not meta:
            print(f"Warning: no metadata for {ticker}, skipping")
            continue
        companies.append((ticker, *meta))

    with get_db_connection() as conn:
        batch_insert(conn, UPSERT_COMPANY_SQL, companies)

    print(f"dim_company populated with {len(companies)} companies.")
