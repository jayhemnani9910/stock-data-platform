from db_utils import get_db_connection, upsert_stock_prices


def populate_fact_stock_price():
    """Seed fact_stock_price_daily with one sample row per ticker for testing."""
    sample_data = [
        ("AAPL", "2024-01-02", 150.00, 155.00, 149.00, 154.00, 100000000),
        ("AMZN", "2024-01-02", 152.00, 156.00, 151.00, 155.50, 55000000),
        ("DIS", "2024-01-02", 90.00, 92.00, 89.50, 91.00, 12000000),
        ("GOOG", "2024-01-02", 140.00, 143.00, 139.00, 142.00, 25000000),
        ("JPM", "2024-01-02", 170.00, 173.00, 169.00, 172.00, 10000000),
        ("META", "2024-01-02", 350.00, 358.00, 348.00, 355.00, 18000000),
        ("MSFT", "2024-01-02", 375.00, 380.00, 373.00, 378.00, 22000000),
        ("NFLX", "2024-01-02", 485.00, 492.00, 483.00, 490.00, 8000000),
        ("NVDA", "2024-01-02", 480.00, 495.00, 478.00, 492.00, 45000000),
        ("TSLA", "2024-01-02", 248.00, 255.00, 246.00, 252.00, 30000000),
    ]

    rows = []
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT ticker, company_key FROM dim_company WHERE is_current=TRUE")
            company_map = {ticker: key for ticker, key in cur.fetchall()}
        for ticker, date, open_, high, low, close, volume in sample_data:
            company_key = company_map.get(ticker)
            if not company_key:
                print(f"Skipping: {ticker} not found in dim_company")
                continue
            rows.append((date, company_key, open_, high, low, close, volume))

        if rows:
            upsert_stock_prices(conn, rows)

    print(f"fact_stock_price_daily seeded with {len(rows)} sample rows.")
