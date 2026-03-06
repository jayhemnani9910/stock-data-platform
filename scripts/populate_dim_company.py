import psycopg2

def populate_dim_company():
    conn = psycopg2.connect(
        host="timescaledb",
        dbname="stockdw",
        user="data226",
        password="12345678",
        port=5432
    )
    cur = conn.cursor()

    companies = [
        {"ticker": "AAPL", "company_name": "Apple Inc.", "sector": "Technology", "industry": "Consumer Electronics", "exchange": "NASDAQ"},
        {"ticker": "GOOG", "company_name": "Alphabet Inc.", "sector": "Technology", "industry": "Internet Content", "exchange": "NASDAQ"},
        {"ticker": "JPM", "company_name": "JPMorgan Chase & Co.", "sector": "Finance", "industry": "Banking", "exchange": "NYSE"},
        {"ticker": "META", "company_name": "Meta Platforms", "sector": "Technology", "industry": "Social Media", "exchange": "NASDAQ"},
    ]

    for c in companies:
        cur.execute("""
            INSERT INTO dim_company (ticker, company_name, sector, industry, exchange, is_current, effective_date)
            VALUES (%s, %s, %s, %s, %s, TRUE, CURRENT_DATE)
            ON CONFLICT (ticker) DO NOTHING;
        """, (c["ticker"], c["company_name"], c["sector"], c["industry"], c["exchange"]))

    conn.commit()
    cur.close()
    conn.close()
    print("✅ dim_company populated.")
