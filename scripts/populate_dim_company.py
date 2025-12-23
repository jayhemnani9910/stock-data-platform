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
        {"ticker": "AAPL", "name": "Apple Inc.", "sector": "Technology", "industry": "Consumer Electronics", "exchange": "NASDAQ"},
        {"ticker": "GOOG", "name": "Alphabet Inc.", "sector": "Technology", "industry": "Internet Content", "exchange": "NASDAQ"},
        {"ticker": "JPM", "name": "JPMorgan Chase & Co.", "sector": "Finance", "industry": "Banking", "exchange": "NYSE"},
        {"ticker": "META", "name": "Meta Platforms", "sector": "Technology", "industry": "Social Media", "exchange": "NASDAQ"},
    ]

    for c in companies:
        cur.execute("""
            INSERT INTO dim_company (ticker, name, sector, industry, exchange)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (ticker) DO NOTHING;
        """, (c["ticker"], c["name"], c["sector"], c["industry"], c["exchange"]))

    conn.commit()
    cur.close()
    conn.close()
    print("✅ dim_company populated.")
