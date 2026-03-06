import psycopg2

def populate_fact_stock_price():
    conn = psycopg2.connect(
        host="timescaledb",
        dbname="stockdw",
        user="data226",
        password="12345678",
        port=5432
    )
    cur = conn.cursor()

    # Dummy sample data
    stock_data = [
        {"ticker": "AAPL", "date": "2024-01-02", "open": 150.00, "high": 155.00, "low": 149.00, "close": 154.00, "volume": 100000000},
        {"ticker": "GOOG", "date": "2024-01-02", "open": 2700.00, "high": 2750.00, "low": 2690.00, "close": 2725.00, "volume": 2000000},
    ]

    # Map ticker to company_key from dim_company table
    cur.execute("SELECT ticker, company_key FROM dim_company WHERE is_current=TRUE")
    rows = cur.fetchall()
    company_map = {ticker: key for ticker, key in rows}

    for row in stock_data:
        ticker = row["ticker"]
        company_key = company_map.get(ticker)
        if not company_key:
            print(f"❌ Skipping: {ticker} not found in dim_company")
            continue

        cur.execute("""
            INSERT INTO fact_stock_price_daily (date, company_key, open, high, low, close, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (date, company_key) DO UPDATE 
            SET open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume;
        """, (
            row["date"], company_key,
            row["open"], row["high"], row["low"], row["close"], row["volume"]
        ))

    conn.commit()
    cur.close()
    conn.close()
    print("✅ fact_stock_price_daily populated successfully.")
