# scripts/populate_dim_date.py

import psycopg2
from datetime import date, timedelta

START_DATE = date(1990, 1, 1)
END_DATE = date(2035, 12, 31)

def generate_dates():
    conn = psycopg2.connect(
        host="timescaledb", dbname="stockdw",
        user="data226", password="12345678", port=5432
    )
    cur = conn.cursor()

    current = START_DATE
    while current <= END_DATE:
        year = current.year
        quarter = (current.month - 1) // 3 + 1
        month = current.month
        day = current.day
        day_of_week = current.weekday()
        is_weekend = day_of_week >= 5

        cur.execute("""
            INSERT INTO dim_date (date, year, quarter, month, day_of_month, day_of_week, is_weekend)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (date) DO NOTHING;
        """, (current, year, quarter, month, day, day_of_week, is_weekend))

        current += timedelta(days=1)

    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ dim_date populated from {START_DATE} to {END_DATE}")

if __name__ == "__main__":
    generate_dates()
