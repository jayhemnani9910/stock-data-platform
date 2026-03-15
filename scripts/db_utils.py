import os
import time
import psycopg2
from contextlib import contextmanager
from psycopg2.extras import execute_values

MAX_RETRIES = 10
BACKOFF_BASE = 5
BACKOFF_CAP = 60


def connect_db(retries=True, max_retries=MAX_RETRIES):
    params = {
        "host": os.environ["DB_HOST"],
        "dbname": os.environ["DB_NAME"],
        "user": os.environ["DB_USER"],
        "password": os.environ["DB_PASSWORD"],
        "port": int(os.environ.get("DB_PORT", 5432)),
    }
    if not retries:
        return psycopg2.connect(**params)

    for attempt in range(1, max_retries + 1):
        try:
            conn = psycopg2.connect(**params)
            print("Connected to TimescaleDB.")
            return conn
        except Exception as e:
            delay = min(BACKOFF_BASE * (2 ** (attempt - 1)), BACKOFF_CAP)
            print(f"DB connection attempt {attempt}/{max_retries} failed: {e}. Retrying in {delay}s...")
            time.sleep(delay)
    raise ConnectionError(f"Failed to connect to TimescaleDB after {max_retries} attempts")


@contextmanager
def get_db_connection(retries=True):
    """Context manager that properly closes the connection on exit."""
    conn = connect_db(retries=retries)
    try:
        yield conn
    finally:
        conn.close()


def get_company_key(conn, ticker):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT company_key FROM dim_company WHERE ticker = %s AND is_current = TRUE",
            (ticker,),
        )
        row = cur.fetchone()
        return row[0] if row else None


UPSERT_STOCK_PRICE_SQL = """
    INSERT INTO fact_stock_price_daily (date, company_key, open, high, low, close, volume)
    VALUES %s
    ON CONFLICT (date, company_key) DO UPDATE
    SET open = EXCLUDED.open,
        high = EXCLUDED.high,
        low = EXCLUDED.low,
        close = EXCLUDED.close,
        volume = EXCLUDED.volume
"""


def batch_insert(conn, sql, records, page_size=1000):
    """Generic batch insert using execute_values."""
    with conn.cursor() as cur:
        execute_values(cur, sql, records, page_size=page_size)
    conn.commit()


def upsert_stock_prices(conn, rows, page_size=500):
    batch_insert(conn, UPSERT_STOCK_PRICE_SQL, rows, page_size=page_size)
