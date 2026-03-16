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


UPSERT_FUNDAMENTALS_SQL = """
    INSERT INTO fact_company_fundamentals
        (date, company_key, market_cap, trailing_pe, forward_pe, price_to_book,
         dividend_rate, dividend_yield, beta, week_52_high, week_52_low, employees, business_summary)
    VALUES %s
    ON CONFLICT (date, company_key) DO UPDATE
    SET market_cap = EXCLUDED.market_cap,
        trailing_pe = EXCLUDED.trailing_pe,
        forward_pe = EXCLUDED.forward_pe,
        price_to_book = EXCLUDED.price_to_book,
        dividend_rate = EXCLUDED.dividend_rate,
        dividend_yield = EXCLUDED.dividend_yield,
        beta = EXCLUDED.beta,
        week_52_high = EXCLUDED.week_52_high,
        week_52_low = EXCLUDED.week_52_low,
        employees = EXCLUDED.employees,
        business_summary = EXCLUDED.business_summary
"""

UPSERT_EARNINGS_SQL = """
    INSERT INTO fact_earnings (report_date, company_key, eps_estimate, eps_actual, surprise_pct)
    VALUES %s
    ON CONFLICT (report_date, company_key) DO UPDATE
    SET eps_estimate = EXCLUDED.eps_estimate,
        eps_actual = EXCLUDED.eps_actual,
        surprise_pct = EXCLUDED.surprise_pct
"""

UPSERT_SEC_FINANCIALS_SQL = """
    INSERT INTO fact_sec_financials
        (company_key, period_end, statement_type, line_item, filing_date, filing_type, value)
    VALUES %s
    ON CONFLICT (company_key, period_end, statement_type, line_item) DO UPDATE
    SET filing_date = EXCLUDED.filing_date,
        filing_type = EXCLUDED.filing_type,
        value = EXCLUDED.value
"""

UPSERT_MACRO_DATA_SQL = """
    INSERT INTO fact_macro_data (date, indicator_key, value)
    VALUES %s
    ON CONFLICT (date, indicator_key) DO UPDATE
    SET value = EXCLUDED.value
"""


def get_indicator_key(conn, series_id):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT indicator_key FROM dim_macro_indicator WHERE series_id = %s",
            (series_id,),
        )
        row = cur.fetchone()
        return row[0] if row else None
