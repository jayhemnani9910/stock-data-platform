from kafka import KafkaProducer
import json
import time
import os
import sys
from datetime import datetime, time as dtime
from zoneinfo import ZoneInfo
from concurrent.futures import ThreadPoolExecutor, as_completed
from yfinance import Ticker
from yfinance.exceptions import YFRateLimitError

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'scripts'))
from db_utils import connect_db

ET = ZoneInfo("America/New_York")
MARKET_OPEN = dtime(9, 30)
MARKET_CLOSE = dtime(16, 0)

POLL_INTERVAL = 15
OFF_HOURS_INTERVAL = 300
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "stock-data")
MAX_RETRIES = 10
BACKOFF_BASE = 5
BACKOFF_CAP = 60


def _load_tickers():
    tickers_str = os.environ.get("STOCK_TICKERS", "AAPL")
    return [t.strip() for t in tickers_str.split(",") if t.strip()]


def _is_market_open():
    now = datetime.now(ET)
    if now.weekday() >= 5:
        return False
    return MARKET_OPEN <= now.time() <= MARKET_CLOSE


def _resolve_company_keys(tickers):
    conn = connect_db()
    try:
        while True:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT ticker, company_key FROM dim_company WHERE ticker = ANY(%s) AND is_current = TRUE",
                    (list(tickers),),
                )
                company_keys = {ticker: key for ticker, key in cur.fetchall()}
            missing = [t for t in tickers if t not in company_keys]
            if not missing:
                return company_keys
            print(f"{', '.join(missing)} not found in dim_company yet. Retrying in 10 seconds...")
            time.sleep(10)
    finally:
        conn.close()


def _fetch_ticker_data(ticker_obj, ticker):
    """Fetch latest 1-min bar for a single ticker. Returns (ticker, payload) or (ticker, None)."""
    data = ticker_obj.history(period="5m", interval="1m").tail(1)
    if data.empty:
        return ticker, None
    last_ts = data.index[-1]
    return ticker, {
        "date": last_ts.date().isoformat(),
        "open": float(data["Open"].iloc[-1]),
        "high": float(data["High"].iloc[-1]),
        "low": float(data["Low"].iloc[-1]),
        "close": float(data["Close"].iloc[-1]),
        "volume": int(data["Volume"].iloc[-1])
    }


def main():
    tickers = _load_tickers()
    print(f"Producing for {len(tickers)} tickers: {', '.join(tickers)}")

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=[os.environ.get('KAFKA_BOOTSTRAP', 'stock-data-platform-kafka:9092')],
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("Connected to Kafka broker.")
            break
        except Exception as e:
            delay = min(BACKOFF_BASE * (2 ** (attempt - 1)), BACKOFF_CAP)
            print(f"Kafka connection attempt {attempt}/{MAX_RETRIES} failed: {e}. Retrying in {delay}s...")
            time.sleep(delay)
    else:
        raise ConnectionError(f"Failed to connect to Kafka after {MAX_RETRIES} attempts")

    company_keys = _resolve_company_keys(tickers)
    ticker_objs = {t: Ticker(t) for t in tickers}
    logged_closed = False

    while True:
        if not _is_market_open():
            if not logged_closed:
                print("Market is closed. Checking again in 5 minutes...")
                logged_closed = True
            time.sleep(OFF_HOURS_INTERVAL)
            continue

        logged_closed = False
        try:
            with ThreadPoolExecutor(max_workers=min(len(tickers), 5)) as executor:
                futures = {
                    executor.submit(_fetch_ticker_data, ticker_objs[t], t): t
                    for t in tickers
                }
                for future in as_completed(futures):
                    ticker = futures[future]
                    try:
                        _, payload = future.result()
                        if payload:
                            payload["company_key"] = company_keys[ticker]
                            producer.send(KAFKA_TOPIC, value=payload)
                            print(f"Sent [{ticker}]:", payload)
                    except YFRateLimitError:
                        print("Rate limited by Yahoo Finance. Backing off for 60 seconds...")
                        time.sleep(60)
                    except Exception as e:
                        print(f"Error fetching {ticker}: {e}")
        except Exception as e:
            print(f"Unexpected error in fetch cycle: {e}")

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
