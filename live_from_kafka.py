import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from datetime import time as dtime
from zoneinfo import ZoneInfo

from kafka import KafkaProducer
from yfinance import Ticker
from yfinance.exceptions import YFRateLimitError

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "scripts"))
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


def _is_market_open(now=None):
    """True during regular US market hours. `now` is injectable so this is
    testable without freezing the clock."""
    now = now or datetime.now(ET)
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


def _day_bar_from_intraday(data):
    """Fold a day of 1-minute bars into the day's bar so far.

    The payload lands in fact_stock_price_daily, so it has to describe the day,
    not a minute. Taking .tail(1) instead -- which is what this used to do --
    published the in-progress minute: across the 6,270 messages sitting in the
    topic, 97.1% carried volume 0 and 97.0% had open == high == low == close,
    because the final minute of a yfinance intraday frame is a partial bar with
    no trades in it yet. A row created from one of those had a zero-width range
    and a volume of 0 against a real 35 million.

    Open is the session's first print, high and low span the whole day, close
    is the latest price, and volume is the day's total. Returns None for an
    empty frame or a day with no volume yet (pre-open), which the caller drops.
    """
    if data is None or data.empty:
        return None
    volume = int(data["Volume"].sum())
    if volume <= 0:
        return None
    return {
        "date": data.index[-1].date().isoformat(),
        "open": float(data["Open"].iloc[0]),
        "high": float(data["High"].max()),
        "low": float(data["Low"].min()),
        "close": float(data["Close"].iloc[-1]),
        "volume": volume,
    }


def _fetch_ticker_data(ticker_obj, ticker):
    """Fetch the day's bar so far for a single ticker. Returns (ticker, payload) or (ticker, None)."""
    # period must be one of yfinance's accepted windows (1d, 5d, 1mo, ...).
    # "5m" is an interval, not a period, and silently returned an empty frame
    # for every ticker on every cycle, so nothing was ever produced.
    data = ticker_obj.history(period="1d", interval="1m")
    return ticker, _day_bar_from_intraday(data)


def main():
    tickers = _load_tickers()
    print(f"Producing for {len(tickers)} tickers: {', '.join(tickers)}")

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=[os.environ.get("KAFKA_BOOTSTRAP", "stock-data-platform-kafka:9092")],
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
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
                futures = {executor.submit(_fetch_ticker_data, ticker_objs[t], t): t for t in tickers}
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
