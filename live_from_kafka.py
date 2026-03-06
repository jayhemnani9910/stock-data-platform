from kafka import KafkaProducer
import yfinance as yf
import json
import time
import psycopg2
from yfinance import Ticker
from yfinance.exceptions import YFRateLimitError

# Retry until Kafka is available
while True:
    try:
        producer = KafkaProducer(
            bootstrap_servers=['stock-data-platform-kafka:9092'],  # container name in docker-compose
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("✅ Connected to Kafka broker.")
        break
    except Exception as e:
        print("Retrying Kafka connection in 5 seconds...", e)
        time.sleep(5)

def connect_db():
    while True:
        try:
            conn = psycopg2.connect(
                host="timescaledb",
                dbname="stockdw",
                user="data226",
                password="12345678",
                port=5432
            )
            return conn
        except Exception as e:
            print("Retrying DB connection in 5 seconds...", e)
            time.sleep(5)

def get_company_key(ticker):
    conn = connect_db()
    cur = conn.cursor()
    cur.execute(
        "SELECT company_key FROM dim_company WHERE ticker = %s AND is_current = TRUE",
        (ticker,)
    )
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row[0] if row else None

TICKER = "AAPL"
company_key = None
while company_key is None:
    company_key = get_company_key(TICKER)
    if company_key is None:
        print(f"⏳ {TICKER} not found in dim_company yet. Retrying in 10 seconds...")
        time.sleep(10)

# Send mock stock data continuously
while True:
    try:
        data = Ticker(TICKER).history(period="1d", interval="1m").tail(1)
        if data.empty:
            print("⏭️ No new market data yet. Retrying in 30 seconds...")
            time.sleep(30)
            continue
        last_ts = data.index[-1]
        payload = {
            "company_key": company_key,
            "date": last_ts.date().isoformat(),
            "open": float(data["Open"].iloc[-1]),
            "high": float(data["High"].iloc[-1]),
            "low": float(data["Low"].iloc[-1]),
            "close": float(data["Close"].iloc[-1]),
            "volume": int(data["Volume"].iloc[-1])
        }
        producer.send("stock-data", value=payload)
        print("📤 Sent:", payload)
        time.sleep(15)
    except YFRateLimitError:
        print("⏳ Rate limited by Yahoo Finance. Backing off for 60 seconds...")
        time.sleep(60)
    except Exception as e:
        print("❌ Unexpected error:", e)
        time.sleep(30)
