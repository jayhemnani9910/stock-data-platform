from kafka import KafkaProducer
import yfinance as yf
import json
import time
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

# Send mock stock data continuously
while True:
    try:
        data = Ticker("AAPL").history(period="1d", interval="1m").tail(1)
        payload = {
            "company_key": 1,
            "date": str(data.index[-1]),
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
