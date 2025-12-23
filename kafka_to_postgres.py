from kafka import KafkaConsumer
import json
import psycopg2
import time

# Wait for Kafka + DB readiness
time.sleep(10)

while True:
    try:
        consumer = KafkaConsumer(
            'stock-data',
            bootstrap_servers=['stock-data-platform-kafka:9092'],
            auto_offset_reset='earliest',
            group_id='stock-data-consumer',
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        print("✅ Connected to Kafka broker.")
        break
    except Exception as e:
        print("Retrying Kafka consumer in 5 seconds...", e)
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
            print("✅ Connected to TimescaleDB.")
            return conn
        except Exception as e:
            print("Retrying DB connection in 5 seconds...", e)
            time.sleep(5)

conn = connect_db()
cur = conn.cursor()

for message in consumer:
    data = message.value

    try:
        cur.execute("""
            INSERT INTO fact_stock_price_daily (company_key, date, open, high, low, close, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (date, company_key) DO UPDATE
            SET open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume;
        """, (
            data['company_key'], data['date'],
            data['open'], data['high'],
            data['low'], data['close'],
            data['volume']
        ))
        conn.commit()
        print("✅ Inserted:", data)
    except Exception as e:
        conn.rollback()
        print("❌ Insert failed:", e, data)

cur.close()
conn.close()
