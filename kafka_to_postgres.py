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
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        print("✅ Connected to Kafka broker.")
        break
    except Exception as e:
        print("Retrying Kafka consumer in 5 seconds...", e)
        time.sleep(5)

conn = psycopg2.connect(
    host="timescaledb",
    dbname="stockdw",
    user="data226",
    password="12345678",
    port=5432
)
cur = conn.cursor()

for message in consumer:
    data = message.value

    cur.execute("""
        INSERT INTO fact_stock_price_daily (company_key, date, open, high, low, close, volume)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING;
    """, (
        data['company_key'], data['date'],
        data['open'], data['high'],
        data['low'], data['close'],
        data['volume']
    ))

    conn.commit()
    print("✅ Inserted:", data)

cur.close()
conn.close()
