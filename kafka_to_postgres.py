from kafka import KafkaConsumer
import json
import time
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "scripts"))
from db_utils import connect_db, upsert_stock_prices

BATCH_SIZE = 50
FLUSH_INTERVAL = 10
POLL_TIMEOUT_MS = 5000
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "stock-data")
MAX_RETRIES = 10
BACKOFF_BASE = 5
BACKOFF_CAP = 60


def _connect_kafka():
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=[
                    os.environ.get("KAFKA_BOOTSTRAP", "stock-data-platform-kafka:9092")
                ],
                auto_offset_reset="earliest",
                group_id="stock-data-consumer",
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            )
            print("Connected to Kafka broker.")
            return consumer
        except Exception as e:
            delay = min(BACKOFF_BASE * (2 ** (attempt - 1)), BACKOFF_CAP)
            print(
                f"Kafka connection attempt {attempt}/{MAX_RETRIES} failed: {e}. Retrying in {delay}s..."
            )
            time.sleep(delay)
    raise ConnectionError(f"Failed to connect to Kafka after {MAX_RETRIES} attempts")


def _flush_batch(conn, batch):
    try:
        upsert_stock_prices(conn, batch)
        print(f"Committed batch of {len(batch)} messages")
        return conn
    except Exception as e:
        print(f"Batch insert failed: {e}. Reconnecting to DB...")
        try:
            conn.close()
        except Exception:
            pass
        conn = connect_db()
        try:
            upsert_stock_prices(conn, batch)
            print(f"Committed batch of {len(batch)} messages after reconnect")
        except Exception as e2:
            conn.rollback()
            print(
                f"Batch insert failed after reconnect: {e2}. Discarding {len(batch)} messages."
            )
        return conn


def main():
    consumer = _connect_kafka()
    conn = connect_db()
    batch = []
    last_flush = time.time()

    try:
        while True:
            records = consumer.poll(timeout_ms=POLL_TIMEOUT_MS)
            for tp, messages in records.items():
                for message in messages:
                    data = message.value
                    try:
                        batch.append(
                            (
                                data["date"],
                                data["company_key"],
                                data["open"],
                                data["high"],
                                data["low"],
                                data["close"],
                                data["volume"],
                            )
                        )
                    except (KeyError, TypeError) as e:
                        print(f"Malformed message, skipping: {e} {data}")

            now = time.time()
            if batch and (
                len(batch) >= BATCH_SIZE or now - last_flush >= FLUSH_INTERVAL
            ):
                conn = _flush_batch(conn, batch)
                batch = []
                last_flush = now
    finally:
        if batch:
            try:
                upsert_stock_prices(conn, batch)
                print(f"Committed final batch of {len(batch)} messages")
            except Exception as e:
                print(f"Failed to commit final batch: {e}")
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()
