import json
import os
import sys
import time

from kafka import KafkaConsumer

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "scripts"))
from db_utils import connect_db, upsert_streaming_prices

BATCH_SIZE = 50
FLUSH_INTERVAL = 10
POLL_TIMEOUT_MS = 5000
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "stock-data")
MAX_RETRIES = 10
BACKOFF_BASE = 5
BACKOFF_CAP = 60


def _deserialize(m):
    try:
        return json.loads(m.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        print(f"Malformed message, could not decode JSON: {e}")
        return None


def _connect_kafka():
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=[os.environ.get("KAFKA_BOOTSTRAP", "stock-data-platform-kafka:9092")],
                auto_offset_reset="earliest",
                group_id="stock-data-consumer",
                value_deserializer=_deserialize,
                enable_auto_commit=False,
            )
            print("Connected to Kafka broker.")
            return consumer
        except Exception as e:
            delay = min(BACKOFF_BASE * (2 ** (attempt - 1)), BACKOFF_CAP)
            print(f"Kafka connection attempt {attempt}/{MAX_RETRIES} failed: {e}. Retrying in {delay}s...")
            time.sleep(delay)
    raise ConnectionError(f"Failed to connect to Kafka after {MAX_RETRIES} attempts")


def _collapse_batch(batch):
    """Merge rows that share (date, company_key) before the upsert.

    execute_values sends the whole batch as one INSERT, and Postgres rejects a
    statement whose rows collide on the conflict target with "ON CONFLICT DO
    UPDATE command cannot affect row a second time". The producer emits one tick
    per ticker per cycle, all stamped with the same calendar date, so any batch
    spanning more than one cycle collides and the entire batch was discarded.

    Collapse with the same rule the SQL uses: first open, widest high and low,
    latest close, largest volume.
    """
    merged = {}
    for date, key, open_, high, low, close, volume in batch:
        prev = merged.get((date, key))
        if prev is None:
            merged[(date, key)] = [date, key, open_, high, low, close, volume]
        else:
            prev[3] = max(prev[3], high)
            prev[4] = min(prev[4], low)
            prev[5] = close
            prev[6] = max(prev[6], volume)
    return [tuple(r) for r in merged.values()]


def _flush_batch(conn, batch):
    batch = _collapse_batch(batch)
    try:
        upsert_streaming_prices(conn, batch)
        print(f"Committed batch of {len(batch)} messages")
        return conn, True
    except Exception as e:
        print(f"Batch insert failed: {e}. Reconnecting to DB...")
        try:
            conn.close()
        except Exception:
            pass
        conn = connect_db()
        try:
            upsert_streaming_prices(conn, batch)
            print(f"Committed batch of {len(batch)} messages after reconnect")
            return conn, True
        except Exception as e2:
            conn.rollback()
            print(f"Batch insert failed after reconnect: {e2}. Discarding {len(batch)} messages.")
            return conn, False


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
                    if data is None:
                        continue
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
            if batch and (len(batch) >= BATCH_SIZE or now - last_flush >= FLUSH_INTERVAL):
                conn, ok = _flush_batch(conn, batch)
                if ok:
                    consumer.commit()
                batch = []
                last_flush = now
    finally:
        if batch:
            try:
                upsert_streaming_prices(conn, batch)
                print(f"Committed final batch of {len(batch)} messages")
                consumer.commit()
            except Exception as e:
                print(f"Failed to commit final batch: {e}")
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()
