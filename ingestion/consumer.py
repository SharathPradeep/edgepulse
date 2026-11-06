"""EdgePulse ingestion consumer (Block 1.4 skeleton).

Reads the three gateway topics, decodes each message against its registered
Avro contract, routes anything undecodable or invalid to the dead-letter topic
and collects good records into micro-batches. In Block 2.1, write_batch()
becomes a PyIceberg append to bronze; here it only reports what it would write.

Delivery guarantee: at-least-once. Offsets are committed only after a batch
has been written, so a crash before the write means the batch is re-read,
never lost. Duplicates this can cause are removed in silver (merge on event_id).
"""
import os
import signal
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import yaml
from confluent_kafka import Consumer, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

BOOTSTRAP = os.getenv("BOOTSTRAP", "localhost:9092")
SR_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
CONFIG_PATH = Path(os.getenv("TOPICS_CONFIG", Path(__file__).resolve().parent / "topics.yml"))

REQUIRED_FIELDS = ["event_id", "device_id", "tenant_id", "event_ts"]

running = True


def stop(*_):
    global running
    running = False


signal.signal(signal.SIGTERM, stop)
signal.signal(signal.SIGINT, stop)


def with_retries(fn, attempts=5, base_delay=1.0):
    """Call fn(); on failure wait 1s, 2s, 4s, 8s... (exponential backoff) before trying again."""
    for attempt in range(1, attempts + 1):
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001
            if attempt == attempts:
                raise
            delay = base_delay * 2 ** (attempt - 1)
            print(f"Attempt {attempt} failed ({exc}). Retrying in {delay:.0f}s")
            time.sleep(delay)


def validate(record):
    """Business checks beyond the Avro types. Returns an error string or None."""
    for field in REQUIRED_FIELDS:
        if record.get(field) in (None, ""):
            return f"missing {field}"
    if record["event_ts"] > datetime.now(timezone.utc) + timedelta(minutes=5):
        return "event_ts more than 5 minutes in the future"
    return None


def write_batch(batches, routes):
    """Block 1.4 stub. Block 2.1 replaces this with a PyIceberg append per table."""
    for topic, records in batches.items():
        if records:
            print(f"  would write {len(records):>5} records to {routes[topic]['target_table']}")


def main():
    cfg = yaml.safe_load(CONFIG_PATH.read_text())
    routes = {t["topic"]: t for t in cfg["topics"]}
    dlq_topic = cfg["dlq_topic"]
    max_records = cfg["consumer"]["batch_max_records"]
    max_seconds = cfg["consumer"]["batch_max_seconds"]

    deserializer = AvroDeserializer(SchemaRegistryClient({"url": SR_URL}))
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": cfg["consumer"]["group_id"],
        "auto.offset.reset": "earliest",   # a brand-new group starts from the oldest retained message
        "enable.auto.commit": False,       # we commit ourselves, only after a successful write
    })
    dlq = Producer({"bootstrap.servers": BOOTSTRAP, "acks": "all", "enable.idempotence": True})
    consumer.subscribe(list(routes))

    batches = {t: [] for t in routes}
    batch_started = time.monotonic()
    dlq_count = 0
    print(f"Consuming {list(routes)} as group {cfg['consumer']['group_id']}. Ctrl+C to stop.")

    def send_to_dlq(msg, error):
        dlq.produce(dlq_topic, key=msg.key(), value=msg.value(), headers={
            "error": error[:500],
            "source_topic": msg.topic(),
            "source_partition": str(msg.partition()),
            "source_offset": str(msg.offset()),
            "failed_at": datetime.now(timezone.utc).isoformat(),
        })

    def flush():
        nonlocal batches, batch_started, dlq_count
        total = sum(len(v) for v in batches.values())
        if total or dlq_count:
            print(f"Flushing batch: {total} good records, {dlq_count} sent to DLQ")
            with_retries(lambda: write_batch(batches, routes))
            dlq.flush(10)
            consumer.commit(asynchronous=False)   # commit only after the write succeeded
        batches = {t: [] for t in routes}
        batch_started = time.monotonic()
        dlq_count = 0

    try:
        while running:
            for msg in consumer.consume(num_messages=500, timeout=1.0):
                if msg.error():
                    print(f"Kafka error: {msg.error()}")
                    continue
                try:
                    record = deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
                except Exception as exc:  # noqa: BLE001
                    send_to_dlq(msg, f"deserialisation failed: {exc}")
                    dlq_count += 1
                    continue
                error = validate(record) if isinstance(record, dict) else "not a record"
                if error:
                    send_to_dlq(msg, error)
                    dlq_count += 1
                    continue
                record["_kafka_partition"] = msg.partition()
                record["_kafka_offset"] = msg.offset()
                record["_ingest_ts"] = datetime.now(timezone.utc)
                batches[msg.topic()].append(record)
            dlq.poll(0)
            total = sum(len(v) for v in batches.values())
            if total >= max_records or time.monotonic() - batch_started >= max_seconds:
                flush()
    finally:
        flush()
        consumer.close()
        print("Consumer stopped cleanly")


if __name__ == "__main__":
    main()
