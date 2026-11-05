"""EdgePulse telemetry generator.

Simulates 200 app-delivery gateways producing request logs, WAF alerts and
device metrics to Kafka as Avro, keyed by device_id. Fault switches (env vars)
inject duplicates, late events and poison messages for the failure drills.
"""
import json
import os
import random
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer

from registry import DEVICES, WAF_RULES

BOOTSTRAP = os.getenv("BOOTSTRAP", "localhost:9092")
SR_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
RATE = float(os.getenv("RATE", "20"))                         # events per second
DUPLICATE_RATE = float(os.getenv("DUPLICATE_RATE", "0"))      # share of events sent twice
LATE_RATE = float(os.getenv("LATE_RATE", "0"))                # share with event_ts 1-6 hours old
POISON_RATE = float(os.getenv("POISON_RATE", "0"))            # share sent as non-Avro bytes
CONTRACTS = Path(os.getenv("CONTRACTS_DIR", Path(__file__).resolve().parent.parent / "contracts"))

# kind -> (topic, contract file, share of traffic)
TOPICS = {
    "request_log": ("gateway.request_log", "request_log_v1.avsc", 0.80),
    "waf_alert": ("gateway.waf_alert", "waf_alert_v1.avsc", 0.05),
    "device_metric": ("gateway.device_metric", "device_metric_v1.avsc", 0.15),
}


def envelope(event_type, dev):
    ts = datetime.now(timezone.utc)
    if random.random() < LATE_RATE:
        ts -= timedelta(minutes=random.randint(60, 360))
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "device_id": dev["device_id"],
        "tenant_id": dev["tenant_id"],
        "product_version": dev["product_version"],
        "event_ts": ts,
        "schema_version": 1,
    }


def request_log(dev):
    status = random.choices(
        [200, 201, 204, 301, 400, 401, 403, 404, 500, 502, 503],
        weights=[60, 5, 3, 2, 5, 3, 3, 8, 5, 3, 3],
    )[0]
    return {
        **envelope("request_log", dev),
        "method": random.choice(["GET", "GET", "GET", "POST", "PUT", "DELETE"]),
        "path": random.choice(["/", "/login", "/api/orders", "/api/cart", "/static/app.js", "/health"]),
        "status_code": status,
        "latency_ms": round(random.lognormvariate(3.5, 0.6), 2),
        "bytes_out": random.randint(200, 250_000),
        "client_ip": ".".join(str(random.randint(1, 254)) for _ in range(4)),
        "client_region": dev["region"],
        "headers": {
            "user-agent": random.choice(["Mozilla/5.0", "curl/8.4", "okhttp/4.12"]),
            "x-request-id": str(uuid.uuid4()),
        },
    }


def waf_alert(dev):
    rule_id = random.choice(list(WAF_RULES))
    return {
        **envelope("waf_alert", dev),
        "rule_id": rule_id,
        "severity": random.choices(["low", "medium", "high", "critical"], weights=[50, 30, 15, 5])[0],
        "action": random.choice(["blocked", "flagged"]),
        "attack_category": WAF_RULES[rule_id],
        "request_id": str(uuid.uuid4()),
        "details": json.dumps({"score": random.randint(1, 100), "matched_field": random.choice(["query", "body", "header"])}),
    }


def device_metric(dev):
    return {
        **envelope("device_metric", dev),
        "cpu_pct": round(min(100.0, max(0.0, random.gauss(45, 15))), 1),
        "mem_pct": round(min(100.0, max(0.0, random.gauss(60, 10))), 1),
        "active_connections": max(0, int(random.gauss(800, 250))),
        "throughput_mbps": round(max(0.0, random.gauss(350, 120)), 1),
    }


BUILDERS = {"request_log": request_log, "waf_alert": waf_alert, "device_metric": device_metric}


def main():
    sr = SchemaRegistryClient({"url": SR_URL})
    # auto.register.schemas False: producers may only use contracts already registered
    serializers = {
        kind: AvroSerializer(sr, (CONTRACTS / file_name).read_text(), conf={"auto.register.schemas": False})
        for kind, (_, file_name, _) in TOPICS.items()
    }
    key_ser = StringSerializer("utf_8")
    producer = Producer({
        "bootstrap.servers": BOOTSTRAP,
        "acks": "all",                 # wait for all in-sync replicas
        "enable.idempotence": True,    # no duplicates from producer retries
        "linger.ms": 50,               # batch small messages for efficiency
        "compression.type": "lz4",
        "client.id": "edgepulse-generator",
    })

    def on_delivery(err, msg):
        if err is not None:
            print(f"Delivery failed for {msg.topic()}: {err}")

    def send(kind, event):
        topic = TOPICS[kind][0]
        value = serializers[kind](event, SerializationContext(topic, MessageField.VALUE))
        producer.produce(topic, key=key_ser(event["device_id"]), value=value, on_delivery=on_delivery)

    kinds = list(TOPICS)
    weights = [TOPICS[k][2] for k in kinds]
    sent = 0
    print(f"Producing about {RATE}/s to {BOOTSTRAP} "
          f"(duplicates={DUPLICATE_RATE}, late={LATE_RATE}, poison={POISON_RATE}). Ctrl+C to stop.")
    try:
        while True:
            kind = random.choices(kinds, weights)[0]
            dev = random.choice(DEVICES)
            if random.random() < POISON_RATE:
                producer.produce(TOPICS[kind][0], key=key_ser(dev["device_id"]),
                                 value=b'{"not": "avro"}', on_delivery=on_delivery)
            else:
                event = BUILDERS[kind](dev)
                send(kind, event)
                if random.random() < DUPLICATE_RATE:
                    send(kind, event)  # same event_id twice
            sent += 1
            producer.poll(0)
            if sent % 500 == 0:
                print(f"{sent} events sent")
            time.sleep(1 / RATE)
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush(10)
        print(f"Stopped after {sent} events")


if __name__ == "__main__":
    main()
