"""Register the v1 event contracts in Schema Registry and set BACKWARD compatibility.

Contracts are registered here, not by producers, so a producer can never
publish a new schema version by accident.
"""
import os
from pathlib import Path

from confluent_kafka.schema_registry import Schema, SchemaRegistryClient

SR_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
HERE = Path(__file__).resolve().parent

CONTRACTS = {
    "gateway.request_log-value": "request_log_v1.avsc",
    "gateway.waf_alert-value": "waf_alert_v1.avsc",
    "gateway.device_metric-value": "device_metric_v1.avsc",
}

client = SchemaRegistryClient({"url": SR_URL})
for subject, file_name in CONTRACTS.items():
    schema = Schema((HERE / file_name).read_text(), "AVRO")
    schema_id = client.register_schema(subject, schema)
    client.set_compatibility(subject_name=subject, level="BACKWARD")
    print(f"{subject}: schema id {schema_id}, compatibility BACKWARD")
