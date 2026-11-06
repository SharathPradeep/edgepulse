"""Bronze writer against a local Iceberg catalogue (no AWS needed)."""
import json
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog

import bronze_writer
import generator
from registry import DEVICES

CONTRACTS = Path(__file__).resolve().parent.parent / "contracts"


class FakeRegistry:
    def __init__(self):
        self.schemas = {topic: (CONTRACTS / f).read_text() for topic, f, _ in generator.TOPICS.values()}

    def get_latest_version(self, subject):
        schema_str = self.schemas[subject.removesuffix("-value")]
        return type("V", (), {"schema": type("S", (), {"schema_str": schema_str})()})()


def records(kind, n):
    out = []
    for i in range(n):
        r = generator.BUILDERS[kind](DEVICES[i % len(DEVICES)])
        r.update(_kafka_partition=0, _kafka_offset=i, _ingest_ts=datetime.now(timezone.utc))
        out.append(r)
    return out


def make_writer(tmp_path):
    catalog = SqlCatalog("test", uri=f"sqlite:///{tmp_path}/catalog.db", warehouse=f"file://{tmp_path}")
    catalog.create_namespace("ep_bronze")
    registry = FakeRegistry()
    return bronze_writer.BronzeWriter(catalog=catalog, schema_registry=registry), catalog, registry


def test_avro_types_map_to_arrow():
    schema = bronze_writer.arrow_schema_from_avro((CONTRACTS / "request_log_v1.avsc").read_text())
    assert schema.field("event_ts").type == pa.timestamp("us", tz="UTC")
    assert schema.field("headers").type == pa.map_(pa.string(), pa.string())
    assert schema.names[-3:] == ["_kafka_partition", "_kafka_offset", "_ingest_ts"]


def test_first_write_creates_partitioned_table(tmp_path):
    writer, catalog, _ = make_writer(tmp_path)
    assert writer.append("gateway.request_log", "ep_bronze.request_log", "event_ts", records("request_log", 100)) == 100
    table = catalog.load_table("ep_bronze.request_log")
    assert table.spec().fields[0].name == "event_ts_day"
    assert table.scan().to_arrow().num_rows == 100


def test_new_contract_field_widens_table(tmp_path):
    writer, catalog, registry = make_writer(tmp_path)
    writer.append("gateway.request_log", "ep_bronze.request_log", "event_ts", records("request_log", 10))
    v2 = json.loads(registry.schemas["gateway.request_log"])
    v2["fields"].append({"name": "tls_version", "type": ["null", "string"], "default": None})
    registry.schemas["gateway.request_log"] = json.dumps(v2)
    new_rows = records("request_log", 5)
    for r in new_rows:
        r["tls_version"] = "TLSv1.3"
    writer.append("gateway.request_log", "ep_bronze.request_log", "event_ts", new_rows)
    data = catalog.load_table("ep_bronze.request_log").scan().to_arrow()
    assert "tls_version" in data.schema.names
    assert data.num_rows == 15 and data.column("tls_version").null_count == 10
