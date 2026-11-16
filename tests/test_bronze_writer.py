"""Bronze writer against a local Iceberg catalogue (no AWS needed)."""
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pytest
from pyiceberg.catalog.sql import SqlCatalog

import bronze_writer
import generator
from registry import DEVICES

CONTRACTS = Path(__file__).resolve().parent.parent / "contracts"
REQUEST_LOG = ("gateway.request_log", "ep_bronze.request_log", "event_ts")   # topic, table, partition column


class FakeRegistry:
    """Stands in for Schema Registry. Each topic holds a list of contract files returned one per
    lookup, with the last one repeating, so a test can simulate a client cache that goes stale."""

    def __init__(self, request_log=("request_log_v2.avsc",)):
        self.latest = {topic: [f] for topic, f, _ in generator.TOPICS.values()}
        self.latest["gateway.request_log"] = list(request_log)

    def get_latest_version(self, subject):
        queue = self.latest[subject.removesuffix("-value")]
        file_name = queue.pop(0) if len(queue) > 1 else queue[0]
        schema_str = (CONTRACTS / file_name).read_text()
        return type("V", (), {"schema": type("S", (), {"schema_str": schema_str})()})()


def records(kind, n):
    out = []
    for i in range(n):
        r = generator.BUILDERS[kind](DEVICES[i % len(DEVICES)])
        r.update(_kafka_partition=0, _kafka_offset=i, _ingest_ts=datetime.now(timezone.utc))
        out.append(r)
    return out


def v1_records(n):
    """request_log events from a producer still on contract v1."""
    out = records("request_log", n)
    for r in out:
        r.pop("tls_version")
        r["schema_version"] = 1
    return out


def v2_records(n):
    """request_log events from a producer on contract v2, with a known tls_version."""
    out = records("request_log", n)
    for r in out:
        r["tls_version"] = "TLSv1.3"
    return out


def make_writer(tmp_path, registry):
    catalog = SqlCatalog("test", uri=f"sqlite:///{tmp_path}/catalog.db", warehouse=f"file://{tmp_path}")
    catalog.create_namespace("ep_bronze")
    return bronze_writer.BronzeWriter(catalog=catalog, schema_registry=registry), catalog


def bronze(catalog):
    return catalog.load_table(REQUEST_LOG[1]).scan().to_arrow()


def test_avro_types_map_to_arrow():
    schema = bronze_writer.arrow_schema_from_avro((CONTRACTS / "request_log_v1.avsc").read_text())
    assert schema.field("event_ts").type == pa.timestamp("us", tz="UTC")
    assert schema.field("headers").type == pa.map_(pa.string(), pa.string())
    assert schema.names[-3:] == ["_kafka_partition", "_kafka_offset", "_ingest_ts"]
    v2 = bronze_writer.arrow_schema_from_avro((CONTRACTS / "request_log_v2.avsc").read_text())
    assert v2.field("tls_version").type == pa.string() and v2.field("tls_version").nullable


def test_first_write_creates_partitioned_table(tmp_path):
    writer, catalog = make_writer(tmp_path, FakeRegistry())
    assert writer.append(*REQUEST_LOG, records("request_log", 100)) == 100
    table = catalog.load_table(REQUEST_LOG[1])
    assert table.spec().fields[0].name == "event_ts_day"
    assert table.scan().to_arrow().num_rows == 100


def test_new_contract_field_widens_table(tmp_path):
    registry = FakeRegistry(request_log=["request_log_v1.avsc"])
    writer, catalog = make_writer(tmp_path, registry)
    writer.append(*REQUEST_LOG, v1_records(10))
    registry.latest["gateway.request_log"] = ["request_log_v2.avsc"]       # v2 is registered
    writer.append(*REQUEST_LOG, v2_records(5))
    data = bronze(catalog)
    assert "tls_version" in data.schema.names
    assert data.num_rows == 15 and data.column("tls_version").null_count == 10   # v1 rows read as null


def test_stale_contract_cache_is_refreshed_not_dropped(tmp_path):
    registry = FakeRegistry(request_log=["request_log_v1.avsc"])
    writer, catalog = make_writer(tmp_path, registry)
    writer.append(*REQUEST_LOG, v1_records(10))
    # v2 is registered, but the client's first lookup still answers v1 from its cache
    registry.latest["gateway.request_log"] = ["request_log_v1.avsc", "request_log_v2.avsc"]
    writer.append(*REQUEST_LOG, v2_records(5))
    assert bronze(catalog).column("tls_version").to_pylist().count("TLSv1.3") == 5    # nothing dropped


def test_field_outside_every_contract_fails_the_batch(tmp_path):
    writer, catalog = make_writer(tmp_path, FakeRegistry())
    writer.append(*REQUEST_LOG, v2_records(3))
    bad = v2_records(2)
    bad[0]["surprise_field"] = "x"
    with pytest.raises(ValueError, match="surprise_field"):
        writer.append(*REQUEST_LOG, bad)
    assert bronze(catalog).num_rows == 3            # the failed batch wrote nothing, so it can be replayed
