"""Writes micro-batches to Apache Iceberg bronze tables registered in the AWS Glue Data Catalog.

- Table columns come from the latest registered Avro contract, plus Kafka lineage columns.
- Tables are created on first write, partitioned by day(event_ts).
- If a newer contract adds fields, the table schema is widened (union by name) before the append.
"""
import json
import os

import pyarrow as pa
from confluent_kafka.schema_registry import SchemaRegistryClient
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.transforms import DayTransform

REGION = os.getenv("AWS_REGION", "eu-north-1")
PROFILE = os.getenv("AWS_PROFILE", "edgepulse")
SR_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")

LINEAGE_FIELDS = [
    pa.field("_kafka_partition", pa.int32()),
    pa.field("_kafka_offset", pa.int64()),
    pa.field("_ingest_ts", pa.timestamp("us", tz="UTC")),
]

_PRIMITIVES = {
    "string": pa.string(), "int": pa.int32(), "long": pa.int64(),
    "float": pa.float32(), "double": pa.float64(), "boolean": pa.bool_(),
}


def _arrow_type(avro_type):
    if isinstance(avro_type, list):                      # union such as ["null", "string"]
        return _arrow_type([t for t in avro_type if t != "null"][0])
    if isinstance(avro_type, dict):
        if avro_type.get("logicalType") in ("timestamp-millis", "timestamp-micros"):
            return pa.timestamp("us", tz="UTC")
        if avro_type["type"] == "map":
            return pa.map_(pa.string(), _arrow_type(avro_type["values"]))
        return _arrow_type(avro_type["type"])
    return _PRIMITIVES[avro_type]


def arrow_schema_from_avro(avro_schema_str):
    fields = [pa.field(f["name"], _arrow_type(f["type"])) for f in json.loads(avro_schema_str)["fields"]]
    return pa.schema(fields + LINEAGE_FIELDS)


class BronzeWriter:
    def __init__(self, catalog=None, schema_registry=None):
        self.catalog = catalog or load_catalog("glue", **{
            "type": "glue",
            "client.region": REGION,
            "client.profile-name": PROFILE,
            "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
        })
        self.sr = schema_registry or SchemaRegistryClient({"url": SR_URL})
        self._tables = {}

    def latest_arrow_schema(self, topic):
        latest = self.sr.get_latest_version(f"{topic}-value")
        return arrow_schema_from_avro(latest.schema.schema_str)

    def _table(self, identifier, arrow_schema, partition_column):
        if identifier in self._tables:
            return self._tables[identifier]
        try:
            table = self.catalog.load_table(identifier)
        except NoSuchTableError:
            table = self.catalog.create_table(identifier, schema=arrow_schema)
            with table.update_spec() as spec:
                spec.add_field(partition_column, DayTransform(), f"{partition_column}_day")
            print(f"  created Iceberg table {identifier}, partitioned by day({partition_column})")
        self._tables[identifier] = table
        return table

    def append(self, topic, identifier, partition_column, records):
        arrow_schema = self.latest_arrow_schema(topic)
        table = self._table(identifier, arrow_schema, partition_column)
        current = set(table.schema().column_names)
        if any(name not in current for name in arrow_schema.names):
            with table.update_schema() as update:
                update.union_by_name(arrow_schema)
            print(f"  evolved {identifier}: added {[n for n in arrow_schema.names if n not in current]}")
        data = pa.Table.from_pylist(records, schema=arrow_schema)
        table.append(data)
        return data.num_rows
