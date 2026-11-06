   # ADR 003: Bronze as Apache Iceberg tables in the Glue catalogue
   Status: Accepted
   Decision: The consumer appends micro-batches to Iceberg tables via PyIceberg, partitioned by day(event_ts), with Kafka partition, offset and ingest time on every row.
   Consequences: Snapshots give time travel and safe reads during writes. Hidden partitioning keeps Athena scans small. Frequent small batches create many small files, which maintenance (OPTIMIZE) will compact on Day 4.