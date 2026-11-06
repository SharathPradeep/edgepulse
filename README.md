# EdgePulse

A product telemetry pipeline: simulated app-delivery gateways stream request logs, WAF alerts and device metrics through Kafka (Avro contracts in Schema Registry) into Apache Iceberg tables on AWS S3, catalogued in Glue and queried with Athena. dbt and Airflow come next.

Work in progress. A full README with architecture, setup and screenshots follows on Day 5.

## Resilience

- **Hard kill before write:** no gaps in Kafka offsets in bronze (nothing lost). The one missing offset (partition 2, offset 2708) was a malformed message rejected to the dead-letter topic with "Invalid magic byte", so every source offset is accounted for in bronze or the DLQ.
- **Crash after write, before commit:** one batch replayed (duplicates, never loss). Silver deduplicates on event_id.