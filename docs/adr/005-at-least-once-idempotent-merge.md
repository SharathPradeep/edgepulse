# ADR 005: At-least-once delivery with an idempotent merge in silver

## Status
Accepted

## Context
The consumer commits Kafka offsets only after a successful write, so a crash replays messages and bronze can hold duplicates. Producers can also send late events.

## Decision
- Keep bronze append-only, including duplicates, for audit and replay.
- Silver models are incremental Iceberg merges on event_id. Duplicates within a batch are removed with row_number, and the latest arrival wins.
- The incremental window filters on arrival time (_ingest_ts) with a 2-hour lookback, so late events are always picked up and re-reads are harmless.
- Client IPs are hashed with a secret salt held outside Git.

## Consequences
- Simpler than exactly-once Kafka transactions, and survives crashes with no data loss.
- Re-running a model over the same input leaves row counts unchanged (verified).
- MERGE scans the silver target. At larger volumes, add incremental_predicates to limit the scan to recent partitions.
- Changing the salt changes every hash, so it is treated as permanent.