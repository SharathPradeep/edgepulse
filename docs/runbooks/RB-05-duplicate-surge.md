# RB-05: Duplicate surge

| | |
|---|---|
| Symptom | `run_dq_checks` fails on `bronze_duplicate_rate` with an `ALERT` line. Gold is usually green |
| Thresholds | Warn above 1%, error above 3% of bronze rows in the last 2 hours of arrivals (`include/dq/checks.py`) |
| Effect | Alerting check, not blocking. Silver merges on `event_id`, so duplicates normally never reach silver or gold |

## What it means

Bronze is receiving more than one copy of the same event. Bronze keeps every copy on purpose, and silver keeps exactly one. The alert tells you a producer or the consumer is misbehaving. It does not mean the published data is wrong.

**Do not clean up duplicates by hand.** Never delete from bronze, and never run DML on silver or gold. That is what turned a harmless alert into data damage in [RCA-002](../rca/RCA-002-silver-damaged-during-duplicate-incident.md).

## Check

1. **Confirm silver is unaffected:**
   ```sql
   select
     (select count(*) - count(distinct event_id) from ep_bronze.request_log
       where event_ts > current_timestamp - interval '1' day
         and _ingest_ts > current_timestamp - interval '2' hour) as bronze_extra_copies,
     (select count(*) - count(distinct event_id) from ep_silver.stg_request_log
       where event_ts > cast(current_timestamp at time zone 'UTC' as timestamp(6)) - interval '1' day) as silver_duplicates;
   ```
   Expect `silver_duplicates` = 0.
2. **Who is sending the copies?**
   ```sql
   select
     count(*) - count(distinct event_id) as extra_copies,
     count(*) - count(distinct concat(cast(_kafka_partition as varchar), ':', cast(_kafka_offset as varchar))) as same_offset_copies
   from ep_bronze.request_log
   where event_ts > current_timestamp - interval '1' day
     and _ingest_ts > current_timestamp - interval '2' hour;
   ```

   | Result | Cause |
   |---|---|
   | `same_offset_copies` close to `extra_copies` | The consumer wrote the same Kafka messages twice: it crashed after a write and before committing, then replayed. Expected under at-least-once delivery, normally small |
   | `same_offset_copies` near 0 | A producer published the same event several times, for example a retry storm |

3. **Concentrated or everywhere?**
   ```sql
   select device_id, count(*) - count(distinct event_id) as extra_copies
   from ep_bronze.request_log
   where event_ts > current_timestamp - interval '1' day
     and _ingest_ts > current_timestamp - interval '2' hour
   group by 1 order by 2 desc limit 10;
   ```
   A few devices means a device-side fault. All devices means a shared producer or the consumer.

## Fix

| Cause | Fix |
|---|---|
| Producer retry storm | Fix or restart the producer. In this project: `docker compose up -d generator` resets `DUPLICATE_RATE` to 0 |
| Consumer crash and replay | Stop the crash loop with RB-01 |
| Silver or gold damaged anyway | RB-06 |

## Verify

- `silver_duplicates` stays at 0.
- `bronze_duplicate_rate` returns to pass. It is measured over the last 2 hours of arrivals, so it can take up to 2 hours after the fix.

## In production

Make producers idempotent (`enable.idempotence`) and keep `event_id` stable across retries, so copies are cheap to remove downstream.