# RB-03: Freshness breach

| | |
|---|---|
| Symptom | `edgepulse_hourly` fails on a bronze freshness task in the silver group, with an `ALERT` line in the log |
| Thresholds | Warn when the newest bronze row is over 15 minutes old, error over 60 minutes (`models/sources.yml`, based on `_ingest_ts`) |
| Effect | Blocking check: silver and gold do not run, so stale data is never published as fresh |

## What it means

No new rows have reached that bronze table recently. Either nothing is being produced, or something between Kafka and S3 is stuck.

## Check

1. **Which tables failed?**
   - **All three:** a shared component is down (Kafka, the consumer or the generator).
   - **One table:** that topic's producer, or writes to that one table.
2. **What is running?**
   ```bash
   docker compose ps
   ```
3. **Newest row per bronze table** (null means nothing in the last day):
   ```sql
   select 'request_log' as source, max(_ingest_ts) as newest from ep_bronze.request_log
     where event_ts > current_timestamp - interval '1' day
   union all
   select 'waf_alert', max(_ingest_ts) from ep_bronze.waf_alert
     where event_ts > current_timestamp - interval '1' day
   union all
   select 'device_metric', max(_ingest_ts) from ep_bronze.device_metric
     where event_ts > current_timestamp - interval '1' day;
   ```
4. **Kafka UI > Consumers > `edgepulse-bronze-loader`.**
   - Lag growing: messages are arriving but not being written. Go to RB-01.
   - Lag near zero: nothing new is arriving. The problem is upstream (producer).

## Fix

| Cause | Fix |
|---|---|
| Consumer stopped or failing | RB-01 |
| Generator or producer stopped | Restart it: `docker compose up -d generator` |
| Pipeline stopped on purpose (development) | Expected. Restart the stream before running the DAG |

Then in Airflow, open the failed run and **Clear** the failed task, or trigger a new run.

## Verify

- The freshness tasks pass and silver and gold run.
- `silver_rows_last_hour` is back in range (above 10,000).

No reprocessing is needed: silver reads by arrival time with a 2-hour lookback, so the delayed data flows through on the next run.

## In production

Page on the error threshold only during business hours unless the data has a tighter SLA, and show the freshness status on the data product's catalogue page.