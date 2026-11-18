# RB-06: Data repair with targeted reprocessing

| | |
|---|---|
| Use when | Silver or gold is wrong for a known window: rows deleted or changed by mistake, or a fixed logic bug that must be applied to past data |
| Do not use for | Recent data that is late (RB-01, RB-03) or duplicates in bronze (RB-05) |
| Tool | `edgepulse_reprocess` DAG, triggered by hand |

## What it does

Bronze is the source of truth. The DAG rebuilds one event-time window from bronze: silver re-reads bronze by event time and overwrites every matched row, and gold recomputes the same hours. Tests run after every model, so a repair is only published if it passes.

## Steps

1. **Scope the damage.** Work out the affected event hours in UTC.
   If the damage happened within the last day or so, an Iceberg snapshot from before it may still exist (VACUUM on Sundays keeps one day). Find it:
   ```sql
   select committed_at, snapshot_id, operation
   from "ep_silver"."stg_request_log$snapshots"
   order by committed_at desc limit 10;
   ```
   Then count the damaged rows by comparing that snapshot with the current table (see Verify below).
2. **Pause `edgepulse_hourly`** so two DAGs never merge into the same table at once.
3. **Trigger `edgepulse_reprocess`** with start and end hours, for example `2026-11-16 13:00:00` and `2026-11-16 14:00:00`. Whole hours only, at most 48 hours per run. Split bigger windows into several runs.
4. **Watch the run.** `check_window`, silver and gold must all be green. A failed test stops the repair before gold.
5. **Verify** (below), then **unpause `edgepulse_hourly`**.

## Verify

Silver, gold and bronze agree for the window. All three numbers should match:
```sql
select
  (select count(distinct event_id) from ep_bronze.request_log
    where event_ts >= timestamp '2026-11-16 13:00:00 UTC' and event_ts < timestamp '2026-11-16 14:00:00 UTC') as bronze_unique_events,
  (select count(*) from ep_silver.stg_request_log
    where event_ts >= timestamp '2026-11-16 13:00:00' and event_ts < timestamp '2026-11-16 14:00:00') as silver_rows,
  (select sum(request_count) from ep_gold.fct_request_hourly_v2
    where event_hour >= timestamp '2026-11-16 13:00:00' and event_hour < timestamp '2026-11-16 14:00:00') as gold_requests;
```

If a pre-damage snapshot exists, prove the repair row for row. Expect 0:
```sql
select count(*) as differences from (
  select event_id, status_code, latency_ms, client_ip_hash, is_late
  from ep_silver.stg_request_log for version as of <snapshot_id>
  where event_ts >= timestamp '2026-11-16 13:00:00' and event_ts < timestamp '2026-11-16 14:00:00'
  except
  select event_id, status_code, latency_ms, client_ip_hash, is_late
  from ep_silver.stg_request_log
  where event_ts >= timestamp '2026-11-16 13:00:00' and event_ts < timestamp '2026-11-16 14:00:00'
);
```

## After the repair

- `silver_dead_row_ratio` may warn: the repair rewrites every row in the window on purpose. Sunday's `edgepulse_maintenance` compacts them, or trigger it by hand.
- Record the window, run ID and verification numbers in the incident's RCA.

## Limits

- **Bad data in bronze itself** is reproduced by a repair. That needs a fix at the source and re-ingestion, which this project does not automate.
- **Time travel** only reaches back as far as snapshots are kept. Replaying from bronze works for any age.
- **The PII salt (`EP_PII_SALT`) must never change.** A repair with a different salt would produce different `client_ip_hash` values for the same IPs.

## In production

Require an approval before a repair runs, use a lock or pool instead of pausing the hourly DAG and allow reprocessing from a separate operator role only.