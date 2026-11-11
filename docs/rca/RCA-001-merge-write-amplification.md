# RCA-001: Write amplification from incremental merges

| | |
|---|---|
| Status | Resolved |
| Severity | Low (no data correctness impact, efficiency and cost only) |
| Detected | 2026-11-08, during performance baselining |
| Resolved | 2026-11-08 |
| Affected | ep_silver.stg_request_log, stg_waf_alert, stg_device_metric; ep_gold.fct_request_hourly_v2, fct_device_health_hourly, fct_waf_alert |

## Summary

Every scheduled dbt run rewrote rows that had not changed. Under Iceberg merge-on-read, each rewrite left the old row in its data file, hidden by a delete file. By the time it was found, 43% of rows stored in silver and 55% in gold were dead. Queries returned correct results throughout, but read and stored far more data than needed.

## Impact

| Measure | Silver (stg_request_log) | Gold (fct_request_hourly_v2) |
|---|---|---|
| Rows stored in data files | 127,801 | 2,528 |
| Rows visible to queries | 72,996 | 1,150 |
| Dead rows | 54,805 (43%) | 1,378 (55%) |
| Storage before and after fix and compaction | 11,882 KB to 6,788 KB | 1,214 KB to 528 KB |

- **Data correctness:** none. Unique, not null and completeness tests passed throughout, and dashboards showed correct numbers.
- **Efficiency:** every query applied delete files at read time. Data scanned across three typical queries fell 52% after the fix and compaction.
- **Growth:** at production volume, dead rows would have grown with every hourly run, raising storage, scan cost and query latency over time.

## Timeline

| Date | Event |
|---|---|
| 2026-11-06 | Silver models deployed as incremental Iceberg merges on event_id with a 2-hour arrival-time lookback |
| 2026-11-06 to 08 | Gold hourly facts deployed with the same lookback pattern. Several dbt runs, all green |
| 2026-11-08 | Performance baseline showed silver `$files` summing to 127,801 rows while `count(*)` returned 72,996 |
| 2026-11-08 | Comparing `$files` record counts with `"$path"` row counts per file showed rows hidden at read time. One silver file was entirely superseded |
| 2026-11-08 | Root cause identified. `update_condition` added to silver, then gold. Repeat runs changed 0 rows and committed no snapshots |
| 2026-11-08 | OPTIMIZE and VACUUM removed existing dead rows. After numbers recorded in docs/perf.md |

## Detection

Found by manual investigation during performance baselining, **not by monitoring**. Data quality tests check correctness, so they passed. Athena's `$files` metadata table does not list Iceberg delete files, so the problem was invisible in file-level metadata too.

## Root cause (five whys)

1. **Why did queries read more data than needed?** Silver and gold data files held 54,805 and 1,378 rows that queries had to skip.
2. **Why were there hidden rows?** Athena's Iceberg MERGE is merge-on-read. Updating a row writes a new copy and a delete file that masks the old one, which stays on disk.
3. **Why were rows updated?** The merge used an unconditional `WHEN MATCHED THEN UPDATE`, so every matched row was rewritten.
4. **Why did so many rows match?** By design, each run re-reads a 2-hour lookback to catch batches that land late. Almost every row in that window already existed in the target.
5. **Why was it not caught earlier?** Nothing measured storage efficiency. Tests covered correctness only, and the delete files did not show in `$files`.

**Root cause:** a safety lookback combined with an unconditional merge update caused write amplification under merge-on-read, and there was no metric that would reveal it.

## Resolution

Merges now rewrite a row only when the incoming copy is genuinely newer:

| Model | Condition |
|---|---|
| Silver models and fct_waf_alert | `src._ingest_ts > target._ingest_ts` (a later duplicate) |
| fct_request_hourly_v2, fct_device_health_hourly | `src._source_max_ingest_ts > target._source_max_ingest_ts` (the hour received new events) |

The lookback is kept, so late batches are still picked up and late events still correct their hour. Existing dead rows were removed with `OPTIMIZE ... REWRITE DATA USING BIN_PACK` and `VACUUM`.

**Verification:** two further runs of silver and gold changed 0 rows and committed no new snapshots. After compaction, dead rows were 0 on every table.

## What went well

- The performance baseline was taken before maintenance, which exposed the gap.
- The fix kept the safety guarantees of the lookback and the late-data recompute.
- Merge on event_id kept data correct throughout, so there was no customer-facing impact.

## What could be better

- Storage efficiency was not monitored, so the issue was found by chance.
- The merge pattern was copied from silver to gold without reviewing its write behaviour.

## Prevention

| Action | Status |
|---|---|
| Add a dead-row ratio check per merged table to the Airflow quality checks, writing to `ep_ops.dq_results`. Warn above 20% | Done. Runs hourly in `run_dq_checks`, warn above 20%, error above 50% |
| Schedule OPTIMIZE and VACUUM on recent partitions | Planned (Block 4.3) |
| Rerun `scripts/perf_baseline.py` after each maintenance run and track the trend | Planned |
| Review checklist for incremental models: does the merge rewrite unchanged rows? | Done (added to README lessons learned) |

## Evidence

Dead rows per table:
```sql
select (select sum(record_count) from "ep_silver"."stg_request_log$files")
     - (select count(*) from ep_silver.stg_request_log) as dead_rows;
```

Rows written vs rows visible per file:
```sql
with claimed as (
    select file_path, record_count from "ep_silver"."stg_request_log$files"
),
actual as (
    select "$path" as file_path, count(*) as rows_returned
    from ep_silver.stg_request_log group by 1
)
select c.file_path, c.record_count, a.rows_returned
from claimed c left join actual a on c.file_path = a.file_path;
```