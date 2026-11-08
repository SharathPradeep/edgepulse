# Performance baseline

Workgroup `edgepulse`, eu-north-1. Measured with `scripts/perf_baseline.py`. The same day (2026-11-06) is queried before and after maintenance so the numbers are comparable.

## Summary

Iceberg compaction plus conditional merges cut Athena data scanned by **52%** across three typical queries (up to 56% on the gold dashboard query), reduced every table to a single data file per partition and removed all dead rows.

## Before maintenance (Day 3)

### Queries

| Query | Data scanned (KB) | Median run time (ms) |
|---|---|---|
| Q1 bronze, one device, one day | 414 | 701 |
| Q2 silver, error rate by version | 60 | 664 |
| Q3 gold, p95 by version | 1,141 | 1,220 |

### Tables

| Table | Data files | Rows in files | Visible rows | Dead rows | Avg data file (KB) | Snapshots |
|---|---|---|---|---|---|---|
| ep_bronze.request_log | 62 | 76,746 | 76,746 | 0 | 97.2 | 62 |
| ep_silver.stg_request_log | 4 | 127,801 | 72,996 | 54,805 | 2,970.4 | 4 |
| ep_gold.fct_request_hourly_v2 | 3 | 2,528 | 1,150 | 1,378 | 404.5 | 3 |

Dead rows are rows still stored in data files but hidden at read time by Iceberg delete files (merge-on-read). Athena's `$files` does not list delete files, so dead rows are measured as file record counts minus `count(*)`.

## Observations before maintenance

- **Bronze has the small files problem.** The consumer commits one file and one snapshot per minute, so files average under 100 KB, far below the 128 to 512 MB that Parquet and Athena work best with. Every query opens and reads the footer of each file.
- **Q1 reads the whole day for one device.** Partition pruning limits it to one day, but rows for dev-0042 are spread across every file, so nothing else can be skipped.
- **Silver scans far less than bronze** (60 KB vs 414 KB). It has fewer, larger files, and Q2 reads only two narrow columns.
- **Gold scans the most** (1,141 KB) despite having the fewest rows, because each row carries a latency digest (a binary quantile sketch). This is the cost of accurate percentiles at any level. If cost mattered at scale, the digest could move to a separate table read only by percentile queries.
- **Deduplication works.** Bronze holds 76,746 rows but 72,996 unique events, because 3,750 duplicates (4.9%) were injected in the resilience drill. Silver holds exactly 72,996.
- **Merges caused write amplification.** 43% of silver rows and 55% of gold rows on disk were dead. Every run re-read the 2-hour lookback, and MERGE rewrote matched rows even when nothing had changed. Found by comparing `$files` record counts with `"$path"` row counts per file. One silver file was entirely superseded.
- **Fixed with conditional updates.** Silver and `fct_waf_alert` rewrite a row only when a later copy arrives (`src._ingest_ts > target._ingest_ts`). The hourly gold facts rewrite an hour only when it received new events (`src._source_max_ingest_ts > target._source_max_ingest_ts`). Repeat silver and gold runs after the fix changed 0 rows, committed no new snapshots and dead rows stopped growing.
- **Cost context:** Athena bills a minimum of 10 MB per query, so at this volume the improvements matter more for run time, metadata and storage than for the bill. The same ratios apply at production scale, where they do affect cost.

## Maintenance (Day 4)

Done on 2026-11-08 with the consumer stopped, to avoid commit conflicts with the per-minute appends:

1. Lowered `optimize_rewrite_data_file_threshold` and `optimize_rewrite_delete_file_threshold` to 1 (defaults 5 and 2, which would have skipped silver's 4 files) and set `vacuum_max_snapshot_age_seconds` to 86400 (1 day).
2. `OPTIMIZE ... REWRITE DATA USING BIN_PACK` on bronze, silver and the three gold facts.
3. `VACUUM` on the same tables to expire old snapshots and delete unreferenced files.

Athena's OPTIMIZE only bin-packs, so files are not sorted by device_id. Sorting would need Spark's `rewrite_data_files` with a sort strategy (a Glue or EMR job) and is the next step for cutting Q1 bytes further.

## After maintenance

### Queries

| Query | Before (KB) | After (KB) | Change | Before (ms) | After (ms) | Change |
|---|---|---|---|---|---|---|
| Q1 bronze, one device, one day | 414 | 241 | -42% | 701 | 661 | -6% |
| Q2 silver, error rate by version | 60 | 36 | -40% | 664 | 549 | -17% |
| Q3 gold, p95 by version | 1,141 | 498 | -56% | 1,220 | 1,075 | -12% |
| **Total** | **1,615** | **775** | **-52%** | | | |

### Tables

| Table | Files before | Files after | Dead rows before | Dead rows after | Storage before (KB) | Storage after (KB) | Snapshots before | Snapshots after |
|---|---|---|---|---|---|---|---|---|
| ep_bronze.request_log | 62 | 1 | 0 | 0 | 6,026 | 4,691 | 62 | 1 |
| ep_silver.stg_request_log | 4 | 1 | 54,805 | 0 | 11,882 | 6,788 | 4 | 2 |
| ep_gold.fct_request_hourly_v2 | 3 | 1 | 1,378 | 0 | 1,214 | 528 | 3 | 4 |

Storage is data files multiplied by the average file size.

## Observations after maintenance

- **Bytes scanned fell more than expected, even on bronze, which had no dead rows.** One large Parquet file compresses better than 62 small ones (bronze storage fell 22%) and carries one footer and set of column statistics instead of 62.
- **Silver and gold gained most** because compaction applied the delete files and dropped 54,805 and 1,378 dead rows. Storage fell 43% and 56%.
- **Run times fell 6 to 17%.** At this data size most of the run time is fixed query planning and start-up overhead, so the gap grows with volume.
- **Snapshot counts differ by design.** Bronze and silver snapshots from 2026-11-06 were older than the 1-day retention and were expired. Gold's recent snapshots from today's dbt runs are inside the retention window, so they were kept (plus one from OPTIMIZE). Time travel now reaches back 1 day.

## Next steps

- Schedule OPTIMIZE and VACUUM weekly in Airflow, in a window where the consumer pauses or with commit retries.
- Sort bronze by device_id with a Spark rewrite job to cut point-lookup scans further.
- Raise the consumer batch size or interval to create fewer, larger bronze files in the first place.