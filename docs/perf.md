# Performance baseline

Workgroup `edgepulse`, eu-north-1. Measured with `scripts/perf_baseline.py`. The same day (2026-11-06) is queried before and after maintenance so the numbers are comparable.

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

## Observations

- **Bronze has the small files problem.** The consumer commits one file and one snapshot per minute, so files average under 100 KB, far below the 128 to 512 MB that Parquet and Athena work best with. Every query opens and reads the footer of each file.
- **Q1 reads the whole day for one device.** Partition pruning limits it to one day, but rows for dev-0042 are spread across every file, so nothing else can be skipped. Sorting files by device_id during compaction should let Parquet min/max statistics skip most of them.
- **Silver scans far less than bronze** (60 KB vs 414 KB). It has fewer, larger files, and Q2 reads only two narrow columns.
- **Gold scans the most** (1,141 KB) despite having the fewest rows, because each row carries a latency digest (a binary quantile sketch). This is the cost of accurate percentiles at any level. If cost mattered at scale, the digest could move to a separate table read only by percentile queries.
- **Deduplication works.** Bronze holds 76,746 rows but 72,996 unique events, because 3,750 duplicates (4.9%) were injected in the resilience drill. Silver holds exactly 72,996.
- **Merges caused write amplification.** 43% of silver rows and 55% of gold rows on disk were dead. Every run re-read the 2-hour lookback, and MERGE rewrote matched rows even when nothing had changed. Found by comparing `$files` record counts with `"$path"` row counts per file. One silver file was entirely superseded.
- **Fixed with conditional updates.** Silver and `fct_waf_alert` rewrite a row only when a later copy arrives (`src._ingest_ts > target._ingest_ts`). The hourly gold facts rewrite an hour only when it received new events (`src._source_max_ingest_ts > target._source_max_ingest_ts`). Repeat silver and gold runs after the fix changed 0 rows, committed no new snapshots and dead rows stopped growing. Existing dead rows are removed by compaction on Day 4.
- **Cost context:** Athena bills a minimum of 10 MB per query, so at this volume the improvements show in file counts, dead rows, snapshot counts and run time more than in cost.

## After maintenance (Day 4)

Planned: `OPTIMIZE ... REWRITE DATA USING BIN_PACK` (sorted by device_id for bronze), `VACUUM` to expire old snapshots and remove orphan files, then rerun the script for the same day.

### Queries

| Query | Before (KB) | After (KB) | Change | Before (ms) | After (ms) |
|---|---|---|---|---|---|
| Q1 bronze | 414 | | | 701 | |
| Q2 silver | 60 | | | 664 | |
| Q3 gold | 1,141 | | | 1,220 | |

### Tables

| Table | Files before | Files after | Dead rows before | Dead rows after | Snapshots before | Snapshots after |
|---|---|---|---|---|---|---|
| ep_bronze.request_log | 62 | | 0 | | 62 | |
| ep_silver.stg_request_log | 4 | | 54,805 | | 4 | |
| ep_gold.fct_request_hourly_v2 | 3 | | 1,378 | | 3 | |