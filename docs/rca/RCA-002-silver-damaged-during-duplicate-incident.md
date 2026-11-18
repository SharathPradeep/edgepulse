# RCA-002: Silver damaged by a manual cleanup during a duplicate surge

> **Simulated incident.** This was a planned failure drill (Block 4.4 b) on synthetic data, staged to exercise detection, repair and verification. Times marked "about" are approximate.

| | |
|---|---|
| Status | Resolved |
| Severity | Medium (one hour of silver incorrect; gold and bronze unaffected) |
| Detected | 2026-11-18, about 07:36 UTC, by the dbt test `assert_silver_request_log_complete` (missing rows only) |
| Resolved | 2026-11-18, about 07:45 UTC, by an `edgepulse_reprocess` run |
| Affected | `ep_silver.stg_request_log`, event hour 2026-11-16 13:00 to 14:00 UTC |

## Summary

A producer retry storm resent about 30% of request events for 15 minutes. The `bronze_duplicate_rate` check raised an error while silver and gold stayed correct, because silver merges on `event_id`. During triage, two manual statements were run against silver to "clean up" duplicates: a DELETE that removed one Kafka partition's rows for one hour and an UPDATE that set `status_code` to 500 on another partition's rows in the same hour. The duplicates had never reached silver, so the cleanup only caused damage. The completeness test caught the missing rows but not the changed values. The hour was rebuilt from bronze with the new targeted reprocessing DAG and Iceberg time travel proved it matched its pre-incident state row for row.

## Impact

| Measure | Value |
|---|---|
| Rows in the hour before the incident | 49,930 |
| Rows deleted | 14,702 (29%) |
| Rows differing from the pre-incident snapshot (deleted plus wrong `status_code`) | about 30,200 (exact figure not recorded) |
| Rows with a wrong `status_code` (derived) | about 15,500 |
| Rows differing after the repair | 0 |
| Gold `fct_request_hourly_v2` for the hour | Unaffected: not recomputed between the damage and the repair (49,930 requests after the repair) |

Anyone querying silver for that hour would have seen 29% of requests missing and an inflated server error rate.

## Timeline (UTC, 2026-11-18)

| Time | Event |
|---|---|
| About 07:10 | Duplicate surge starts: the producer resends about 30% of events (`DUPLICATE_RATE=0.30`) |
| 07:27:57 | Hourly run commits silver (snapshot `5790686382821901122`). Gold green. `run_dq_checks` fails on `bronze_duplicate_rate` and logs an `ALERT` |
| About 07:30 | Triage: thousands of extra copies in bronze, 0 duplicates in silver. Producer reset, stream stopped, hourly DAG paused |
| 07:34:29 | Manual DELETE on silver for event hour 2026-11-16 13:00, Kafka partition 1 (snapshot `412463031726354149`) |
| 07:35:19 | Manual UPDATE sets `status_code = 500` for partition 2 rows in the same hour (snapshot `721999993765208967`) |
| About 07:36 | `assert_silver_request_log_complete` fails on the missing rows. No test flags the changed values |
| About 07:40 | Time travel against snapshot `5790686382821901122` measures about 30,200 damaged rows |
| About 07:45 | `edgepulse_reprocess` runs for 2026-11-16 13:00 to 14:00. Silver and gold green, completeness test passes |
| About 07:47 | Verification: 49,930 rows before, 35,228 after the DELETE, 49,930 after the repair. Row-for-row differences: 0. Gold: 49,930 |

## Detection

- **The duplicate surge** was detected automatically on the first hourly run, as designed.
- **The missing rows** were detected by the completeness test, which compares bronze event IDs with silver.
- **The changed values were not detected by any check.** `status_code = 500` is a valid value, so uniqueness, not-null, accepted-values and relationship tests all passed. The damage was only measured through a time-travel comparison.

## Root cause (five whys)

1. **Why was silver wrong?** Two manual statements deleted and changed rows in one hour.
2. **Why were they run?** To clean up duplicates under the pressure of an active alert.
3. **Why did cleanup seem necessary?** Nothing told the responder that silver deduplicates by design. There was no runbook, and the alert reported the bronze rate without saying whether published data was affected.
4. **Why could a person change silver directly?** The same AWS identity is used for dbt and for manual queries, so anyone with access can run DML on pipeline-owned tables.
5. **Why were the changed values not caught?** Tests check structure and completeness. A change that stays within valid values passes all of them.

**Root cause:** manual DML on a pipeline-owned table during an incident, with no documented safe response, combined with tests that detect missing rows but not changed values.

## Resolution

The hour was rebuilt from bronze with `edgepulse_reprocess` (start `2026-11-16 13:00:00`, end `2026-11-16 14:00:00`). In reprocess mode silver reads bronze by event time for the window and overwrites every matched row, and gold recomputes the same hours. Deleted rows came back through the merge's insert branch and changed rows were overwritten through its update branch.

**Verification:** row counts returned to 49,930. Comparing the repaired hour with snapshot `5790686382821901122` on `event_id`, `status_code`, `latency_ms`, `client_ip_hash` and `is_late` found 0 differences. Gold matched at 49,930 requests.

## What went well

- The duplicate check alerted without blocking gold, and silver's merge kept the published data correct during the surge.
- Bronze kept every raw event, so a full repair was possible.
- Iceberg snapshots made it possible to measure the damage exactly and prove the repair.
- The repair was one triggered run with tests gating it, not hand-written SQL.

## What could be better

- Manual DML on silver was possible and seemed reasonable under pressure.
- Value-level damage went unnoticed by every automated check.
- Time travel only covers about a day of snapshots, so older damage could be repaired but not measured this way.
- The duplicate alert did not state impact, so it invited an unnecessary fix.

## Prevention

| Action | Status |
|---|---|
| Runbooks for duplicate surges (RB-05) and data repair (RB-06), stating "no manual DML on silver or gold" | Done |
| Targeted reprocessing DAG as the only repair path | Done (`edgepulse_reprocess`) |
| Hourly reconciliation of closed hours: bronze unique events, silver rows and gold requests must match | Proposed |
| Value-level check: compare a per-hour checksum of key columns between bronze-derived and silver data | Proposed |
| Separate identities: the pipeline role writes silver and gold, people get read-only access | Proposed (production) |
| Add the impact to the duplicate alert ("silver deduplicates, see RB-05") | Proposed |

## Evidence

Snapshots:
```sql
select committed_at, snapshot_id, operation
from "ep_silver"."stg_request_log$snapshots"
order by committed_at desc limit 4;
```

Row counts at three points:
```sql
select 'before delete' as state, count(*) as silver_rows
from ep_silver.stg_request_log for version as of 5790686382821901122
where event_ts >= timestamp '2026-11-16 13:00:00' and event_ts < timestamp '2026-11-16 14:00:00'
union all
select 'after delete', count(*)
from ep_silver.stg_request_log for version as of 412463031726354149
where event_ts >= timestamp '2026-11-16 13:00:00' and event_ts < timestamp '2026-11-16 14:00:00'
union all
select 'after repair', count(*)
from ep_silver.stg_request_log
where event_ts >= timestamp '2026-11-16 13:00:00' and event_ts < timestamp '2026-11-16 14:00:00';
```

Row-for-row comparison (about 30,200 before the repair, 0 after):
```sql
select count(*) as differences from (
  select event_id, status_code, latency_ms, client_ip_hash, is_late
  from ep_silver.stg_request_log for version as of 5790686382821901122
  where event_ts >= timestamp '2026-11-16 13:00:00' and event_ts < timestamp '2026-11-16 14:00:00'
  except
  select event_id, status_code, latency_ms, client_ip_hash, is_late
  from ep_silver.stg_request_log
  where event_ts >= timestamp '2026-11-16 13:00:00' and event_ts < timestamp '2026-11-16 14:00:00'
);
```