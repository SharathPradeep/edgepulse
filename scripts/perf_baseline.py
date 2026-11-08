"""Run three typical queries and table file stats, then print markdown for docs/perf.md.

Usage: python scripts/perf_baseline.py --day 2026-11-07 --label before
"""
import argparse
import statistics
import time

import boto3

WORKGROUP = "edgepulse"
athena = boto3.Session(profile_name="edgepulse", region_name="eu-north-1").client("athena")

QUERIES = {
    "Q1 bronze, one device, one day": """
        select count(*), avg(latency_ms) from ep_bronze.request_log
        where event_ts >= timestamp '{day} 00:00:00 UTC'
          and event_ts <  timestamp '{day} 00:00:00 UTC' + interval '1' day
          and device_id = 'dev-0042'""",
    "Q2 silver, error rate by version": """
        select product_version, count(*), avg(case when status_code >= 500 then 1.0 else 0 end)
        from ep_silver.stg_request_log
        where event_ts >= timestamp '{day} 00:00:00'
          and event_ts <  timestamp '{day} 00:00:00' + interval '1' day
        group by 1""",
    "Q3 gold, p95 by version": """
        select d.product_version,
               value_at_quantile(merge(cast(f.latency_digest as qdigest(double))), 0.95)
        from ep_gold.fct_request_hourly_v2 f
        join ep_gold.dim_device d on f.device_id = d.device_id
        where f.event_hour >= timestamp '{day} 00:00:00'
          and f.event_hour <  timestamp '{day} 00:00:00' + interval '1' day
        group by 1""",
}

TABLES = [
    ("ep_bronze", "request_log"),
    ("ep_silver", "stg_request_log"),
    ("ep_gold", "fct_request_hourly_v2"),
]


def run(sql):
    qid = athena.start_query_execution(QueryString=sql, WorkGroup=WORKGROUP)["QueryExecutionId"]
    while True:
        q = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]
        state = q["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(1)
    if state != "SUCCEEDED":
        raise RuntimeError(f"{state}: {q['Status'].get('StateChangeReason')}")
    return qid, q["Statistics"]


def first_row(qid):
    rows = athena.get_query_results(QueryExecutionId=qid)["ResultSet"]["Rows"]
    return [c.get("VarCharValue") for c in rows[1]["Data"]]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--day", required=True, help="A full day of data, YYYY-MM-DD")
    ap.add_argument("--label", default="before")
    args = ap.parse_args()

    print(f"\n### Queries ({args.label}, day {args.day})\n")
    print("| Query | Data scanned (KB) | Median run time (ms) |\n|---|---|---|")
    for name, sql in QUERIES.items():
        runs = [run(sql.format(day=args.day))[1] for _ in range(3)]
        kb = runs[0]["DataScannedInBytes"] / 1024
        ms = statistics.median(r["EngineExecutionTimeInMillis"] for r in runs)
        print(f"| {name} | {kb:,.0f} | {ms:,.0f} |")

    print(f"\n### Tables ({args.label})\n")
    print("| Table | Data files | Rows in files | Visible rows | Dead rows | Avg data file (KB) | Snapshots |")
    print("|---|---|---|---|---|---|---|")
    for db, table in TABLES:
        qid, _ = run(f'select count(*), sum(record_count), avg(file_size_in_bytes) / 1024 from "{db}"."{table}$files"')
        files, file_rows, avg_kb = first_row(qid)
        qid, _ = run(f"select count(*) from {db}.{table}")
        visible = first_row(qid)[0]
        qid, _ = run(f'select count(*) from "{db}"."{table}$snapshots"')
        snaps = first_row(qid)[0]
        dead = int(file_rows) - int(visible)
        print(f"| {db}.{table} | {files} | {int(file_rows):,} | {int(visible):,} | {dead:,} | {float(avg_kb):,.1f} | {snaps} |")


if __name__ == "__main__":
    main()