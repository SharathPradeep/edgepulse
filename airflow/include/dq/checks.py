"""Pipeline health checks run after silver. Each query returns one number."""

CHECKS = [
    {
        "name": "bronze_duplicate_rate",
        "table": "ep_bronze.request_log",
        "sql": """
            select 1 - cast(count(distinct event_id) as double) / nullif(count(*), 0)
            from ep_bronze.request_log
            where _ingest_ts > current_timestamp - interval '2' hour""",
        "warn_above": 0.01,
        "error_above": 0.03,
    },
    {
        "name": "silver_late_rate",
        "table": "ep_silver.stg_request_log",
        "sql": """
            select avg(case when is_late then 1.0 else 0.0 end)
            from ep_silver.stg_request_log
            where _ingest_ts > cast(current_timestamp at time zone 'UTC' as timestamp(6)) - interval '2' hour""",
        "warn_above": 0.05,
        "error_above": 0.10,
    },
    {
        "name": "silver_rows_last_hour",
        "table": "ep_silver.stg_request_log",
        "sql": """
            select count(*)
            from ep_silver.stg_request_log
            where _ingest_ts > cast(current_timestamp at time zone 'UTC' as timestamp(6)) - interval '1' hour""",
        "warn_below": 10000,
        "error_below": 1000,
        "error_above": 200000,
    },
    {
        "name": "silver_dead_row_ratio",
        "table": "ep_silver.stg_request_log",
        "sql": """
            select 1 - cast((select count(*) from ep_silver.stg_request_log) as double)
                     / nullif((select sum(record_count) from "ep_silver"."stg_request_log$files"), 0)""",
        "warn_above": 0.20,
        "error_above": 0.50,
    },
]


def evaluate(value, check):
    """Return pass, warn or error for one metric."""
    if value is None:
        return "warn"
    if value > check.get("error_above", float("inf")) or value < check.get("error_below", float("-inf")):
        return "error"
    if value > check.get("warn_above", float("inf")) or value < check.get("warn_below", float("-inf")):
        return "warn"
    return "pass"