"""Weekly Iceberg maintenance: compact recent partitions, then expire old snapshots and orphan files."""
from datetime import datetime, timedelta

from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.sdk import TaskGroup, dag

from include.alerts import notify_failure

# table: column its day partition is based on
TABLES = {
    "ep_bronze.request_log": ("event_ts", True),
    "ep_bronze.waf_alert": ("event_ts", True),
    "ep_bronze.device_metric": ("event_ts", True),
    "ep_silver.stg_request_log": ("event_ts", False),
    "ep_silver.stg_waf_alert": ("event_ts", False),
    "ep_silver.stg_device_metric": ("event_ts", False),
    "ep_gold.fct_request_hourly_v2": ("event_hour", False),
    "ep_gold.fct_device_health_hourly": ("event_hour", False),
    "ep_gold.fct_waf_alert": ("event_ts", False),
}
RECENT_DAYS = 7

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "on_failure_callback": notify_failure,
}


@dag(
    schedule="30 3 * * 0",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=3,
    default_args=default_args,
    tags=["edgepulse", "maintenance"],
)
def edgepulse_maintenance():
    for table, (ts_column, has_tz) in TABLES.items():
        database = table.split(".")[0]
        with TaskGroup(group_id=table.replace(".", "__")):
            since = "{{ macros.ds_add(ds, -" + str(RECENT_DAYS) + ") }} 00:00:00" + (" UTC" if has_tz else "")
            optimize = AthenaOperator(
                task_id="optimize",
                query=f"OPTIMIZE {table} REWRITE DATA USING BIN_PACK WHERE {ts_column} >= TIMESTAMP '{since}'",
                database=database,
                workgroup="edgepulse",
                aws_conn_id="aws_edgepulse",
            )
            vacuum = AthenaOperator(
                task_id="vacuum",
                query=f"VACUUM {table}",
                database=database,
                workgroup="edgepulse",
                aws_conn_id="aws_edgepulse",
            )
            optimize >> vacuum


edgepulse_maintenance()