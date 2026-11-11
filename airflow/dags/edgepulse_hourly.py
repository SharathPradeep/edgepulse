"""Hourly EdgePulse pipeline: bronze freshness gate, silver, then gold and data quality checks."""
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.exceptions import AirflowFailException
from airflow.sdk import dag, get_current_context, task
from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import SourceRenderingBehavior, TestBehavior
from cosmos.profiles import AthenaAccessKeyProfileMapping

from include.alerts import notify_failure
from include.dq.checks import CHECKS, evaluate

DBT_PROJECT = Path("/usr/local/airflow/include/dbt")
DBT_EXECUTABLE = "/usr/local/airflow/dbt_venv/bin/dbt"

profile_config = ProfileConfig(
    profile_name="edgepulse",
    target_name="dev",
    profile_mapping=AthenaAccessKeyProfileMapping(
        conn_id="aws_edgepulse",
        profile_args={
            "database": "awsdatacatalog",
            "schema": "ep_silver",
            "region_name": "eu-north-1",
            "work_group": "edgepulse",
            "s3_staging_dir": "s3://edgepulse-lake-sharath-0001/athena-results/dbt/",
            "s3_data_dir": "s3://edgepulse-lake-sharath-0001/",
            "s3_data_naming": "schema_table",
            "threads": 4,
            "num_retries": 3,
        },
    ),
)

project_config = ProjectConfig(DBT_PROJECT)
execution_config = ExecutionConfig(dbt_executable_path=DBT_EXECUTABLE)

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=15),
    "on_failure_callback": notify_failure,
}


@dag(
    schedule="@hourly",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["edgepulse", "dbt"],
)
def edgepulse_hourly():
    # Silver: bronze freshness tasks run first, then each silver model with its tests
    silver = DbtTaskGroup(
        group_id="silver",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(
            select=["path:models/silver", "source:bronze"],
            test_behavior=TestBehavior.AFTER_EACH,
            source_rendering_behavior=SourceRenderingBehavior.WITH_TESTS_OR_FRESHNESS,
        ),
    )

    # Gold: each gold model with its tests
    gold = DbtTaskGroup(
        group_id="gold",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(
            select=["path:models/gold"],
            test_behavior=TestBehavior.AFTER_EACH,
        ),
    )

    # Pipeline health checks, results written to ep_ops.dq_results
    @task(retries=0)
    def run_dq_checks():
        from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

        athena = AwsBaseHook(aws_conn_id="aws_edgepulse", client_type="athena").get_conn()

        def query(sql):
            qid = athena.start_query_execution(QueryString=sql, WorkGroup="edgepulse")["QueryExecutionId"]
            while True:
                status = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]
                if status["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
                    break
                time.sleep(1)
            if status["State"] != "SUCCEEDED":
                raise RuntimeError(status.get("StateChangeReason"))
            return athena.get_query_results(QueryExecutionId=qid)["ResultSet"]["Rows"]

        run_id = get_current_context()["run_id"]
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        results = []
        for check in CHECKS:
            rows = query(check["sql"])
            raw = rows[1]["Data"][0].get("VarCharValue")
            value = float(raw) if raw is not None else None
            status = evaluate(value, check)
            thresholds = {k: v for k, v in check.items() if k.startswith(("warn", "error"))}
            results.append((check, value, status, thresholds))
            print(f"{check['name']}: {value} -> {status}")

        values = ",\n".join(
            f"('{run_id}', '{c['name']}', '{c['table']}', {'null' if v is None else v}, "
            f"'{s}', '{str(t).replace(chr(39), chr(34))}', timestamp '{now}')"
            for c, v, s, t in results
        )
        query(f"insert into ep_ops.dq_results values {values}")

        errors = [c["name"] for c, _, s, _ in results if s == "error"]
        if errors:
            raise AirflowFailException(f"Data quality errors: {errors}")

    dq_checks = run_dq_checks()

    # Gold and the checks both wait for silver, and run side by side
    silver >> [gold, dq_checks]


edgepulse_hourly()