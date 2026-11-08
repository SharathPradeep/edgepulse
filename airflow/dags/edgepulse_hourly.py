"""Hourly EdgePulse transformation: silver then gold, one Airflow task per dbt model."""
from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import dag
from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import TestBehavior
from cosmos.profiles import AthenaAccessKeyProfileMapping

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
    silver = DbtTaskGroup(
        group_id="silver",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(select=["path:models/silver"], test_behavior=TestBehavior.AFTER_EACH),
    )

    gold = DbtTaskGroup(
        group_id="gold",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(select=["path:models/gold"], test_behavior=TestBehavior.AFTER_EACH),
    )

    silver >> gold


edgepulse_hourly()