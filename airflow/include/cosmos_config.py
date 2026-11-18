"""Cosmos settings shared by every EdgePulse DAG that runs dbt."""
from pathlib import Path

from cosmos import ExecutionConfig, ProfileConfig, ProjectConfig
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