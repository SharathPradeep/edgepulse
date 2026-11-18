"""Targeted reprocessing: rebuild one event-time window from bronze through silver and gold.

Trigger it manually from the Airflow UI and enter a start and end hour (UTC). Silver re-reads bronze
by event time for that window and merges with updates allowed, so bronze wins. Gold recomputes the
same hours. Tests run after every model, so a repair is only published if it passes.
Pause edgepulse_hourly while this runs, so two DAGs never merge into the same table at once.
"""
from datetime import datetime, timedelta

from airflow.exceptions import AirflowFailException
from airflow.sdk import Param, dag, get_current_context, task
from cosmos import DbtTaskGroup, RenderConfig
from cosmos.constants import TestBehavior

from include.alerts import notify_failure
from include.cosmos_config import execution_config, profile_config, project_config

WHOLE_HOUR = r"^\d{4}-\d{2}-\d{2} \d{2}:00:00$"   # whole hours keep gold's hourly rows complete
MAX_WINDOW_HOURS = 48                              # guard rail on how much one run can rewrite

# Templated at run time, so every dbt task gets this run's window as dbt vars
REPROCESS_VARS = {"vars": {"reprocess_start": "{{ params.start }}", "reprocess_end": "{{ params.end }}"}}


@dag(
    schedule=None,
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2), "on_failure_callback": notify_failure},
    params={
        "start": Param("2026-11-16 00:00:00", type="string", pattern=WHOLE_HOUR,
                       description="Window start in UTC, inclusive. Format YYYY-MM-DD HH:00:00"),
        "end": Param("2026-11-16 01:00:00", type="string", pattern=WHOLE_HOUR,
                     description="Window end in UTC, exclusive. Format YYYY-MM-DD HH:00:00"),
    },
    tags=["edgepulse", "dbt", "reprocess"],
)
def edgepulse_reprocess():
    @task(retries=0)
    def check_window():
        params = get_current_context()["params"]
        start = datetime.strptime(params["start"], "%Y-%m-%d %H:%M:%S")
        end = datetime.strptime(params["end"], "%Y-%m-%d %H:%M:%S")
        hours = (end - start).total_seconds() / 3600
        if not 0 < hours <= MAX_WINDOW_HOURS:
            raise AirflowFailException(f"Window must be 1 to {MAX_WINDOW_HOURS} hours, got {hours:g}")
        print(f"Reprocessing {hours:g} hour(s): {start} to {end} UTC")

    silver = DbtTaskGroup(
        group_id="silver",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(select=["path:models/silver"], test_behavior=TestBehavior.AFTER_EACH),
        operator_args=REPROCESS_VARS,
    )

    gold = DbtTaskGroup(
        group_id="gold",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(select=["path:models/gold"], test_behavior=TestBehavior.AFTER_EACH),
        operator_args=REPROCESS_VARS,
    )

    check_window() >> silver >> gold


edgepulse_reprocess()