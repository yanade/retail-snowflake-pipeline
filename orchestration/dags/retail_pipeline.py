"""retail_pipeline: load_raw, dbt build and DVT for one manual window, each recorded in pipeline_audit (ADR-029)."""

from collections.abc import Callable
from pathlib import Path
from typing import Any

import pendulum
from airflow.sdk import Param, dag, get_current_context, task

DAG_NAME = "retail_pipeline"
DBT = Path("/opt/airflow/venv-dbt/bin/dbt")  # built into the image, orchestration/Dockerfile
DVT_PYTHON = Path("/opt/airflow/venv-dvt/bin/python")
WINDOW_END_PATTERN = r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$"  # last_watermark's format, UTC


def audited(work: Callable[[], Any], to_outcome: Callable[[Any], Any]) -> None:
    """
    Run one task's work inside run_and_record, keyed by the running task instance.

    Args:
        work: The task's work.
        to_outcome: Maps work's result to a TaskOutcome.
    """
    from audit.recorder import run_and_record  # run-time import keeps DAG parsing light

    ti = get_current_context()["ti"]
    run_and_record(work, to_outcome, dag_name=ti.dag_id, run_id=ti.run_id,
                   task_name=ti.task_id, try_number=ti.try_number)


@dag(
    dag_id=DAG_NAME,
    schedule=None,  # manual runs until Phase F: a required param rules out a schedule
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,  # explicit: the 3.x default comes from config
    max_active_runs=1,  # watermarks advance one run at a time
    is_paused_upon_creation=True,
    params={
        "window_end": Param(  # no default: a run without it is refused at trigger
            type="string",
            pattern=WINDOW_END_PATTERN,  # a malformed value is refused at trigger, not by DVT a minute later
            description="UTC end of the ADF load window, last_watermark in pipeline_watermark_control, e.g. 2026-10-13 20:05:18",
        ),
    },
    tags=["retail"],
)
def retail_pipeline() -> None:
    """Phase B: load the served exports, build dbt, validate with DVT; ADF and Databricks still run by hand."""

    @task
    def load_raw() -> None:
        """COPY the window's served exports into Snowflake raw and check each landed exactly once."""
        from audit.outcomes import load_raw_outcome
        from loading.load_raw import load_raw as copy_exports

        run_id = get_current_context()["ti"].run_id
        audited(copy_exports, lambda checks: load_raw_outcome(checks, run_id))

    @task
    def dbt_build() -> None:
        """Build and test every dbt model."""
        from audit.outcomes import dbt_outcome, load_dbt_results
        from pipeline.runners import DBT_PROJECT_DIR, run_dbt_build

        run_results = DBT_PROJECT_DIR / "target" / "run_results.json"
        audited(lambda: run_dbt_build(DBT), lambda code: dbt_outcome(load_dbt_results(run_results), code))

    @task
    def dvt() -> None:
        """Reconcile PostgreSQL up to the window end with Snowflake staging."""
        from audit.outcomes import dvt_outcome, load_dvt_results
        from pipeline.runners import DVT_RESULTS_DIR, run_dvt

        context = get_current_context()
        run_id, window_end = context["ti"].run_id, context["params"]["window_end"]
        results = DVT_RESULTS_DIR / f"{run_id}.json"
        audited(lambda: run_dvt(DVT_PYTHON, window_end, run_id),
                lambda code: dvt_outcome(load_dvt_results(results), code, run_id))

    load_raw() >> dbt_build() >> dvt()


retail_pipeline()