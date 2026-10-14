"""retail_pipeline: Phase A smoke task, proves the image, venvs, mounts and the audit write (ADR-029)."""

import subprocess

import pendulum
from airflow.sdk import dag, get_current_context, task

DAG_NAME = "retail_pipeline"
VENV_PACKAGES = {  # each tool venv's interpreter, and the package that names it
    "/opt/airflow/venv-dbt/bin/python": "dbt-core",
    "/opt/airflow/venv-dvt/bin/python": "google-pso-data-validator",
}


def venv_versions() -> dict[str, str]:
    """
    Ask each tool venv's own interpreter for its package version.

    Returns:
        Package name to installed version.
    """
    versions = {}
    for python, package in VENV_PACKAGES.items():
        result = subprocess.run(
            [python, "-c", f"import importlib.metadata as m; print(m.version('{package}'))"],
            capture_output=True,
            text=True,
            check=True,  # a broken venv raises, so the task fails loudly
        )
        versions[package] = result.stdout.strip()
    return versions


@dag(
    dag_id=DAG_NAME,
    schedule="@daily",
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,  # explicit: the 3.x default comes from config
    max_active_runs=1,  # watermarks advance one run at a time
    is_paused_upon_creation=True,
    tags=["retail"],
)
def retail_pipeline() -> None:
    """Phase A: one smoke task; Phase B replaces it with the processing chain."""

    @task
    def smoke_check() -> dict[str, str]:
        """
        Check both tool venvs and record the outcome in pipeline_audit.

        Returns:
            Package name to installed version.
        """
        from audit.recorder import run_and_record  # run-time import keeps DAG parsing light
        from audit.records import SUCCESS, TaskOutcome

        ti = get_current_context()["ti"]
        return run_and_record(
            work=venv_versions,
            to_outcome=lambda versions: TaskOutcome(SUCCESS, details=versions),
            dag_name=ti.dag_id,
            run_id=ti.run_id,
            task_name=ti.task_id,
            try_number=ti.try_number,
        )

    smoke_check()


retail_pipeline()