"""Structure tests for the retail_pipeline DAG; they run in venv-orchestration and skip where Airflow is absent."""

import copy
from pathlib import Path

import pytest

pytest.importorskip("airflow")  # the main venv has no Airflow

from airflow.dag_processing.dagbag import DagBag  # noqa: E402
from airflow.sdk.exceptions import ParamValidationError  # noqa: E402

from validation.dvt_suite import parse_window_end  # noqa: E402

DAGS_FOLDER = Path(__file__).resolve().parent.parent / "dags"
WATERMARK = "2026-10-13 20:05:18"


@pytest.fixture(scope="module")
def dag():
    """retail_pipeline parsed as the dag-processor parses it, without a metadata database."""
    bag = DagBag(dag_folder=DAGS_FOLDER)
    assert bag.import_errors == {}
    return bag.dags["retail_pipeline"]  # get_dag() would query the metadata database


def test_tasks_run_load_raw_then_dbt_build_then_dvt(dag):
    assert {task.task_id for task in dag.tasks} == {"load_raw", "dbt_build", "dvt"}
    assert dag.get_task("load_raw").downstream_task_ids == {"dbt_build"}
    assert dag.get_task("dbt_build").downstream_task_ids == {"dvt"}
    assert dag.get_task("dvt").downstream_task_ids == set()


def test_runs_only_when_triggered_and_one_at_a_time(dag):
    assert not dag.timetable.can_be_scheduled  # schedule=None until Phase F
    assert dag.max_active_runs == 1


def test_a_run_without_window_end_is_refused(dag):
    with pytest.raises(ParamValidationError):
        copy.deepcopy(dag.params).validate()


@pytest.mark.parametrize("value", ["2026-10-13 HH:MM:SS", "2026-10-13T20:05:18", "<last_watermark>"])
def test_window_end_must_look_like_a_watermark(dag, value):
    params = copy.deepcopy(dag.params)
    with pytest.raises(ParamValidationError):
        params["window_end"] = value


def test_a_watermark_is_accepted_and_dvt_can_parse_it(dag):
    params = copy.deepcopy(dag.params)
    params["window_end"] = WATERMARK

    assert params.validate() == {"window_end": WATERMARK}
    parse_window_end(WATERMARK)  # what the param lets through, run_validations accepts