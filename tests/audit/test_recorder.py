"""Unit tests for audit/recorder.py: every path writes one row and the task's own error survives."""

import logging

import pytest

from audit.recorder import run_and_record
from audit.records import DVT_MISMATCH, FAILED, SUCCESS, TaskOutcome

IDENTITY = {"dag_name": "pipeline_dag", "run_id": "manual__1", "task_name": "load_raw", "try_number": 2}


def _succeeded(result: int) -> TaskOutcome:
    """A mapper that turns the result into a row count."""
    return TaskOutcome(SUCCESS, rows_ingested=result)


def _raise(error: Exception):
    """Let a lambda raise."""
    raise error


def _broken_write(record):
    """An audit write that always fails."""
    raise ConnectionError("snowflake unreachable")


def test_success_returns_the_result_and_writes_one_row():
    written = []

    assert run_and_record(lambda: 52, _succeeded, **IDENTITY, write=written.append) == 52

    [record] = written
    assert (record.outcome.status, record.outcome.rows_ingested, record.try_number) == (SUCCESS, 52, 2)
    assert record.started_at.tzinfo is not None and record.started_at <= record.finished_at


def test_task_error_is_recorded_and_re_raised_unchanged():
    written = []
    error = ValueError("raw differs from the column contract")

    with pytest.raises(ValueError) as raised:
        run_and_record(lambda: _raise(error), _succeeded, **IDENTITY, write=written.append)

    assert raised.value is error  # the same object, not a wrapper
    assert written[0].outcome.error_message == "ValueError: raw differs from the column contract"


def test_a_failing_audit_write_does_not_hide_the_task_error(caplog):
    with caplog.at_level(logging.ERROR), pytest.raises(ValueError, match="task broke"):
        run_and_record(lambda: _raise(ValueError("task broke")), _succeeded, **IDENTITY, write=_broken_write)

    assert "audit write failed" in caplog.text


def test_a_successful_task_that_cannot_be_recorded_fails():
    with pytest.raises(ConnectionError):
        run_and_record(lambda: 52, _succeeded, **IDENTITY, write=_broken_write)


def test_a_mapper_error_is_recorded_as_a_failure():
    written = []

    with pytest.raises(KeyError):
        run_and_record(lambda: {}, lambda result: _succeeded(result["rows"]), **IDENTITY, write=written.append)

    assert written[0].outcome.status == FAILED


def test_a_failed_outcome_is_written_then_fails_the_task():
    """DVT exits 1 without raising; the audit row and Airflow must still agree."""
    written = []
    mismatch = TaskOutcome(FAILED, DVT_MISMATCH, error_message="1 of 32 validations mismatched")

    with pytest.raises(RuntimeError, match="1 of 32"):
        run_and_record(lambda: 1, lambda exit_code: mismatch, **IDENTITY, write=written.append)

    assert written[0].outcome.dvt_status == DVT_MISMATCH
