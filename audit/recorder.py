"""Run one task and record its outcome in pipeline_audit without hiding the task's own error (ADR-026)."""

import logging
from collections.abc import Callable
from datetime import datetime, timezone
from typing import TypeVar

from audit.outcomes import failure_outcome
from audit.records import FAILED, AuditRecord, TaskOutcome
from audit.writer import record_task

T = TypeVar("T")

logger = logging.getLogger(__name__)


def run_and_record(
    work: Callable[[], T],
    to_outcome: Callable[[T], TaskOutcome],
    dag_name: str,
    run_id: str,
    task_name: str,
    try_number: int | None = None,
    write: Callable[[AuditRecord], None] = record_task,
) -> T:
    """
    Run a task, write its audit row, and fail the same way the task did.

    Args:
        work: The task itself.
        to_outcome: Maps work's result to an outcome, e.g. load_raw_outcome.
        dag_name: Row identity, from the Airflow context, like run_id, task_name and try_number.
        write: Persists one record; record_task in production, a list's append in tests.

    Returns:
        What work returned.

    Raises:
        The task's own exception after recording it; RuntimeError for a FAILED outcome such as a DVT
        mismatch; the audit error when a successful task could not be recorded.
    """
    started_at = _now()

    def build(outcome: TaskOutcome) -> AuditRecord:
        return AuditRecord(dag_name, run_id, task_name, outcome, try_number, started_at, _now())

    try:
        result = work()
        outcome = to_outcome(result)  # a mapper bug is recorded like any other failure
    except Exception as error:
        _write_quietly(write, build(failure_outcome(error)))
        raise  # the task's own error and traceback, never the audit one
    write(build(outcome))  # not swallowed: an unrecorded success is not done, and a retry is safe
    if outcome.status == FAILED:
        raise RuntimeError(outcome.error_message)  # e.g. DVT exit 1: Airflow must fail the task too
    return result


def _write_quietly(write: Callable[[AuditRecord], None], record: AuditRecord) -> None:
    """Write a failure record; if that fails too, log it so the caller can re-raise the task's error."""
    try:
        write(record)
    except Exception:
        logger.exception("audit write failed for %s %s %s", record.dag_name, record.run_id, record.task_name)


def _now() -> datetime:
    """Current time, UTC, timezone-aware."""
    return datetime.now(timezone.utc)
