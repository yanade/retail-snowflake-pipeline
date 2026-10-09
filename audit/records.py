"""Rows for pipeline_audit and the mapping from DVT results to them (ADR-026)."""

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from validation.dvt_suite import COUNT_TABLES, RECONCILIATIONS, SOURCE_SCHEMA, failed_rows

SUCCESS, FAILED, SKIPPED = "SUCCESS", "FAILED", "SKIPPED"  # did the task run, Airflow's words
DVT_MATCH, DVT_MISMATCH, DVT_SKIPPED = "MATCH", "MISMATCH", "SKIPPED"  # did source and target agree
STATUSES = (SUCCESS, FAILED, SKIPPED)
DVT_STATUSES = (DVT_MATCH, DVT_MISMATCH, DVT_SKIPPED)
DVT_EXIT_MATCH, DVT_EXIT_MISMATCH = 0, 1  # run_validations: 0 all matched, 1 any mismatch
EXPECTED_DVT_CHECKS = (
    *(f"{SOURCE_SCHEMA}.{table}" for table in COUNT_TABLES),  # run_suite labels counts by source table
    *(check.table for check in RECONCILIATIONS),
)
FAILED_CHECK_FIELDS = (
    "check", "validation_name", "group_by_columns", "source_agg_value", "target_agg_value", "validation_status",
)


@dataclass(frozen=True)
class TaskOutcome:
    """What one task produced, before Airflow adds which run and when."""

    status: str
    dvt_status: str | None = None
    rows_ingested: int | None = None  # None means not measured, 0 means measured and none
    rows_failed: int | None = None
    error_message: str | None = None
    details: dict | None = None

    def __post_init__(self) -> None:
        """Reject combinations the audit contract does not allow; Snowflake has no CHECK constraints."""
        if self.status not in STATUSES:
            raise ValueError(f"status must be one of {STATUSES}, got {self.status!r}")
        if self.dvt_status not in (None, *DVT_STATUSES):
            raise ValueError(f"dvt_status must be one of {DVT_STATUSES} or None, got {self.dvt_status!r}")
        if self.status == FAILED and not self.error_message:
            raise ValueError("a FAILED outcome needs an error_message")
        if self.dvt_status == DVT_MATCH and self.status != SUCCESS:
            raise ValueError("dvt_status MATCH needs status SUCCESS")
        if self.dvt_status == DVT_MISMATCH and self.status != FAILED:
            raise ValueError("dvt_status MISMATCH needs status FAILED")  # the runner exits 1, so Airflow fails the task
        for name in ("rows_ingested", "rows_failed"):
            if (getattr(self, name) or 0) < 0:
                raise ValueError(f"{name} cannot be negative, got {getattr(self, name)}")


@dataclass(frozen=True)
class AuditRecord:
    """One pipeline_audit row: which task of which run, and what it produced."""

    dag_name: str
    run_id: str
    task_name: str
    outcome: TaskOutcome
    try_number: int | None = None
    started_at: datetime | None = None  # UTC
    finished_at: datetime | None = None  # UTC

    def __post_init__(self) -> None:
        """Reject an empty key part; NOT NULL in Snowflake still accepts ''."""
        for name in ("dag_name", "run_id", "task_name"):
            if not getattr(self, name):
                raise ValueError(f"{name} must not be empty")


def load_dvt_results(path: Path) -> list[dict] | None:
    """
    Rows of a run_validations results file.

    Args:
        path: validation/results/<run_id>.json.

    Returns:
        The rows, or None when the runner wrote no file.
    """
    if not path.exists():
        return None  # the runner stopped before write_results: unknown, not a mismatch
    return json.loads(path.read_text())


def dvt_outcome(rows: list[dict] | None, exit_code: int, run_id: str) -> TaskOutcome:
    """
    Map one run_validations run to an outcome; only a complete, consistent result is MATCH or MISMATCH.

    Args:
        rows: From load_dvt_results(); None when no file was written.
        exit_code: The runner's exit code.
        run_id: The run this audit row is for.

    Returns:
        SUCCESS/MATCH, FAILED/MISMATCH, or FAILED with no dvt_status when the result is unusable.
    """
    if rows is None:
        return _unusable(f"no results file, exit code {exit_code}")
    other_runs = sorted({str(row.get("run_id")) for row in rows} - {run_id})
    if other_runs:
        return _unusable(f"results belong to another run: {', '.join(other_runs)}")
    reported = {row.get("check") for row in rows}
    missing = [check for check in EXPECTED_DVT_CHECKS if check not in reported]
    if missing:
        return _unusable(f"no result for: {', '.join(missing)}")  # an empty file lands here, never MATCH

    failures = failed_rows(rows)
    if exit_code == DVT_EXIT_MATCH and not failures:
        return TaskOutcome(SUCCESS, DVT_MATCH, details={"validations": len(rows)})
    if exit_code == DVT_EXIT_MISMATCH and failures:
        return TaskOutcome(
            FAILED, DVT_MISMATCH,
            error_message=f"{len(failures)} of {len(rows)} validations mismatched",
            details={
                "validations": len(rows),
                "failed": [{field: row.get(field) for field in FAILED_CHECK_FIELDS} for row in failures],
            },
        )
    return _unusable(f"exit code {exit_code} disagrees with {len(failures)} failed of {len(rows)}")


def _unusable(reason: str) -> TaskOutcome:
    """The DVT task failed without a result to trust: not a data mismatch, so no dvt_status."""
    return TaskOutcome(FAILED, error_message=f"DVT result unusable: {reason}")
