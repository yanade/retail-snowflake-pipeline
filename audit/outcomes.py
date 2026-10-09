"""Map each task's output to a TaskOutcome for pipeline_audit (ADR-026)."""

import json
from dataclasses import asdict
from pathlib import Path

from audit.records import DVT_MATCH, DVT_MISMATCH, DVT_SKIPPED, FAILED, SKIPPED, SUCCESS, TaskOutcome
from loading.raw_loader import ExportCheck
from validation.dvt_suite import COUNT_TABLES, RECONCILIATIONS, SOURCE_SCHEMA, failed_rows

DVT_EXIT_MATCH, DVT_EXIT_MISMATCH = 0, 1  # run_validations: 0 all matched, 1 any mismatch
EXPECTED_DVT_CHECKS = (
    *(f"{SOURCE_SCHEMA}.{table}" for table in COUNT_TABLES),  # run_suite labels counts by source table
    *(check.table for check in RECONCILIATIONS),
)
FAILED_CHECK_FIELDS = (
    "check", "validation_name", "group_by_columns", "source_agg_value", "target_agg_value", "validation_status",
)
MAX_ERROR_CHARS = 500  # the full traceback stays in the Airflow log


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


def raw_to_curated_outcome(metrics: dict[str, dict[str, int]]) -> TaskOutcome:
    """
    Map 01_raw_to_curated's exit JSON; rejected rows went to dead-letter, so the task still succeeded.

    Args:
        metrics: {table: {rows_read, rows_valid, rows_rejected}} from the notebook.

    Returns:
        SUCCESS with valid and rejected rows summed over tables.
    """
    return TaskOutcome(
        SUCCESS,
        rows_ingested=sum(counts["rows_valid"] for counts in metrics.values()),  # after dedupe: what MERGE writes
        rows_failed=sum(counts["rows_rejected"] for counts in metrics.values()),
        details={"tables": metrics},  # keeps rows_read: read minus valid minus rejected is duplicates dropped
    )


def curated_to_served_outcome(summaries: dict[str, dict | None]) -> TaskOutcome:
    """
    Map 02_curated_to_served's exit JSON.

    Args:
        summaries: {table: export summary, or None when nothing changed} from the notebook.

    Returns:
        SUCCESS with exported rows summed; 0 when no table changed.
    """
    exported = [summary for summary in summaries.values() if summary is not None]  # None: checked, nothing new
    return TaskOutcome(
        SUCCESS,
        rows_ingested=sum(summary["row_count"] for summary in exported),
        details={"tables": summaries},
    )


def load_raw_outcome(checks: list[ExportCheck], run_id: str) -> TaskOutcome:
    """
    Map load_raw's checks to rows in raw from this run's exports.

    Args:
        checks: From load_raw(); it raises before returning if any export is not in raw exactly once.
        run_id: The DAG run, as 02_curated_to_served wrote it to the manifest.

    Returns:
        SUCCESS with this run's rows; every export in the window goes to details.
    """
    this_run = [check for check in checks if check.run_id == run_id]
    return TaskOutcome(
        SUCCESS,
        rows_ingested=sum(check.actual for check in this_run),  # state, not COPY's report: the same on every retry
        details={"exports": [asdict(check) for check in checks]},
    )


def failure_outcome(error: BaseException) -> TaskOutcome:
    """
    Record a task that raised: exception type and first message line, capped.

    Args:
        error: What the task raised.

    Returns:
        FAILED with no counts and no dvt_status.
    """
    first_line = (str(error).splitlines() or [""])[0]  # run_dvt puts stderr after the first line
    return TaskOutcome(FAILED, error_message=f"{type(error).__name__}: {first_line}"[:MAX_ERROR_CHARS])


def skipped_outcome(reason: str, validates: bool = False) -> TaskOutcome:
    """
    Record a task that was not run.

    Args:
        reason: Why, e.g. upstream_failed or no_new_data.
        validates: True for the DVT task, so dvt_status says SKIPPED instead of NULL.

    Returns:
        SKIPPED with the reason in details.
    """
    return TaskOutcome(SKIPPED, DVT_SKIPPED if validates else None, details={"skip_reason": reason})
