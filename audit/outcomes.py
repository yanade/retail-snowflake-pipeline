"""Map each task's output to a TaskOutcome for pipeline_audit (ADR-026)."""

import json
from pathlib import Path

from audit.records import DVT_MATCH, DVT_MISMATCH, FAILED, SUCCESS, TaskOutcome
from validation.dvt_suite import COUNT_TABLES, RECONCILIATIONS, SOURCE_SCHEMA, failed_rows

DVT_EXIT_MATCH, DVT_EXIT_MISMATCH = 0, 1  # run_validations: 0 all matched, 1 any mismatch
EXPECTED_DVT_CHECKS = (
    *(f"{SOURCE_SCHEMA}.{table}" for table in COUNT_TABLES),  # run_suite labels counts by source table
    *(check.table for check in RECONCILIATIONS),
)
FAILED_CHECK_FIELDS = (
    "check", "validation_name", "group_by_columns", "source_agg_value", "target_agg_value", "validation_status",
)


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
