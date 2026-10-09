"""Unit tests for audit/outcomes.py: each task's output mapped to an outcome."""

import json
from pathlib import Path

import pytest

from audit.outcomes import (
    EXPECTED_DVT_CHECKS, MAX_ERROR_CHARS, curated_to_served_outcome, dvt_outcome, failure_outcome,
    load_dvt_results, load_raw_outcome, raw_to_curated_outcome, skipped_outcome,
)
from audit.records import DVT_MATCH, DVT_MISMATCH, DVT_SKIPPED, FAILED, SKIPPED, SUCCESS
from loading.raw_loader import ExportCheck


def _rows(status: str = "success", run_id: str = "run-1") -> list[dict]:
    """One result row per expected check, shaped like validation/results/<run_id>.json."""
    return [{"check": check, "validation_name": "count", "run_id": run_id, "validation_status": status}
            for check in EXPECTED_DVT_CHECKS]


def test_all_checks_agreeing_is_match():
    outcome = dvt_outcome(_rows(), 0, "run-1")

    assert (outcome.status, outcome.dvt_status) == (SUCCESS, DVT_MATCH)
    assert outcome.details == {"validations": len(EXPECTED_DVT_CHECKS)}


def test_a_differing_check_is_mismatch_with_the_check_in_details():
    rows = _rows()
    rows[0] = {**rows[0], "validation_status": "fail"}

    outcome = dvt_outcome(rows, 1, "run-1")

    assert (outcome.status, outcome.dvt_status) == (FAILED, DVT_MISMATCH)
    assert outcome.error_message == f"1 of {len(rows)} validations mismatched"
    assert [check["check"] for check in outcome.details["failed"]] == [rows[0]["check"]]


@pytest.mark.parametrize(("rows", "exit_code", "reason"), [
    (None, 1, "no results file"),                       # crashed before write_results
    ([], 1, "no result for"),                           # all([]) is True: must never be MATCH
    (_rows()[1:], 0, "no result for"),                  # one check missing
    (_rows(run_id="run-0"), 1, "another run"),          # stale file from a different run
    (_rows("fail"), 0, "disagrees"),                    # exit code and rows contradict
    (_rows(), 1, "disagrees"),
    (_rows(), 2, "disagrees"),
])
def test_an_unusable_result_is_failed_without_dvt_status(rows, exit_code, reason):
    outcome = dvt_outcome(rows, exit_code, "run-1")

    assert (outcome.status, outcome.dvt_status) == (FAILED, None)  # unknown, not a MISMATCH
    assert reason in outcome.error_message


def test_missing_results_file_reads_as_none(tmp_path: Path):
    assert load_dvt_results(tmp_path / "run-1.json") is None


def test_results_file_round_trips(tmp_path: Path):
    path = tmp_path / "run-1.json"
    path.write_text(json.dumps(_rows()))

    assert load_dvt_results(path) == _rows()


def test_raw_to_curated_sums_valid_and_rejected_and_still_succeeds():
    metrics = {"orders": {"rows_read": 10, "rows_valid": 7, "rows_rejected": 2},
               "payments": {"rows_read": 5, "rows_valid": 5, "rows_rejected": 0}}

    outcome = raw_to_curated_outcome(metrics)

    assert (outcome.status, outcome.rows_ingested, outcome.rows_failed) == (SUCCESS, 12, 2)  # rejects are not a failure
    assert outcome.details == {"tables": metrics}


def test_raw_to_curated_with_no_new_files_measures_zero():
    outcome = raw_to_curated_outcome({"orders": {"rows_read": 0, "rows_valid": 0, "rows_rejected": 0}})

    assert (outcome.rows_ingested, outcome.rows_failed) == (0, 0)


def test_curated_to_served_counts_only_tables_that_exported():
    summaries = {"orders": {"export_mode": "cdf", "start_version": 3, "end_version": 4,
                            "row_count": 40, "file_count": 1},
                 "stores": None}

    outcome = curated_to_served_outcome(summaries)

    assert (outcome.rows_ingested, outcome.rows_failed) == (40, None)  # exporting rejects nothing: not measured
    assert outcome.details["tables"]["stores"] is None


def test_curated_to_served_with_nothing_changed_is_zero_not_null():
    assert curated_to_served_outcome({"orders": None, "stores": None}).rows_ingested == 0


def test_load_raw_counts_only_this_runs_exports():
    checks = [ExportCheck("orders", "run-1", 40, 40), ExportCheck("payments", "run-1", 12, 12),
              ExportCheck("orders", "run-0", 30, 30)]  # an older export still inside the 7-day window

    outcome = load_raw_outcome(checks, "run-1")

    assert outcome.rows_ingested == 52
    assert len(outcome.details["exports"]) == 3


def test_load_raw_with_no_exports_this_run_is_zero():
    assert load_raw_outcome([], "run-1").rows_ingested == 0


def test_failure_keeps_type_and_first_line_only():
    outcome = failure_outcome(RuntimeError("DVT exited 2, the check did not run:\nstderr with details"))

    assert (outcome.status, outcome.dvt_status) == (FAILED, None)
    assert outcome.error_message == "RuntimeError: DVT exited 2, the check did not run:"


def test_failure_message_is_capped_and_survives_an_empty_message():
    assert len(failure_outcome(ValueError("x" * 5000)).error_message) == MAX_ERROR_CHARS
    assert failure_outcome(RuntimeError()).error_message == "RuntimeError: "


@pytest.mark.parametrize(("validates", "dvt_status"), [(False, None), (True, DVT_SKIPPED)])
def test_skipped_records_the_reason(validates, dvt_status):
    outcome = skipped_outcome("upstream_failed", validates)

    assert (outcome.status, outcome.dvt_status) == (SKIPPED, dvt_status)
    assert outcome.details == {"skip_reason": "upstream_failed"}
