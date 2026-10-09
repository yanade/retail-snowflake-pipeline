"""Unit tests for audit/records.py: the outcome contract and the DVT mapping."""

import json
from pathlib import Path

import pytest

from audit.records import (
    DVT_MATCH, DVT_MISMATCH, EXPECTED_DVT_CHECKS, FAILED, SUCCESS,
    AuditRecord, TaskOutcome, dvt_outcome, load_dvt_results,
)


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


@pytest.mark.parametrize("kwargs", [
    {"status": "DONE"},
    {"status": SUCCESS, "dvt_status": "PASS"},           # the old vocabulary is rejected
    {"status": FAILED},                                  # no error_message
    {"status": FAILED, "dvt_status": DVT_MATCH, "error_message": "x"},
    {"status": SUCCESS, "dvt_status": DVT_MISMATCH},
    {"status": SUCCESS, "rows_ingested": -1},
])
def test_outcome_rejects_combinations_outside_the_contract(kwargs):
    with pytest.raises(ValueError):
        TaskOutcome(**kwargs)


def test_zero_rows_is_a_valid_measurement():
    assert TaskOutcome(SUCCESS, rows_ingested=0, rows_failed=0).rows_ingested == 0


def test_record_rejects_an_empty_key_part():
    with pytest.raises(ValueError, match="run_id"):
        AuditRecord("pipeline_dag", "", "validate", TaskOutcome(SUCCESS))
