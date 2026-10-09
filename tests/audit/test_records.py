"""Unit tests for audit/records.py: the rules every pipeline_audit row must meet."""

import pytest

from audit.records import DVT_MATCH, DVT_MISMATCH, FAILED, SUCCESS, AuditRecord, TaskOutcome


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
