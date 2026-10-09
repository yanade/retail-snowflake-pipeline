"""Unit tests for validation/run_validations.py: log lines, the result file and the exit code."""

import json
from pathlib import Path

import pytest

from validation import run_validations
from validation.run_validations import describe, main, write_results


def _row(table: str | None, status: str = "success") -> dict:
    """One DVT result row, reduced to the fields the runner reads."""
    return {"source_table_name": table, "validation_name": "count", "validation_status": status}


def test_describe_names_the_check_group_values_and_status():
    row = {"check": "payments", "validation_name": "count", "group_by_columns": '{"currency_code": "CAD"}',
           "source_agg_value": "334", "target_agg_value": "333", "validation_status": "fail"}

    assert describe(row) == 'payments count {"currency_code": "CAD"}: source 334, target 333, fail'


def test_results_are_written_under_the_run_id(tmp_path: Path):
    path = write_results([_row("retail_oltp.stores")], "run-1", tmp_path / "results")

    assert path == tmp_path / "results" / "run-1.json"
    assert json.loads(path.read_text()) == [_row("retail_oltp.stores")]


@pytest.mark.parametrize(("status", "exit_code"), [("success", 0), ("fail", 1)])
def test_main_exit_code_follows_the_results(monkeypatch, tmp_path: Path, status: str, exit_code: int):
    """The point of the runner: DVT exits 0 on a mismatch, this must not."""
    monkeypatch.setattr(run_validations, "run_suite", lambda *args: [{**_row("t", status), "check": "t"}])  # no databases

    assert main(["--window-end", "2026-10-03 15:25:24", "--run-id", "run-1"], results_dir=tmp_path) == exit_code
    assert (tmp_path / "run-1.json").exists()  # written whatever the outcome
