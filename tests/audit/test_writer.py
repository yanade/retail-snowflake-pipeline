"""Unit tests for audit/writer.py: the MERGE text, bind values and the one-row check."""

import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from audit.records import SUCCESS, AuditRecord, TaskOutcome
from audit.writer import AUDIT_COLUMNS, MERGE_SQL, to_params, write_record

DDL_PATH = Path(__file__).resolve().parents[2] / "snowflake" / "ddl" / "create_audit_tables.sql"
TABLE_PATTERN = re.compile(r"CREATE TABLE IF NOT EXISTS ecommerce_db\.audit\.pipeline_audit \((.*?)\n\)", re.S)


class FakeCursor:
    """Records what was executed and returns a fixed MERGE result."""

    def __init__(self, result: tuple[int, int]):
        self.result = result
        self.executed = []

    def execute(self, sql: str, params: dict) -> None:
        self.executed.append((sql, params))

    def fetchone(self) -> tuple[int, int]:
        return self.result


def _record(**overrides) -> AuditRecord:
    """A valid record; overrides replace record fields."""
    fields = {"dag_name": "pipeline_dag", "run_id": "manual__1", "task_name": "load_raw",
              "outcome": TaskOutcome(SUCCESS, rows_ingested=52, details={"exports": []}),
              "try_number": 1,
              "started_at": datetime(2026, 10, 9, 7, 0, tzinfo=timezone.utc),
              "finished_at": datetime(2026, 10, 9, 7, 2, tzinfo=timezone.utc)}
    return AuditRecord(**{**fields, **overrides})


def test_writer_columns_match_the_ddl():
    """A column added to the table must be added to the writer, in the same order."""
    body = TABLE_PATTERN.search(DDL_PATH.read_text()).group(1)
    columns = [line.split()[0] for line in body.strip().splitlines() if line.strip()]

    assert [c for c in columns if c != "CONSTRAINT"] == list(AUDIT_COLUMNS)


def test_merge_matches_on_the_full_key_and_never_updates_it():
    update_clause = MERGE_SQL.split("WHEN MATCHED THEN UPDATE SET")[1].split("WHEN NOT MATCHED")[0]

    assert "ON target.dag_name = source.dag_name AND target.run_id = source.run_id " \
           "AND target.task_name = source.task_name" in MERGE_SQL
    assert not re.search(r"\b(dag_name|run_id|task_name) =", update_clause)


def test_every_placeholder_has_a_bind_value():
    """A placeholder without a value is a KeyError at run time, after the task already finished."""
    assert set(re.findall(r"%\((\w+)\)s", MERGE_SQL)) == set(to_params(_record()))


def test_params_serialise_details_and_store_utc():
    started = datetime(2026, 10, 9, 8, 0, tzinfo=timezone(timedelta(hours=1)))  # 07:00 UTC

    params = to_params(_record(started_at=started))

    assert params["details"] == '{"exports": []}'
    assert params["started_at"] == datetime(2026, 10, 9, 7, 0)  # naive, UTC wall time


def test_a_naive_timestamp_is_rejected():
    with pytest.raises(ValueError, match="no time zone"):
        to_params(_record(started_at=datetime(2026, 10, 9, 7, 0)))


@pytest.mark.parametrize("result", [(1, 0), (0, 1)])
def test_one_row_inserted_or_updated_is_accepted(result):
    cursor = FakeCursor(result)

    write_record(cursor, _record())

    assert cursor.executed[0][1]["task_name"] == "load_raw"


def test_any_other_merge_result_raises():
    with pytest.raises(RuntimeError, match="expected 1"):
        write_record(FakeCursor((0, 0)), _record())
