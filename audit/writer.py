"""Write AuditRecords to pipeline_audit, one MERGE per record (ADR-026)."""

import json
from datetime import datetime, timezone

from snowflake.connector.cursor import SnowflakeCursor

from audit.records import AuditRecord
from loading.raw_loader import connect_snowflake

AUDIT_TABLE = "ecommerce_db.audit.pipeline_audit"
KEY_COLUMNS = ("dag_name", "run_id", "task_name")
AUDIT_COLUMNS = (
    *KEY_COLUMNS, "status", "dvt_status", "rows_ingested", "rows_failed", "error_message",
    "details", "try_number", "started_at", "finished_at", "recorded_at",
)  # DDL order; test_writer keeps the two in sync
SOURCE_EXPRESSIONS = {
    "details": "PARSE_JSON(%(details)s)",  # JSON text in, VARIANT out
    "recorded_at": "SYSDATE()",  # Snowflake's clock, UTC, TIMESTAMP_NTZ
}


def build_merge_sql() -> str:
    """
    MERGE that inserts a new key and updates an existing one, so a retry never adds a row.

    Returns:
        The statement, with pyformat placeholders for every value.
    """
    source = ",\n        ".join(f"{SOURCE_EXPRESSIONS.get(c, f'%({c})s')} AS {c}" for c in AUDIT_COLUMNS)
    match = " AND ".join(f"target.{c} = source.{c}" for c in KEY_COLUMNS)
    update = ", ".join(f"{c} = source.{c}" for c in AUDIT_COLUMNS if c not in KEY_COLUMNS)  # the key never changes
    return (
        f"MERGE INTO {AUDIT_TABLE} AS target\n"
        f"USING (\n    SELECT\n        {source}\n) AS source\n"
        f"ON {match}\n"
        f"WHEN MATCHED THEN UPDATE SET {update}\n"
        f"WHEN NOT MATCHED THEN INSERT ({', '.join(AUDIT_COLUMNS)}) "
        f"VALUES ({', '.join(f'source.{c}' for c in AUDIT_COLUMNS)})"
    )


MERGE_SQL = build_merge_sql()


def to_utc_naive(value: datetime | None) -> datetime | None:
    """
    Convert an aware datetime to naive UTC for a TIMESTAMP_NTZ column (ADR-020).

    Args:
        value: Timezone-aware datetime, or None.

    Returns:
        The same instant as naive UTC, or None.
    """
    if value is None:
        return None
    if value.tzinfo is None:
        raise ValueError(f"timestamp {value} has no time zone, cannot store it as UTC")
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def to_params(record: AuditRecord) -> dict:
    """
    Bind values for MERGE_SQL.

    Args:
        record: The row to write.

    Returns:
        One value per placeholder.
    """
    outcome = record.outcome
    return {
        "dag_name": record.dag_name,
        "run_id": record.run_id,
        "task_name": record.task_name,
        "status": outcome.status,
        "dvt_status": outcome.dvt_status,
        "rows_ingested": outcome.rows_ingested,
        "rows_failed": outcome.rows_failed,
        "error_message": outcome.error_message,
        "details": None if outcome.details is None else json.dumps(outcome.details),
        "try_number": record.try_number,
        "started_at": to_utc_naive(record.started_at),
        "finished_at": to_utc_naive(record.finished_at),
    }


def write_record(cursor: SnowflakeCursor, record: AuditRecord) -> None:
    """
    Run the MERGE for one record and check it touched exactly one row.

    Args:
        cursor: Open cursor on Snowflake, autocommit on.
        record: The row to write.
    """
    cursor.execute(MERGE_SQL, to_params(record))  # bound, so quotes in error_message cannot break the SQL
    inserted, updated = cursor.fetchone()[:2]  # MERGE returns (rows inserted, rows updated)
    if inserted + updated != 1:
        raise RuntimeError(f"audit MERGE for {record.task_name} wrote {inserted} inserted, {updated} updated, expected 1")


def record_task(record: AuditRecord) -> None:
    """
    Open a connection and write one record; raises on any failure.

    Args:
        record: The row to write.
    """
    with connect_snowflake() as conn, conn.cursor() as cursor:
        write_record(cursor, record)
