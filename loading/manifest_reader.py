"""Read served exports from the manifest through the retail_loader SQL warehouse (ADR-020)."""

import os
from dataclasses import dataclass
from datetime import datetime

from databricks import sql
from databricks.sql.client import Connection, Cursor
from databricks.sql.types import Row
from dotenv import load_dotenv


LOAD_WINDOW_DAYS = 7  # ADR-020, well inside COPY's 64-day load memory
REQUIRED_ENV_VARS = ("DATABRICKS_HOST", "DATABRICKS_HTTP_PATH", "DATABRICKS_TOKEN")

MANIFEST_SQL = """
    SELECT run_id, table_name, export_mode, end_version, files, row_count, written_at
    FROM retail_dev.ops.served_manifest
    WHERE written_at >= timestampadd(DAY, -:window_days, current_timestamp())
      AND row_count > 0
    ORDER BY table_name, written_at
"""


@dataclass(frozen=True)
class ManifestExport:
    """One served export, as one manifest row describes it."""

    run_id: str
    table_name: str
    export_mode: str
    end_version: int
    files: tuple[str, ...]  # paths relative to the served stage
    row_count: int
    written_at: datetime  # tz-aware UTC


def connect_databricks() -> Connection:
    """
    Open a connection to the SQL warehouse named in .env.

    Returns:
        An open databricks-sql Connection; the caller closes it.
    """
    load_dotenv()  # .env, if present
    missing = [name for name in REQUIRED_ENV_VARS if not os.getenv(name)]
    if missing:
        raise ValueError(f"{', '.join(missing)} not set. Add to your .env file.")
    return sql.connect(
        server_hostname=os.environ["DATABRICKS_HOST"],
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=os.environ["DATABRICKS_TOKEN"],
    )


def fetch_manifest_rows(cursor: Cursor, window_days: int = LOAD_WINDOW_DAYS) -> list[Row]:
    """
    Manifest rows with files, written within the load window.

    Args:
        cursor: Open cursor on the SQL warehouse.
        window_days: How far back to look, in days.

    Returns:
        Raw driver rows, oldest first within each table.
    """
    cursor.execute(MANIFEST_SQL, {"window_days": window_days})  # a parameter, not pasted into the text
    return cursor.fetchall()


def to_export(row: Row) -> ManifestExport:
    """
    Convert one driver row into plain Python types.

    Args:
        row: A row returned by fetch_manifest_rows().

    Returns:
        The export, with files as a tuple of str.
    """
    if row.written_at.tzinfo is None:
        raise ValueError(f"written_at has no time zone for {row.table_name} run {row.run_id}")
    return ManifestExport(
        run_id=str(row.run_id),
        table_name=str(row.table_name),
        export_mode=str(row.export_mode),
        end_version=int(row.end_version),
        files=tuple(str(path) for path in row.files),  # the driver returns numpy.ndarray
        row_count=int(row.row_count),
        written_at=row.written_at,
    )


def read_manifest(window_days: int = LOAD_WINDOW_DAYS) -> list[ManifestExport]:
    """
    Every export in the load window that wrote files.

    Args:
        window_days: How far back to look, in days.

    Returns:
        Exports ordered by table, then by commit time.
    """
    with connect_databricks() as conn, conn.cursor() as cursor:
        return [to_export(row) for row in fetch_manifest_rows(cursor, window_days)]