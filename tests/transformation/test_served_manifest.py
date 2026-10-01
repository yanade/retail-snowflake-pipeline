"""
Tests for transformation/served_manifest.py. plan_export() is pure Python,
so these run without Spark.
"""

import pytest

from datetime import datetime
from pathlib import Path
from dataclasses import fields

from pyspark.sql import SparkSession

from transformation.served_manifest import SNAPSHOT_MODE, ExportPlan, plan_export
from transformation.served_manifest import CDF_MODE, SNAPSHOT_MODE, MANIFEST_SCHEMA, ExportPlan, ManifestRow, plan_export, append_manifest_row, read_last_export


TABLE = "orders"
TABLE_ID = "table-id-a"
REBUILT_TABLE_ID = "table-id-b"
EARLIER = datetime(2026, 10, 1, 9, 0)
LATER = datetime(2026, 10, 2, 9, 0)
EXPORT_ID = "export-1"
EXPORT_DIR = "customers/year=2026/month=10/day=01/export_id=export-1/"


def test_full_reload_takes_a_snapshot_even_after_a_rebuild() -> None:
    """full_reload is the remedy for a rebuild, so it must win over the table_id check."""
    plan = plan_export(
        table=TABLE,
        current_version=5,
        current_table_id=REBUILT_TABLE_ID,
        last_end_version=57,
        last_table_id=TABLE_ID,
        full_reload=True,
    )

    assert plan == ExportPlan(export_mode=SNAPSHOT_MODE, start_version=None, end_version=5)


def test_no_history_without_full_reload_fails() -> None:
    """A table with no manifest rows is never exported silently in full."""
    with pytest.raises(ValueError, match="full_reload"):
        plan_export(
            table=TABLE,
            current_version=5,
            current_table_id=TABLE_ID,
            last_end_version=None,
            last_table_id=None,
            full_reload=False,
        )


def test_rebuilt_table_without_full_reload_fails() -> None:
    """A new table_id means versions restarted, so the manifest range is meaningless."""
    with pytest.raises(ValueError, match="full_reload"):
        plan_export(
            table=TABLE,
            current_version=60,
            current_table_id=REBUILT_TABLE_ID,
            last_end_version=57,
            last_table_id=TABLE_ID,
            full_reload=False,
        )


def test_manifest_ahead_of_the_table_fails() -> None:
    """The same table cannot have gone backwards; the manifest is wrong."""
    with pytest.raises(ValueError, match="ahead"):
        plan_export(
            table=TABLE,
            current_version=5,
            current_table_id=TABLE_ID,
            last_end_version=57,
            last_table_id=TABLE_ID,
            full_reload=False,
        )


def test_no_new_versions_means_nothing_to_export() -> None:
    """Nothing committed since the last export: no read, no manifest row."""
    plan = plan_export(
        table=TABLE,
        current_version=57,
        current_table_id=TABLE_ID,
        last_end_version=57,
        last_table_id=TABLE_ID,
        full_reload=False,
    )

    assert plan is None


def test_new_versions_are_read_as_changes() -> None:
    """The range starts after the last exported version and ends at the pinned one."""
    plan = plan_export(
        table=TABLE,
        current_version=60,
        current_table_id=TABLE_ID,
        last_end_version=57,
        last_table_id=TABLE_ID,
        full_reload=False,
    )

    assert plan == ExportPlan(export_mode=CDF_MODE, start_version=58, end_version=60)


def test_last_export_is_the_latest_row_not_the_highest_version(
    spark: SparkSession, tmp_path: Path
) -> None:
    """After a rebuild and full_reload, the new snapshot row wins over the older, higher version."""
    curated_root = str(tmp_path)
    append_manifest_row(spark, curated_root, ManifestRow(
        run_id="run-1", table_name=TABLE, table_id=TABLE_ID, export_mode=CDF_MODE,
        start_version=50, end_version=57, files=[], row_count=0, written_at=EARLIER,
    ))
    append_manifest_row(spark, curated_root, ManifestRow(
        run_id="run-2", table_name=TABLE, table_id=REBUILT_TABLE_ID, export_mode=SNAPSHOT_MODE,
        start_version=None, end_version=5, files=[], row_count=0, written_at=LATER,
    ))

    assert read_last_export(spark, curated_root, TABLE) == (5, REBUILT_TABLE_ID)


def test_manifest_row_matches_the_schema() -> None:
    """ManifestRow and MANIFEST_SCHEMA describe the same columns, in the same order."""
    assert [f.name for f in fields(ManifestRow)] == MANIFEST_SCHEMA.fieldNames()


def test_no_manifest_means_no_history(spark: SparkSession, tmp_path: Path) -> None:
    """Before the first export of any table, the manifest table does not exist."""
    assert read_last_export(spark, str(tmp_path), TABLE) == (None, None)


def test_rows_of_another_table_are_not_history(spark: SparkSession, tmp_path: Path) -> None:
    """One manifest serves all tables, so another table's export never becomes this one's watermark."""
    curated_root = str(tmp_path)
    append_manifest_row(spark, curated_root, ManifestRow(
        run_id="run-1", table_name="customers", table_id=TABLE_ID, export_mode=CDF_MODE,
        start_version=50, end_version=57, files=[], row_count=0, written_at=EARLIER,
    ))

    assert read_last_export(spark, curated_root, TABLE) == (None, None)