"""
Tests for transformation/served_manifest.py. plan_export() is pure Python,
so these run without Spark.
"""

import pytest

from transformation.served_manifest import SNAPSHOT_MODE, ExportPlan, plan_export
from transformation.served_manifest import CDF_MODE, SNAPSHOT_MODE, ExportPlan, plan_export


TABLE = "orders"
TABLE_ID = "table-id-a"
REBUILT_TABLE_ID = "table-id-b"


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