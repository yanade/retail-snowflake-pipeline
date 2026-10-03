"""Unit tests for the load_raw CLI: grouping, early exits, schema drift, failure on a bad check, arguments."""

from contextlib import nullcontext
from datetime import datetime, timezone
from unittest.mock import patch

import pytest

from loading import load_raw as cli
from loading.manifest_reader import LOAD_WINDOW_DAYS, ManifestExport
from loading.raw_loader import FileLoad, SchemaDrift


def _export(table: str, files: tuple[str, ...], row_count: int) -> ManifestExport:
    """A manifest export with only the fields these tests care about varied."""
    return ManifestExport(
        run_id="r1", table_name=table, export_mode="snapshot", end_version=1,
        files=files, row_count=row_count, written_at=datetime(2026, 10, 2, tzinfo=timezone.utc),
    )


class _FakeConnection:
    """Context manager standing in for a Snowflake connection."""

    def __enter__(self) -> "_FakeConnection":
        return self

    def __exit__(self, *exc) -> None:
        return None

    def cursor(self, _cursor_class: type) -> nullcontext:
        return nullcontext(object())  # the loader functions are patched, so the cursor is never used


def test_group_files_by_table_keeps_manifest_order():
    """Two exports of one table share one list, snapshot files first."""
    exports = [
        _export("customers", ("customers/s/0.parquet", "customers/s/1.parquet"), 1420),
        _export("customers", ("customers/c/0.parquet",), 31),
        _export("orders", ("orders/s/0.parquet",), 1537),
    ]
    assert cli.group_files_by_table(exports) == {
        "customers": ["customers/s/0.parquet", "customers/s/1.parquet", "customers/c/0.parquet"],
        "orders": ["orders/s/0.parquet"],
    }


def test_empty_window_does_not_connect(monkeypatch):
    """No exports means no Snowflake connection, so the warehouse stays asleep."""
    monkeypatch.setattr(cli, "read_manifest", lambda window_days: [])
    monkeypatch.setattr(cli, "connect_snowflake", lambda: pytest.fail("connected with nothing to load"))
    assert cli.load_raw() == []


def test_dry_run_does_not_connect(monkeypatch):
    """A dry run reads the manifest only."""
    monkeypatch.setattr(cli, "read_manifest", lambda window_days: [_export("orders", ("orders/s/0.parquet",), 1)])
    monkeypatch.setattr(cli, "connect_snowflake", lambda: pytest.fail("connected on a dry run"))
    assert cli.load_raw(dry_run=True) == []


def test_schema_drift_stops_the_run_before_any_copy(monkeypatch):
    """Drift in any table means nothing is loaded at all."""
    monkeypatch.setattr(cli, "read_manifest", lambda window_days: [_export("orders", ("orders/s/0.parquet",), 1)])
    monkeypatch.setattr(cli, "connect_snowflake", _FakeConnection)
    monkeypatch.setattr(cli, "fetch_raw_columns", lambda cursor: {})
    monkeypatch.setattr(cli, "find_schema_drift", lambda actual: [SchemaDrift("stores", ("CITY",), ("TOWN",))])
    monkeypatch.setattr(cli, "copy_table", lambda *args: pytest.fail("COPY ran despite drift"))

    with pytest.raises(RuntimeError, match="nothing loaded.*stores"):
        cli.load_raw()


def test_short_export_fails_the_run(monkeypatch):
    """One export short in raw raises, and the message names it."""
    monkeypatch.setattr(cli, "read_manifest", lambda window_days: [_export("orders", ("orders/s/0.parquet",), 1537)])
    monkeypatch.setattr(cli, "connect_snowflake", _FakeConnection)
    monkeypatch.setattr(cli, "fetch_raw_columns", lambda cursor: {})
    monkeypatch.setattr(cli, "find_schema_drift", lambda actual: [])  # schema is fine, so the load goes ahead
    monkeypatch.setattr(cli, "copy_table", lambda cursor, table, files: [FileLoad(files[0], "LOADED", 1500)])
    monkeypatch.setattr(cli, "fetch_loaded_counts", lambda cursor, table, files: {files[0]: 1500})

    with pytest.raises(RuntimeError, match="1 of 1 exports.*expected=1537, actual=1500"):
        cli.load_raw()


def test_parse_args_defaults():
    """No flags: the ADR-020 window, a real load."""
    with patch("sys.argv", ["load_raw"]):
        args = cli.parse_args()
    assert (args.window_days, args.dry_run) == (LOAD_WINDOW_DAYS, False)


def test_parse_args_rejects_an_empty_window():
    """A zero-day window would load nothing and report success, so it stops at parse time."""
    with patch("sys.argv", ["load_raw", "--window-days", "0"]), pytest.raises(SystemExit):
        cli.parse_args()
