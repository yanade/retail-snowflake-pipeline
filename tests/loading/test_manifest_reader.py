"""Unit tests for the manifest reader: type boundary, query parameters, env checks."""

from datetime import datetime, timezone
from types import SimpleNamespace

import numpy as np
import pytest

from loading import manifest_reader
from loading.manifest_reader import fetch_manifest_rows, to_export

WRITTEN_AT = datetime(2026, 10, 2, 12, 22, 22, tzinfo=timezone.utc)


def _row(files: list[str], written_at: datetime = WRITTEN_AT) -> SimpleNamespace:
    """A stand-in for a driver Row: same attribute access, files as numpy like the real driver."""
    return SimpleNamespace(
        run_id="4469480396113", table_name="customers", export_mode="snapshot",
        end_version=18, files=np.array(files), row_count=1420, written_at=written_at,
    )


class _RecordingCursor:
    """Fake cursor that remembers what execute() received."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []

    def execute(self, query: str, parameters: dict) -> None:
        self.calls.append((query, parameters))

    def fetchall(self) -> list:
        return []


def test_to_export_turns_numpy_files_into_tuple_of_str():
    """Two files, the case where numpy truthiness would raise."""
    export = to_export(_row(["a/part-00000.parquet", "a/part-00001.parquet"]))
    assert export.files == ("a/part-00000.parquet", "a/part-00001.parquet")
    assert all(type(path) is str for path in export.files)  # not numpy.str_


def test_to_export_keeps_a_single_file_as_tuple():
    """One file, the case where a numpy array would pass unnoticed."""
    assert to_export(_row(["a/part-00000.parquet"])).files == ("a/part-00000.parquet",)


def test_to_export_rejects_naive_written_at():
    """A driver change that drops the time zone fails here, not in a later comparison."""
    with pytest.raises(ValueError, match="no time zone"):
        to_export(_row(["a/part-00000.parquet"], written_at=datetime(2026, 10, 2, 12, 22)))


def test_fetch_manifest_rows_passes_window_as_parameter():
    """The window travels as a bound parameter, never pasted into the SQL text."""
    cursor = _RecordingCursor()
    fetch_manifest_rows(cursor, window_days=3)
    query, parameters = cursor.calls[0]
    assert parameters == {"window_days": 3}
    assert ":window_days" in query


def test_connect_databricks_names_missing_variable(monkeypatch):
    """A missing token fails before any connection attempt, and says which variable."""
    monkeypatch.setattr(manifest_reader, "load_dotenv", lambda: None)  # keep the real .env out
    monkeypatch.setenv("DATABRICKS_HOST", "adb-test.azuredatabricks.net")
    monkeypatch.setenv("DATABRICKS_HTTP_PATH", "/sql/1.0/warehouses/test")
    monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
    with pytest.raises(ValueError, match="DATABRICKS_TOKEN"):
        manifest_reader.connect_databricks()