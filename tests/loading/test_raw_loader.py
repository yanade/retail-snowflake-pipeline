"""Unit tests for the raw loader: COPY text, COPY result parsing, batching, reconciliation, env checks."""

from datetime import datetime, timezone

import pytest

from loading import raw_loader
from loading.manifest_reader import ManifestExport
from loading.raw_loader import (
    MAX_FILES_PER_COPY,
    RAW_TABLES,
    ExportCheck,
    FileLoad,
    build_copy_sql,
    build_count_sql,
    copy_table,
    fetch_loaded_counts,
    parse_copy_result,
    reconcile,
)
from transformation.schemas.source_schemas import SOURCE_SCHEMAS

# Real COPY result rows from 2026-10-03, file paths shortened
LOADED_ROW = {
    "file": "azure://acct.blob.core.windows.net/served/customers/export_id=4d9c/part-00001.parquet",
    "status": "LOADED", "rows_parsed": 30, "rows_loaded": 30, "error_limit": 1, "errors_seen": 0,
    "first_error": None, "first_error_line": None, "first_error_character": None,
    "first_error_column_name": None,
}
SKIPPED_ROW = {
    "file": "azure://acct.blob.core.windows.net/served/currencies/export_id=a4aa/part-00000.parquet",
    "status": "LOAD_SKIPPED", "rows_parsed": 0, "rows_loaded": 0, "error_limit": None, "errors_seen": 1,
    "first_error": "File was loaded before.", "first_error_line": None, "first_error_character": None,
    "first_error_column_name": None,
}

CUSTOMERS_FILES = ("customers/e1/part-00000.parquet", "customers/e1/part-00001.parquet")


class _ScriptedCursor:
    """Fake DictCursor: records each statement, answers with the next prepared result."""

    def __init__(self, results: list[list[dict]]) -> None:
        self.statements: list[str] = []
        self._results = iter(results)

    def execute(self, statement: str) -> None:
        self.statements.append(statement)

    def fetchall(self) -> list[dict]:
        return next(self._results)


def _loaded(file: str) -> dict:
    """A LOADED row for the given file, otherwise like the real one."""
    return {**LOADED_ROW, "file": file}


def _export(files: tuple[str, ...] = CUSTOMERS_FILES, row_count: int = 1420) -> ManifestExport:
    """The customers snapshot, as the manifest describes it."""
    return ManifestExport(
        run_id="4469480396113", table_name="customers", export_mode="snapshot", end_version=18,
        files=files, row_count=row_count, written_at=datetime(2026, 10, 2, 12, 22, tzinfo=timezone.utc),
    )


# build_copy_sql

def test_raw_tables_match_source_schemas():
    """The loader's whitelist and the source contract name the same 12 tables."""
    assert RAW_TABLES == set(SOURCE_SCHEMAS)


def test_copy_names_every_file_in_order():
    """Both files of a two-part export, quoted, in manifest order."""
    sql = build_copy_sql("customers", ["customers/a/part-00000.parquet", "customers/a/part-00001.parquet"])
    assert sql.startswith("COPY INTO ecommerce_db.raw.customers\n")
    assert "FILES = ('customers/a/part-00000.parquet', 'customers/a/part-00001.parquet')" in sql
    assert "_source_file = METADATA$FILENAME" in sql


def test_quote_in_path_is_escaped():
    """A quote in a path cannot end the literal early."""
    assert "'customers/it''s.parquet'" in build_copy_sql("customers", ["customers/it's.parquet"])


@pytest.mark.parametrize("table, files, message", [
    ("customer", ["customer/a.parquet"], "Unknown raw table"),
    ("customers", [], "No files"),
    ("customers", ["orders/a.parquet"], "outside customers/"),
    ("customers", ["customers_old/a.parquet"], "outside customers/"),  # prefix without the slash would pass
    ("customers", [f"customers/{i}.parquet" for i in range(MAX_FILES_PER_COPY + 1)], "at most"),
])
def test_invalid_input_fails_loudly(table, files, message):
    """Each guard rejects its case with a message that names the problem."""
    with pytest.raises(ValueError, match=message):
        build_copy_sql(table, files)


# parse_copy_result

def test_loaded_and_skipped_are_both_accepted():
    """A rerun reports errors_seen 1 for a skipped file; status decides, not the error count."""
    loads = parse_copy_result([LOADED_ROW, SKIPPED_ROW])
    assert loads == [
        FileLoad(file=LOADED_ROW["file"], status="LOADED", rows_loaded=30),
        FileLoad(file=SKIPPED_ROW["file"], status="LOAD_SKIPPED", rows_loaded=0),
    ]


def test_partially_loaded_fails_with_snowflakes_reason():
    """Any other status fails, and the message carries COPY's own first_error."""
    row = {**LOADED_ROW, "status": "PARTIALLY_LOADED", "first_error": "Numeric value 'abc' is not recognized"}
    with pytest.raises(ValueError, match="PARTIALLY_LOADED.*Numeric value 'abc'"):
        parse_copy_result([row])


def test_result_without_file_column_fails():
    """The folder-style answer was never seen with FILES; if it appears, say so instead of returning nothing."""
    with pytest.raises(ValueError, match="Unexpected COPY result shape"):
        parse_copy_result([{"status": "Copy executed with 0 files processed."}])


# copy_table

def test_copy_table_splits_files_into_batches(monkeypatch):
    """Three files with a batch size of two give two COPY statements and three results in order."""
    monkeypatch.setattr(raw_loader, "MAX_FILES_PER_COPY", 2)
    files = ["customers/a.parquet", "customers/b.parquet", "customers/c.parquet"]
    cursor = _ScriptedCursor([[_loaded("a"), _loaded("b")], [_loaded("c")]])

    loads = copy_table(cursor, "customers", files)

    assert len(cursor.statements) == 2
    assert "'customers/a.parquet', 'customers/b.parquet'" in cursor.statements[0]
    assert "FILES = ('customers/c.parquet')" in cursor.statements[1]
    assert [load.file for load in loads] == ["a", "b", "c"]


# build_count_sql, fetch_loaded_counts

def test_count_sql_uses_quoted_lower_case_aliases():
    """Unquoted names come back upper case; the quoted aliases fix the DictCursor keys."""
    sql = build_count_sql("customers", CUSTOMERS_FILES)
    assert 'AS "source_file"' in sql and 'AS "row_count"' in sql
    assert f"IN ('{CUSTOMERS_FILES[0]}', '{CUSTOMERS_FILES[1]}')" in sql


def test_fetch_loaded_counts_maps_file_to_count():
    """Rows come back keyed by the aliases and become a plain dict."""
    cursor = _ScriptedCursor([[{"source_file": CUSTOMERS_FILES[0], "row_count": 1390}]])
    assert fetch_loaded_counts(cursor, "customers", CUSTOMERS_FILES) == {CUSTOMERS_FILES[0]: 1390}


# reconcile

def test_export_loaded_once_passes():
    """The real split: 1390 + 30 rows across two files make the manifest's 1420."""
    checks = reconcile([_export()], {CUSTOMERS_FILES[0]: 1390, CUSTOMERS_FILES[1]: 30})
    assert checks == [ExportCheck("customers", "4469480396113", 1420, 1420)]
    assert checks[0].ok


@pytest.mark.parametrize("counts, actual", [
    ({}, 0),                                                     # never loaded
    ({CUSTOMERS_FILES[0]: 1390}, 1390),                          # one file of two
    ({CUSTOMERS_FILES[0]: 2780, CUSTOMERS_FILES[1]: 60}, 2840),  # loaded twice, e.g. FORCE
])
def test_export_not_loaded_exactly_once_fails(counts, actual):
    """Missing, partial and doubled loads all fail, each with the count found."""
    check = reconcile([_export()], counts)[0]
    assert check.actual == actual
    assert not check.ok


# connect_snowflake

def test_connect_snowflake_names_missing_variable(monkeypatch):
    """No connection name fails before any connection attempt, and says which variable."""
    monkeypatch.setattr(raw_loader, "load_dotenv", lambda: None)  # keep the real .env out
    monkeypatch.delenv("SNOWFLAKE_CONNECTION_NAME", raising=False)
    with pytest.raises(ValueError, match="SNOWFLAKE_CONNECTION_NAME"):
        raw_loader.connect_snowflake()