"""Load served exports into the raw tables with COPY INTO (ADR-019, ADR-020)."""

import os
from collections.abc import Sequence
from dataclasses import dataclass
from itertools import batched

import snowflake.connector
from dotenv import load_dotenv
from snowflake.connector import DictCursor, SnowflakeConnection

from loading.manifest_reader import ManifestExport


ACCEPTED_STATUSES = frozenset({"LOADED", "LOAD_SKIPPED"})  # skipped = loaded by an earlier run
RAW_SCHEMA = "ecommerce_db.raw"
STAGE = "@ecommerce_db.raw.served_stage"
MAX_FILES_PER_COPY = 1000  # Snowflake's limit for FILES = (...)

METADATA_COLUMNS = ("_source_file", "_loaded_at")  # filled by COPY, not by the source

RAW_COLUMNS = {  # columns per raw table, in DDL order; a test keeps it equal to SOURCE_SCHEMAS plus dead_letter
    "currencies": (
        "currency_code", "currency_name", "currency_symbol", "decimal_places", "is_active",
        "created_at", "updated_at"
    ),
    "product_categories": (
        "category_id", "parent_category_id", "category_code", "category_name", "is_active",
        "created_at", "updated_at"
    ),
    "suppliers": (
        "supplier_id", "supplier_code", "supplier_name", "country_code", "contact_email",
        "is_active", "created_at", "updated_at"
    ),
    "products": (
        "product_id", "sku", "product_name", "category_id", "supplier_id", "brand",
        "standard_unit_price", "default_currency_code", "is_active", "created_at", "updated_at"
    ),
    "customers": (
        "customer_id", "customer_number", "first_name", "last_name", "email", "phone",
        "country_code", "customer_status", "created_at", "updated_at"
    ),
    "customer_addresses": (
        "address_id", "customer_id", "address_type", "address_line_1", "address_line_2", "city",
        "region", "postal_code", "country_code", "is_default", "created_at", "updated_at"
    ),
    "stores": (
        "store_id", "store_code", "store_name", "store_type", "country_code", "city",
        "opened_date", "closed_date", "is_active", "created_at", "updated_at"
    ),
    "employees": (
        "employee_id", "employee_number", "store_id", "first_name", "last_name", "job_title",
        "email", "hire_date", "termination_date", "is_active", "created_at", "updated_at"
    ),
    "exchange_rates": (
        "exchange_rate_id", "rate_date", "base_currency_code", "target_currency_code",
        "exchange_rate", "source_system", "created_at", "updated_at"
    ),
    "orders": (
        "order_id", "order_number", "customer_id", "store_id", "employee_id", "order_status",
        "order_date", "currency_code", "subtotal_amount", "tax_amount", "shipping_amount",
        "discount_amount", "total_amount", "created_at", "updated_at"
    ),
    "order_items": (
        "order_item_id", "order_id", "line_number", "product_id", "source_product_sku",
        "quantity", "unit_price", "discount_amount", "tax_amount", "line_total_amount",
        "created_at", "updated_at"
    ),
    "payments": (
        "payment_id", "order_id", "payment_reference", "payment_method", "payment_status",
        "payment_amount", "currency_code", "payment_date", "created_at", "updated_at"
    ),
    "dead_letter": (
        "record_id", "source_table", "source_key", "error_reason",
        "raw_payload", "failed_at", "reprocessed", "reprocessed_at"
    ),
}
RAW_TABLES = frozenset(RAW_COLUMNS)

COLUMNS_SQL = """
    SELECT table_name AS "table_name", column_name AS "column_name"
    FROM ecommerce_db.information_schema.columns
    WHERE table_schema = 'RAW'
"""


def _quote(path: str) -> str:
    """SQL string literal for a stage path."""
    return "'" + path.replace("'", "''") + "'"


def build_copy_sql(table: str, files: Sequence[str]) -> str:
    """
    COPY statement that loads exactly the given served files into one raw table.

    Args:
        table: Source table name, one of RAW_TABLES.
        files: Paths relative to the stage, all under the table's own folder.

    Returns:
        The COPY INTO statement.
    """
    if table not in RAW_TABLES:
        raise ValueError(f"Unknown raw table: {table!r}")
    if not files:
        raise ValueError(f"No files to load for {table}")
    if len(files) > MAX_FILES_PER_COPY:
        raise ValueError(f"{len(files)} files for {table}, COPY accepts at most {MAX_FILES_PER_COPY}")
    foreign = [path for path in files if not path.startswith(f"{table}/")]
    if foreign:
        raise ValueError(f"Files outside {table}/: {foreign}")  # would load matching columns into the wrong table

    return (
        f"COPY INTO {RAW_SCHEMA}.{table}\n"
        f"FROM {STAGE}\n"
        f"FILES = ({', '.join(_quote(path) for path in files)})\n"
        "MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE\n"
        "INCLUDE_METADATA = (_source_file = METADATA$FILENAME, _loaded_at = METADATA$START_SCAN_TIME)\n"
        "ON_ERROR = ABORT_STATEMENT"
    )


@dataclass(frozen=True)
class FileLoad:
    """COPY's outcome for one file."""

    file: str  # full azure:// URL, as COPY reports it
    status: str
    rows_loaded: int


def connect_snowflake() -> SnowflakeConnection:
    """
    Open a connection named by SNOWFLAKE_CONNECTION_NAME in connections.toml.

    Returns:
        An open SnowflakeConnection; the caller closes it.
    """
    load_dotenv()  # .env, if present
    name = os.getenv("SNOWFLAKE_CONNECTION_NAME")
    if not name:
        raise ValueError("SNOWFLAKE_CONNECTION_NAME not set. Add to your .env file.")
    return snowflake.connector.connect(connection_name=name)


def parse_copy_result(rows: list[dict]) -> list[FileLoad]:
    """
    Per-file outcomes of one COPY, failing on any status other than loaded or skipped.

    Args:
        rows: COPY's result rows, fetched with a DictCursor.

    Returns:
        One FileLoad per file.
    """
    loads = []
    for row in rows:
        if "file" not in row:
            raise ValueError(f"Unexpected COPY result shape: {row}")
        if row["status"] not in ACCEPTED_STATUSES:
            raise ValueError(f"COPY {row['status']} for {row['file']}: {row['first_error']}")
        loads.append(FileLoad(file=row["file"], status=row["status"], rows_loaded=int(row["rows_loaded"])))
    return loads


def copy_table(cursor: DictCursor, table: str, files: list[str]) -> list[FileLoad]:
    """
    Load the given files into one raw table, in batches COPY accepts.

    Args:
        cursor: Open DictCursor on Snowflake.
        table: Source table name, one of RAW_TABLES.
        files: Paths relative to the stage.

    Returns:
        One FileLoad per file, in the order given.
    """
    loads = []
    for batch in batched(files, MAX_FILES_PER_COPY):  # FILES takes at most 1000 paths
        cursor.execute(build_copy_sql(table, batch))
        loads.extend(parse_copy_result(cursor.fetchall()))
    return loads


@dataclass(frozen=True)
class ExportCheck:
    """Rows found in raw for one export, against the manifest's count."""

    table_name: str
    run_id: str
    expected: int
    actual: int

    @property
    def ok(self) -> bool:
        """True when the export is in raw exactly once."""
        return self.actual == self.expected


def build_count_sql(table: str, files: Sequence[str]) -> str:
    """
    Query counting raw rows per source file, for the given files only.

    Args:
        table: Source table name, one of RAW_TABLES.
        files: Paths relative to the stage, as _source_file stores them.

    Returns:
        The SELECT statement, with lower-case column aliases.
    """
    if table not in RAW_TABLES:
        raise ValueError(f"Unknown raw table: {table!r}")
    if not files:
        raise ValueError(f"No files to count for {table}")
    return (
        'SELECT _source_file AS "source_file", count(*) AS "row_count"\n'  # quoted: keeps the keys lower case
        f"FROM {RAW_SCHEMA}.{table}\n"
        f"WHERE _source_file IN ({', '.join(_quote(path) for path in files)})\n"
        "GROUP BY _source_file"
    )


def fetch_loaded_counts(cursor: DictCursor, table: str, files: Sequence[str]) -> dict[str, int]:
    """
    Rows in one raw table per source file.

    Args:
        cursor: Open DictCursor on Snowflake.
        table: Source table name, one of RAW_TABLES.
        files: Paths relative to the stage.

    Returns:
        Row count per file; a file with no rows is absent.
    """
    cursor.execute(build_count_sql(table, files))
    return {row["source_file"]: int(row["row_count"]) for row in cursor.fetchall()}


def reconcile(exports: Sequence[ManifestExport], loaded_counts: dict[str, int]) -> list[ExportCheck]:
    """
    Compare each export's manifest row count with the rows its files left in raw.

    Args:
        exports: Exports to check.
        loaded_counts: Row count per file, from fetch_loaded_counts().

    Returns:
        One ExportCheck per export, in the order given.
    """
    return [
        ExportCheck(
            table_name=export.table_name,
            run_id=export.run_id,
            expected=export.row_count,
            actual=sum(loaded_counts.get(path, 0) for path in export.files),  # a missing file counts as 0
        )
        for export in exports
    ]


@dataclass(frozen=True)
class SchemaDrift:
    """How one raw table in Snowflake differs from the column contract."""

    table: str
    missing: tuple[str, ...]  # in the contract, not in Snowflake
    extra: tuple[str, ...]  # in Snowflake, not in the contract


def fetch_raw_columns(cursor: DictCursor) -> dict[str, set[str]]:
    """
    Column names of every table in the raw schema, as Snowflake stores them.

    Args:
        cursor: Open DictCursor on Snowflake.

    Returns:
        Table name to its column names, both in stored case.
    """
    cursor.execute(COLUMNS_SQL)
    columns: dict[str, set[str]] = {}
    for row in cursor.fetchall():
        columns.setdefault(row["table_name"], set()).add(row["column_name"])
    return columns


def find_schema_drift(actual: dict[str, set[str]]) -> list[SchemaDrift]:
    """
    Compare deployed raw tables with RAW_COLUMNS, names only (ADR-020).

    Args:
        actual: Columns per table, from fetch_raw_columns().

    Returns:
        One SchemaDrift per table that differs; empty when all 12 match.
    """
    drift = []
    for table, source_columns in RAW_COLUMNS.items():
        expected = {name.upper() for name in (*source_columns, *METADATA_COLUMNS)}  # unquoted DDL names are stored upper case
        found = actual.get(table.upper(), set())
        if found != expected:
            drift.append(SchemaDrift(table, tuple(sorted(expected - found)), tuple(sorted(found - expected))))
    return drift
