"""Plan each served export from the table's manifest history (ADR-020)."""

from dataclasses import dataclass, asdict
from datetime import datetime

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


SNAPSHOT_MODE = "snapshot"
CDF_MODE = "cdf"
MANIFEST_DIRECTORY = "_served_manifest"  # beside _dead_letter, outside the table namespace

MANIFEST_SCHEMA = StructType([
    StructField("run_id", StringType(), nullable=False),
    StructField("table_name", StringType(), nullable=False),
    StructField("table_id", StringType(), nullable=False),
    StructField("export_mode", StringType(), nullable=False),
    StructField("start_version", LongType(), nullable=True),  # NULL for a snapshot
    StructField("end_version", LongType(), nullable=False),
    StructField("files", ArrayType(StringType(), containsNull=False), nullable=False),
    StructField("row_count", LongType(), nullable=False),
    StructField("written_at", TimestampType(), nullable=False),
])


def manifest_path(curated_root: str) -> str:
    """Location of the served manifest Delta table (ADR-020)."""
    return f"{curated_root}/{MANIFEST_DIRECTORY}"


@dataclass(frozen=True)
class ManifestRow:
    """One export of one table, as appended to the manifest."""

    run_id: str
    table_name: str
    table_id: str
    export_mode: str
    start_version: int | None
    end_version: int
    files: list[str]
    row_count: int
    written_at: datetime


@dataclass(frozen=True)
class ExportPlan:
    """What one export reads: a whole table, or a range of versions."""

    export_mode: str
    start_version: int | None
    end_version: int


def append_manifest_row(spark: SparkSession, curated_root: str, row: ManifestRow) -> None:
    """
    Append one export to the manifest; this append is the export's commit point.

    Args:
        spark: Active SparkSession.
        curated_root: Root of the curated zone.
        row: The export to record.
    """
    (
        spark.createDataFrame([asdict(row)], schema=MANIFEST_SCHEMA)
        .write.format("delta")
        .mode("append")  # creates the table on the first run
        .save(manifest_path(curated_root))
    )


def read_last_export(
    spark: SparkSession, curated_root: str, table: str
) -> tuple[int | None, str | None]:
    """
    Read the end_version and table_id of the table's latest manifest row.

    Args:
        spark: Active SparkSession.
        curated_root: Root of the curated zone.
        table: Curated table name.

    Returns:
        (end_version, table_id), or (None, None) if the table was never exported.
    """
    path = manifest_path(curated_root)
    if not DeltaTable.isDeltaTable(spark, path):
        return None, None  # no export of any table yet

    latest = (
        spark.read.format("delta").load(path)
        .where(F.col("table_name") == table)
        .orderBy(F.col("written_at").desc())  # single writer, so written_at is commit order
        .select("end_version", "table_id")
        .first()
    )
    if latest is None:
        return None, None

    return latest["end_version"], latest["table_id"]


def plan_export(
    table: str,
    current_version: int,
    current_table_id: str,
    last_end_version: int | None,
    last_table_id: str | None,
    full_reload: bool,
) -> ExportPlan | None:
    """
    Choose the export mode and version range for one curated table.

    Args:
        table: Curated table name, for error messages.
        current_version: Pinned Delta version.
        current_table_id: Delta table id from DESCRIBE DETAIL.
        last_end_version: max(end_version) from the manifest, None if no rows.
        last_table_id: table_id of the latest manifest row.
        full_reload: Export the whole table regardless of history.

    Returns:
        The plan, or None when nothing was committed since the last export.

    Raises:
        ValueError: No history, rebuilt table, or manifest ahead of the table.
    """
    if full_reload:  # first: it is the remedy for every check below
        return ExportPlan(SNAPSHOT_MODE, None, current_version)
    if last_end_version is None:
        raise ValueError(f"{table}: no export history; run with full_reload=true")
    if last_table_id != current_table_id:
        raise ValueError(f"{table}: table was rebuilt; run with full_reload=true")
    if current_version < last_end_version:
        raise ValueError(f"{table}: manifest is ahead of the table; run with full_reload=true")
    if current_version == last_end_version:
        return None

    return ExportPlan(CDF_MODE, last_end_version + 1, current_version)
