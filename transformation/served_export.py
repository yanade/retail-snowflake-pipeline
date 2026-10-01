"""
Read curated Delta tables for export to the served zone.

A snapshot reads a whole table at one version; a change read takes the
Change Data Feed between two versions. Both return the source shape (ADR-020).
"""
from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from transformation.curated_writer import curated_path
from transformation.schemas.source_schemas import get_source_schema

CHANGE_TYPE_COLUMN = "_change_type"
EXPORTED_CHANGE_TYPES = ("insert", "update_postimage")  # the state after each change


def read_snapshot(
    spark: SparkSession, curated_root: str, table: str, version: int
) -> DataFrame:
    """
    Read a whole curated table as it was at one Delta version.

    Args:
        spark: Active SparkSession.
        curated_root: Root of the curated zone.
        table: Source table name.
        version: Delta version to read, pinned before the export starts.

    Returns:
        Every row of the table at that version, source columns only.
    """
    return (
        spark.read.format("delta")
        .option("versionAsOf", version)
        .load(curated_path(curated_root, table))
        .select(*get_source_schema(table).fieldNames())
    )


def read_changes(
    spark: SparkSession,
    curated_root: str,
    table: str,
    start_version: int,
    end_version: int,
) -> DataFrame:
    """
    Read the rows a curated table gained or changed between two Delta versions.

    Both bounds are inclusive. A key changed twice in the range appears twice:
    served is a change log, and dbt derives history from it (ADR-020).

    Args:
        spark: Active SparkSession.
        curated_root: Root of the curated zone.
        table: Source table name.
        start_version: First version to read, the last exported version + 1.
        end_version: Last version to read, pinned before the export starts.

    Returns:
        Inserted rows and the new state of updated rows, source columns only.
    """
    return (
        spark.read.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", start_version)
        .option("endingVersion", end_version)
        .load(curated_path(curated_root, table))
        .where(F.col(CHANGE_TYPE_COLUMN).isin(*EXPORTED_CHANGE_TYPES))
        .select(*get_source_schema(table).fieldNames())  # also drops the CDF columns
    )


def _run_directory(table: str, run_date: date, run_id: str) -> str:
    """Run directory relative to the served root, as the stage sees it (ADR-020)."""
    return (
        f"{table}/year={run_date:%Y}/month={run_date:%m}/day={run_date:%d}"
        f"/run_id={run_id}"
    )


def write_served(
    spark: SparkSession,
    df: DataFrame,
    served_root: str,
    table: str,
    run_date: date,
    run_id: str,
) -> tuple[list[str], int]:
    """
    Write one export as Parquet into its own run directory.

    Args:
        spark: Active SparkSession.
        df: Output of read_snapshot() or read_changes().
        served_root: Root of the served zone.
        table: Source table name.
        run_date: UTC date of the run.
        run_id: Unique id of the run.

    Returns:
        Files relative to served_root, and the row count read back from them.
    """
    relative_dir = _run_directory(table, run_date, run_id)
    full_dir = f"{served_root}/{relative_dir}"

    df.write.mode("errorifexists").parquet(full_dir)  # a reused run_id fails, never overwrites

    written = spark.read.parquet(full_dir)
    row_count = written.count()
    if row_count == 0:
        return [], 0  # Spark still wrote an empty file; the manifest ignores it

    files = sorted(
        f"{relative_dir}/{uri.rsplit('/', 1)[-1]}" for uri in written.inputFiles()
    )
    return files, row_count

