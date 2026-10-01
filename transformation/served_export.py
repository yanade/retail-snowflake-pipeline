"""
Read curated Delta tables for export to the served zone.

A snapshot reads a whole table at one version; a change read takes the
Change Data Feed between two versions. Both return the source shape (ADR-020).
"""

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

