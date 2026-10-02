"""Check that the served change log rebuilds curated exactly (ADR-020)."""

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from transformation.config.table_config import get_table_config
from transformation.curated_writer import curated_path
from transformation.served_manifest import manifest_path

WATERMARK_COLUMN = "updated_at"


def replay_latest(served: DataFrame, primary_key: list[str]) -> DataFrame:
    """
    Keep the newest row per primary key, as dbt will when it loads served.

    Args:
        served: Every served row of one table, snapshot and changes together.
        primary_key: The table's primary key columns.

    Returns:
        One row per key, with the served columns unchanged.
    """
    newest_first = Window.partitionBy(*primary_key).orderBy(F.col(WATERMARK_COLUMN).desc())
    return (
        served
        .withColumn("_rn", F.row_number().over(newest_first))
        .where(F.col("_rn") == 1)
        .drop("_rn")
    )


def manifest_files(spark: SparkSession, curated_root: str, served_root: str, table: str) -> list[str]:
    """
    List every served file the manifest records for one table.

    Args:
        spark: Active SparkSession.
        curated_root: Root of the curated zone, where the manifest lives.
        served_root: Root of the served zone.
        table: Source table name.

    Returns:
        Full paths, in no particular order.

    Raises:
        ValueError: If the manifest lists no files for the table.
    """
    path = manifest_path(curated_root)
    rows = []
    if DeltaTable.isDeltaTable(spark, path):
        rows = spark.read.format("delta").load(path).where(F.col("table_name") == table).select("files").collect()

    files = [f"{served_root}/{f}" for row in rows for f in row["files"]]  # only what the manifest lists
    if not files:
        raise ValueError(f"{table}: the manifest lists no files; export it first")
    return files


def reconcile(spark: SparkSession, curated_root: str, served_root: str, table: str) -> dict[str, int]:
    """
    Replay the served change log of one table and compare it with curated.

    Args:
        spark: Active SparkSession.
        curated_root: Root of the curated zone.
        served_root: Root of the served zone.
        table: Source table name.

    Returns:
        Row counts and the two difference counts, both 0 when served is complete.
    """
    curated = spark.read.format("delta").load(curated_path(curated_root, table))
    served = spark.read.parquet(*manifest_files(spark, curated_root, served_root, table))
    latest = replay_latest(served, get_table_config(table).primary_key).select(*curated.columns)  # exceptAll matches by position

    return {
        "served_rows": served.count(),
        "latest_per_key": latest.count(),
        "curated_rows": curated.count(),
        "only_in_served": latest.exceptAll(curated).count(),
        "only_in_curated": curated.exceptAll(latest).count(),
    }
