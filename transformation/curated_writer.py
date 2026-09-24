"""
Write valid rows into the curated Delta table for one source table.

One Delta table per source table at <curated_root>/<table>, mirroring the
retail_oltp shape (ADR-010). Rows are upserted on the primary key and an
older version never overwrites a newer one.
"""

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession

from transformation.config.table_config import get_table_config
from transformation.schemas.source_schemas import get_source_schema

TARGET_ALIAS = "target"
SOURCE_ALIAS = "source"
CHECKPOINTS_DIRECTORY = "_checkpoints"


def curated_path(curated_root: str, table: str) -> str:
    """
    Location of one curated Delta table.

    Args:
        curated_root: Root of the curated zone, a local folder in tests or
            abfss://curated@<account>.dfs.core.windows.net on Databricks.
        table: Source table name.

    Returns:
        The table's Delta path, as decided in ADR-010.
    """
    return f"{curated_root}/{table}"


def _source_columns_only(df: DataFrame, table: str) -> DataFrame:
    """
    Keep the source shape, dropping every pipeline metadata column.

    _raw_payload especially must not reach curated: it repeats every value
    and holds personal data that belongs only in dead-letter.

    Args:
        df: Deduplicated valid rows.
        table: Source table name.

    Returns:
        The DataFrame with exactly the columns of the table's schema.
    """
    return df.select(*get_source_schema(table).fieldNames())


def merge_into_curated(
    spark: SparkSession, df: DataFrame, table: str, curated_root: str
) -> str:
    """
    Upsert valid rows into the curated Delta table for one source table.

    Creates the table on the first run. After that, rows are matched on the
    primary key and updated only when the incoming version is newer, so a
    re-read of older data cannot move curated backwards.

    Args:
        spark: Active SparkSession.
        df: Output of dedupe(): one row per primary key.
        table: Source table name.
        curated_root: Root of the curated zone.

    Returns:
        The path written to.

    Raises:
        ValueError: If the table has no config or schema.
    """
    config = get_table_config(table)
    path = curated_path(curated_root, table)
    to_write = _source_columns_only(df, table)

    if not DeltaTable.isDeltaTable(spark, path):
        to_write.write.format("delta").save(path)  # first run: nothing to merge into
        return path

    key_match = " AND ".join(
        f"{TARGET_ALIAS}.{column} = {SOURCE_ALIAS}.{column}" for column in config.primary_key
    )
    incoming_is_newer = (
        f"{SOURCE_ALIAS}.{config.watermark_column} > {TARGET_ALIAS}.{config.watermark_column}"
    )

    (
        DeltaTable.forPath(spark, path)
        .alias(TARGET_ALIAS)
        .merge(to_write.alias(SOURCE_ALIAS), key_match)
        .whenMatchedUpdateAll(condition=incoming_is_newer)  # the guard
        .whenNotMatchedInsertAll()
        .execute()
    )

    return path

def checkpoint_path(curated_root: str, table: str) -> str:
    """
    Location of one table's Auto Loader checkpoint.

    Args:
        curated_root: Root of the curated zone.
        table: Source table name.

    Returns:
        The checkpoint path from ADR-010.
    """
    return f"{curated_root}/{CHECKPOINTS_DIRECTORY}/{table}"