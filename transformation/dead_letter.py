"""
Write rejected rows to the dead-letter Delta table.

Rejected rows are not deduplicated upstream and ADF re-copies rows on purpose,
so the same bad row arrives run after run. record_id is a deterministic hash of
the problem, and the write is an insert-only MERGE, so re-seeing a known bad
row inserts nothing. See ADR-012 and ADR-016.
"""

from delta.tables import DeltaTable
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from transformation.config.table_config import get_table_config
from transformation.dq_rules import DQ_ERRORS_COLUMN
from transformation.raw_payload import RAW_PAYLOAD_COLUMN

DEAD_LETTER_DIRECTORY = "_dead_letter"  # ADR-012: curated/_dead_letter/, underscore keeps it out of the table namespace
KEY_SEPARATOR = "|"                     # between composite primary key parts
REASON_SEPARATOR = ";"                  # between reason codes in one VARCHAR


def dead_letter_path(curated_root: str) -> str:
    """
    Location of the dead-letter Delta table, shared by every source table.

    Args:
        curated_root: Root of the curated zone.

    Returns:
        The Delta path from ADR-012.
    """
    return f"{curated_root}/{DEAD_LETTER_DIRECTORY}"


def _source_key(primary_key: list[str]) -> Column:
    """
    The rejected row's primary key value as text.

    NULL parts are skipped by concat_ws, so a row rejected for a NULL key
    gets an empty string here. raw_payload still identifies it.

    Args:
        primary_key: The table's primary key columns.

    Returns:
        A string Column, parts joined by KEY_SEPARATOR.
    """
    return F.concat_ws(KEY_SEPARATOR, *[F.col(c).cast("string") for c in primary_key])


def build_dead_letter_rows(df: DataFrame, table: str) -> DataFrame:
    """
    Shape rejected rows into the dead-letter schema.

    Args:
        df: Rejected rows from split_valid_rejected().
        table: Source table they came from.

    Returns:
        One row per rejected row, in the dead_letter shape.

    Raises:
        ValueError: If the table has no config.
    """
    config = get_table_config(table)

    source_table = F.lit(table)
    source_key = _source_key(config.primary_key)
    error_reason = F.concat_ws(REASON_SEPARATOR, F.col(DQ_ERRORS_COLUMN))
    raw_payload = F.col(RAW_PAYLOAD_COLUMN)

    return df.select(
        # failed_at is deliberately NOT in the hash: it would make every run a new record
        F.xxhash64(source_table, source_key, error_reason, raw_payload).alias("record_id"),
        source_table.alias("source_table"),
        source_key.alias("source_key"),
        error_reason.alias("error_reason"),
        raw_payload.alias("raw_payload"),
        F.current_timestamp().alias("failed_at"),  # first time this problem was seen
        F.lit(False).alias("reprocessed"),
        F.lit(None).cast("timestamp").alias("reprocessed_at"),
    )


def write_dead_letter(
    spark: SparkSession, rejected: DataFrame, table: str, curated_root: str
) -> str:
    """
    Insert rejected rows that are not already recorded.

    Args:
        spark: Active SparkSession.
        rejected: Rejected rows from split_valid_rejected().
        table: Source table they came from.
        curated_root: Root of the curated zone.

    Returns:
        The path written to.
    """
    rows = build_dead_letter_rows(rejected, table)
    path = dead_letter_path(curated_root)

    if not DeltaTable.isDeltaTable(spark, path):
        rows.write.format("delta").save(path)  # first run, even if empty, so the table always exists
        return path

    (
        DeltaTable.forPath(spark, path)
        .alias("target")
        .merge(rows.alias("source"), "target.record_id = source.record_id")
        .whenNotMatchedInsertAll()  # insert only: a known problem is never recorded twice
        .execute()
    )

    return path

