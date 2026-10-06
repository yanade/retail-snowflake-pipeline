"""
Write rejected rows to the dead-letter Delta table.

record_id hashes the problem: the row version (key + updated_at) and the
reason, or the exact text of a malformed line. Rows are deduplicated on it,
then insert-only MERGEd, so a bad row is recorded once. See ADR-012.
"""

from delta.tables import DeltaTable
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from transformation.config.table_config import get_table_config
from transformation.curated_writer import CHANGE_DATA_FEED_PROPERTY
from transformation.dq_rules import DQ_ERRORS_COLUMN
from transformation.raw_payload import RAW_PAYLOAD_COLUMN
from transformation.raw_reader import CORRUPT_RECORD_COLUMN

DEAD_LETTER_DIRECTORY = "_dead_letter"  # ADR-012: curated/_dead_letter/, underscore keeps it out of the table namespace
KEY_SEPARATOR = "|"                     # between composite primary key parts
REASON_SEPARATOR = ";"                  # between reason codes in one VARCHAR
DEAD_LETTER_TABLE = "dead_letter"  # its name in served, the manifest and Snowflake raw
DEAD_LETTER_COLUMNS = (
    "record_id", "source_table", "source_key", "error_reason",
    "raw_payload", "failed_at", "reprocessed", "reprocessed_at",
)  # build_dead_letter_rows() output, in order


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
    row_version = F.col(config.watermark_column)    # changes on every source update, so key + version is one row version
    malformed_text = F.col(CORRUPT_RECORD_COLUMN)   # NULL for a parsed row, the exact line for a malformed one

    return df.select(
        # identity is the row version and the reason; raw_payload text is not stable (ADR-016)
        F.xxhash64(source_table, source_key, row_version, error_reason, malformed_text).alias("record_id"),
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
    rows = build_dead_letter_rows(rejected, table).dropDuplicates(["record_id"])  # one batch can carry the same bad row twice
    path = dead_letter_path(curated_root)

    if not DeltaTable.isDeltaTable(spark, path):
        (
            rows.write.format("delta")
            .option(CHANGE_DATA_FEED_PROPERTY, "true")  # the served export reads CDF (ADR-020)
            .save(path)
        )  # first run, even if empty, so the table always exists
        return path

    (
        DeltaTable.forPath(spark, path)
        .alias("target")
        .merge(rows.alias("source"), "target.record_id = source.record_id")
        .whenNotMatchedInsertAll()  # insert only: a known problem is never recorded twice
        .execute()
    )

    return path
