"""
Run one batch of raw rows through validation and into the two outputs.

Shared by both readers: the batch reader passes a DataFrame directly, and the
Auto Loader path passes each micro-batch from foreachBatch. Keeping it here
means the streaming wiring holds no logic, because it cannot be tested.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from transformation.curated_writer import merge_into_curated
from transformation.dead_letter import write_dead_letter
from transformation.dedupe import dedupe
from transformation.dq_rules import apply_dq_rules, split_valid_rejected
from transformation.raw_payload import add_raw_payload
from transformation.raw_reader import CORRUPT_RECORD_COLUMN
from transformation.type_casting import cast_to_target


ARTIFACT_CHARACTERS = "[\\s\ufeff]"  # whitespace and the UTF-8 BOM; Java's \s does not include the BOM


def drop_empty_artifacts(raw: DataFrame) -> DataFrame:
    """
    Drop records that hold nothing but a BOM or whitespace, as ADF writes for a 0-row copy.

    Args:
        raw: Output of read_raw() or one Auto Loader micro-batch.

    Returns:
        The same rows minus the artifacts.
    """
    is_artifact = F.regexp_replace(F.col(CORRUPT_RECORD_COLUMN), ARTIFACT_CHARACTERS, "") == ""
    return raw.where(~F.coalesce(is_artifact, F.lit(False)))  # NULL means the line parsed: keep it


def process_raw_batch(
    spark: SparkSession, raw: DataFrame, table: str, curated_root: str
) -> dict[str, int]:
    """
    Validate one batch of raw rows and write both outputs.

    Args:
        spark: Active SparkSession.
        raw: Output of read_raw() or one Auto Loader micro-batch.
        table: Source table name.
        curated_root: Root of the curated zone.

    Returns:
        Row counts for the audit record: rows_read, rows_valid, rows_rejected.

    Raises:
        ValueError: If the table has no schema or config.
    """
    checked = apply_dq_rules(cast_to_target(add_raw_payload(drop_empty_artifacts(raw)), table), table)
    valid, rejected = split_valid_rejected(checked)
    deduped = dedupe(valid, table)

    counts = {
        "rows_read": checked.count(),
        "rows_valid": deduped.count(),
        "rows_rejected": rejected.count(),
    }

    merge_into_curated(spark, deduped, table, curated_root)
    write_dead_letter(spark, rejected, table, curated_root)

    return counts