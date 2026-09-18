"""
Preserve each row's original values before any conversion.

Dead-letter's raw_payload must show what actually arrived. After
cast_to_target(), a value that failed to convert is NULL, so the payload has
to be captured before casting, while the original strings still exist.

Pipeline order: read_raw() -> add_raw_payload() -> cast_to_target()
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from transformation.raw_reader import CORRUPT_RECORD_COLUMN, SOURCE_FILE_COLUMN

RAW_PAYLOAD_COLUMN = "_raw_payload"  # the original record as JSON text, for dead-letter
READER_METADATA_COLUMNS = (CORRUPT_RECORD_COLUMN, SOURCE_FILE_COLUMN)  # added by read_raw(), not source data


def add_raw_payload(df: DataFrame) -> DataFrame:
    """
    Keep a JSON copy of each row's original values, before any conversion.

    For a line that wasn't valid JSON, every source column is NULL, so the
    payload is the original line itself, taken from _corrupt_record.

    Args:
        df: Output of read_raw(): source columns as strings plus reader metadata.

    Returns:
        The same DataFrame with _raw_payload added.
    """
    source_columns = [c for c in df.columns if c not in READER_METADATA_COLUMNS]

    # Keep explicit nulls: "was null" and "was absent" are different evidence
    as_json = F.to_json(F.struct(*source_columns), {"ignoreNullFields": "false"})

    return df.withColumn(
        RAW_PAYLOAD_COLUMN,
        F.coalesce(F.col(CORRUPT_RECORD_COLUMN), as_json),  # malformed line: keep its exact text
    )