"""
Keep one row per primary key: the latest version by the watermark column.

Duplicates are expected, not errors. ADF's lookback window re-copies rows on
purpose, and late-arriving updates add newer versions. Delta MERGE refuses
several source rows for one target row, so the choice is made here.
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from transformation.config.table_config import get_table_config
from transformation.raw_reader import SOURCE_FILE_COLUMN

RANK_COLUMN = "_version_rank"  # temporary: 1 is the version that survives


def dedupe(df: DataFrame, table: str) -> DataFrame:
    """
    Keep the latest version of each primary key.

    Latest means the highest watermark value, with NULLs last. Ties, which
    are the same version copied by overlapping windows, are broken by source
    file name so the same input always gives the same output.

    Args:
        df: Valid rows from split_valid_rejected().
        table: Source table name.

    Returns:
        One row per primary key, same columns as the input.

    Raises:
        ValueError: If the table has no config.
    """
    config = get_table_config(table)

    latest_first = Window.partitionBy(*config.primary_key).orderBy(
        F.col(config.watermark_column).desc_nulls_last(),
        F.col(SOURCE_FILE_COLUMN).desc(),  # deterministic tie-break
    )

    return (
        df.withColumn(RANK_COLUMN, F.row_number().over(latest_first))
        .filter(F.col(RANK_COLUMN) == 1)
        .drop(RANK_COLUMN)
    )