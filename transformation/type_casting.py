"""
Convert the all-string raw columns to their declared target types.

try_cast turns a value that cannot be converted into NULL instead of failing
the batch. Because a NULL could also mean the value was simply missing, the
columns that genuinely failed are recorded in _cast_errors, so the DQ step can
send the row to dead-letter with a reason. See ADR-015.
"""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructField

from transformation.schemas.source_schemas import SOURCE_SCHEMAS

CAST_ERRORS_COLUMN = "_cast_errors"  # names of the columns whose value was present but would not convert


def _typed(field: StructField) -> Column:
    """
    Convert one string column to its target type, NULL if it cannot be converted.

    Args:
        field: The target field from SOURCE_SCHEMAS.

    Returns:
        A Column expression of the target type.
    """
    return F.col(field.name).try_cast(field.dataType)


def _failed_cast(field: StructField) -> Column:
    """
    Flag a value that was present in the raw data but did not convert.

    A NULL raw value is a missing value, not a failed conversion, so it is
    not flagged here. Whether missing is acceptable is a DQ rule.

    Args:
        field: The target field from SOURCE_SCHEMAS.

    Returns:
        The column name as a string literal when the cast failed, else NULL.
    """
    return F.when(
        F.col(field.name).isNotNull() & _typed(field).isNull(),
        F.lit(field.name),
    )


def cast_to_target(df: DataFrame, table: str) -> DataFrame:
    """
    Cast every source column of read_raw() output to its target type.

    Args:
        df: Output of read_raw(): source columns as strings plus metadata columns.
        table: Source table name, a key of SOURCE_SCHEMAS.

    Returns:
        The source columns in their target types, the metadata columns
        unchanged, and _cast_errors listing any columns that failed to convert.

    Raises:
        ValueError: If the table has no declared schema.
    """
    if table not in SOURCE_SCHEMAS:
        raise ValueError(
            f"Unknown source table '{table}'. Expected one of: {sorted(SOURCE_SCHEMAS)}"
        )

    target = SOURCE_SCHEMAS[table]
    source_columns = set(target.fieldNames())
    metadata_columns = [c for c in df.columns if c not in source_columns]  # _corrupt_record, _source_file

    return df.select(
        *[_typed(f).alias(f.name) for f in target.fields],
        *metadata_columns,
        # array() collects one entry per column, array_compact() drops the NULLs
        F.array_compact(F.array(*[_failed_cast(f) for f in target.fields])).alias(CAST_ERRORS_COLUMN),
    )