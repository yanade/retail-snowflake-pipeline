"""
Read one source table from the raw zone into a DataFrame of strings.

"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField

from transformation.schemas.source_schemas import SOURCE_SCHEMAS, to_read_schema

CORRUPT_RECORD_COLUMN = "_corrupt_record"  # the whole original line, when it isn't valid JSON
SOURCE_FILE_COLUMN = "_source_file"        # which raw file, and so which ADF run, a row came from
READ_MODE = "PERMISSIVE"                   # keep malformed lines as rows, don't fail or drop them

# ADF's year=/month=/day= folders are EXTRACTION dates, not business dates
EXTRACTION_PARTITION_COLUMNS = ("year", "month", "day")


def read_raw(spark: SparkSession, table: str, raw_root: str) -> DataFrame:
    """
    Read every raw JSON file for one source table, with all columns as strings.

    Args:
        spark: Active SparkSession.
        table: Source table name, a key of SOURCE_SCHEMAS.
        raw_root: Root of the raw zone, e.g. a local folder in tests or
            abfss://raw@<account>.dfs.core.windows.net on Databricks.

    Returns:
        One row per JSON line: the source columns as strings, plus
        _corrupt_record and _source_file.

    Raises:
        ValueError: If the table has no declared schema.
    """
    if table not in SOURCE_SCHEMAS:
        raise ValueError(
            f"Unknown source table '{table}'. Expected one of: {sorted(SOURCE_SCHEMAS)}"
        )

    # Spark only fills the corrupt-record column if it is part of the schema
    read_schema = to_read_schema(SOURCE_SCHEMAS[table]).add(
        StructField(CORRUPT_RECORD_COLUMN, StringType(), True)
    )

    df = (
        spark.read.schema(read_schema)
        .option("mode", READ_MODE)
        .option("columnNameOfCorruptRecord", CORRUPT_RECORD_COLUMN)
        .json(f"{raw_root}/{table}")
    )

    return (
        df.withColumn(SOURCE_FILE_COLUMN, F.col("_metadata.file_path"))  # hidden column every file source provides
        .drop(*EXTRACTION_PARTITION_COLUMNS)
    )