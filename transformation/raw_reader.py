"""
Read one source table from the raw zone into a DataFrame of strings.

"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from transformation.schemas.source_schemas import get_source_schema, to_read_schema

CORRUPT_RECORD_COLUMN = "_corrupt_record"  # the whole original line, when it isn't valid JSON
SOURCE_FILE_COLUMN = "_source_file"        # which raw file, and so which ADF run, a row came from
READ_MODE = "PERMISSIVE"                   # keep malformed lines as rows, don't fail or drop them
CLOUD_FILES_FORMAT = "json"          # Auto Loader's file format option
RESCUED_DATA_COLUMN = "_rescued_data"  # unexpected columns land here instead of vanishing

# ADF's year=/month=/day= folders are EXTRACTION dates, not business dates
EXTRACTION_PARTITION_COLUMNS = ("year", "month", "day")


def _read_schema(table: str) -> StructType:
    """
    The all-string schema used to read one table's raw JSON.

    Args:
        table: Source table name.

    Returns:
        Every source column as a string, plus the corrupt-record column, which
        Spark only fills when it is part of the schema.
    """
    return to_read_schema(get_source_schema(table)).add(
        StructField(CORRUPT_RECORD_COLUMN, StringType(), True)
    )

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
    # Spark only fills the corrupt-record column if it is part of the schema
    read_schema = _read_schema(table)

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


def read_raw_stream(spark: SparkSession, table: str, raw_root: str) -> DataFrame:
    """
    Read only the raw files Auto Loader has not processed yet.

    Same contract as read_raw(), but returns a streaming DataFrame: which files
    were already read is tracked in the checkpoint passed to writeStream, not
    here. Auto Loader is a Databricks feature, so this path cannot run locally
    or in CI, which is why it holds no logic.

    Args:
        spark: Active SparkSession.
        table: Source table name.
        raw_root: Root of the raw zone.

    Returns:
        A streaming DataFrame with the same columns as read_raw().

    Raises:
        ValueError: If the table has no declared schema.
    """
    stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", CLOUD_FILES_FORMAT)
        .option("cloudFiles.schemaEvolutionMode", "none")   # we declare the schema, so nothing evolves silently
        .option("rescuedDataColumn", RESCUED_DATA_COLUMN)   # a new upstream column is kept, not dropped
        .option("mode", READ_MODE)
        .option("columnNameOfCorruptRecord", CORRUPT_RECORD_COLUMN)
        .schema(_read_schema(table))
        .load(f"{raw_root}/{table}")
    )

    return (
        stream.withColumn(SOURCE_FILE_COLUMN, F.col("_metadata.file_path"))
        .drop(*EXTRACTION_PARTITION_COLUMNS)
    )