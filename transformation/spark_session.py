"""
Spark session setup for the transformation layer.

Two environments run this code: a local session for tests, and Databricks
serverless, which provides its own `spark`. REQUIRED_CONFIGS is applied in
both, so they agree by declaration rather than by coincidence. See ADR-015.
"""

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


# Change results, not speed. Must hold on Databricks too.
REQUIRED_CONFIGS: dict[str, str] = {
    "spark.sql.ansi.enabled": "true",      # overflow and divide-by-zero in our own code fail loudly
    "spark.sql.session.timeZone": "UTC",   # to_date() picks the same calendar day on every machine
}

# Local only. Databricks serverless manages these itself.
LOCAL_CONFIGS: dict[str, str] = {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.sql.shuffle.partitions": "4",   # the default 200 is tuned for a cluster, not a laptop
}

LOCAL_MASTER = "local[*]"                  # one JVM on this machine, one worker thread per CPU core
DEFAULT_APP_NAME = "retail-pipeline-local"


def build_local_spark(app_name: str = DEFAULT_APP_NAME) -> SparkSession:
    """
    Build a local Spark session with Delta Lake and the required configs.

    Used by tests and local runs. On Databricks, call apply_required_configs()
    on the session Databricks provides instead.

    Args:
        app_name: Name shown in the Spark UI and in logs.

    Returns:
        A SparkSession with Delta enabled, ANSI on and the session in UTC.
    """
    builder = SparkSession.builder.master(LOCAL_MASTER).appName(app_name)

    for key, value in {**LOCAL_CONFIGS, **REQUIRED_CONFIGS}.items():
        builder = builder.config(key, value)

    # Adds the Delta jars matching the installed delta-spark version to spark.jars.packages
    return configure_spark_with_delta_pip(builder).getOrCreate()


def apply_required_configs(spark: SparkSession) -> None:
    """
    Set REQUIRED_CONFIGS on an existing session.

    Databricks notebooks call this on the `spark` they are given, so the
    workspace and the local tests run with identical ANSI and time zone rules.

    Args:
        spark: An active SparkSession.
    """
    for key, value in REQUIRED_CONFIGS.items():
        spark.conf.set(key, value)