# Databricks notebook source
# MAGIC %md
# MAGIC # Register curated and dead-letter tables in Unity Catalog
# MAGIC
# MAGIC Run once per deployment, after the first pipeline run has created the
# MAGIC paths. Idempotent: CREATE TABLE IF NOT EXISTS. See ADR-010 and ADR-012.

# COMMAND ----------

import sys
from pathlib import Path

REPO_ROOT = str(Path.cwd().parents[1])
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from transformation.config.table_config import TABLE_CONFIGS
from transformation.curated_writer import curated_path
from transformation.dead_letter import dead_letter_path

# COMMAND ----------

dbutils.widgets.text("curated_root", "abfss://curated@retailpipelinedevx7k.dfs.core.windows.net")
curated_root = dbutils.widgets.get("curated_root")

for table in sorted(TABLE_CONFIGS):
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS retail_dev.curated.{table} "
        f"USING DELTA LOCATION '{curated_path(curated_root, table)}'"
    )

spark.sql(
    f"CREATE TABLE IF NOT EXISTS retail_dev.ops.dead_letter "
    f"USING DELTA LOCATION '{dead_letter_path(curated_root)}'"
)

display(spark.sql("SHOW TABLES IN retail_dev.curated"))