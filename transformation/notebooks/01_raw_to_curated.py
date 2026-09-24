# Databricks notebook source
# MAGIC %md
# MAGIC # Raw to curated
# MAGIC
# MAGIC Reads one JSON zone per source table, validates it, and writes two Delta
# MAGIC outputs: valid rows into `curated/<table>`, rejected rows into
# MAGIC `curated/_dead_letter`. All logic lives in `transformation/`, which is
# MAGIC unit tested. This notebook only wires it together and reports counts.
# MAGIC
# MAGIC The `reader` parameter picks how the raw zone is read:
# MAGIC
# MAGIC - `batch`: every file, every run. Correct because the writes are idempotent.
# MAGIC - `stream`: Auto Loader, only files not yet in the checkpoint.

# COMMAND ----------

import json
import sys
from pathlib import Path

REPO_ROOT = str(Path.cwd().parents[1])  # this notebook sits in <repo>/transformation/notebooks
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)


from transformation.config.table_config import TABLE_CONFIGS
from transformation.curated_writer import checkpoint_path
from transformation.pipeline import process_raw_batch
from transformation.raw_reader import read_raw, read_raw_stream
from transformation.spark_session import apply_required_configs

# COMMAND ----------

# Parameters, so Airflow can pass different zones, a subset of tables or a reader
dbutils.widgets.text("raw_root", "abfss://raw@retailpipelinedevx7k.dfs.core.windows.net")
dbutils.widgets.text("curated_root", "abfss://curated@retailpipelinedevx7k.dfs.core.windows.net")
dbutils.widgets.text("tables", "")
dbutils.widgets.dropdown("reader", "batch", ["batch", "stream"])

raw_root = dbutils.widgets.get("raw_root")
curated_root = dbutils.widgets.get("curated_root")
reader = dbutils.widgets.get("reader")

requested = [t.strip() for t in dbutils.widgets.get("tables").split(",") if t.strip()]
tables = requested or sorted(TABLE_CONFIGS)  # the registry is the source of truth

apply_required_configs(spark)  # ANSI and UTC, ADR-015 and ADR-017

# COMMAND ----------

def process_batch(table: str) -> dict[str, int]:
    """
    Read every raw file for one table, then process it.

    Args:
        table: Source table name.

    Returns:
        Row counts for the audit record.
    """
    return process_raw_batch(spark, read_raw(spark, table, raw_root), table, curated_root)


def process_stream(table: str) -> dict[str, int]:
    """
    Process only the raw files Auto Loader has not seen before.

    Args:
        table: Source table name.

    Returns:
        Row counts summed over every micro-batch. All zeros means no new files.
    """
    totals = {"rows_read": 0, "rows_valid": 0, "rows_rejected": 0}

    def handle_micro_batch(batch_df, batch_id: int) -> None:
        # inside foreachBatch the micro-batch is an ordinary DataFrame, so the
        # window function in dedupe() and Delta MERGE work as in the batch path
        for key, value in process_raw_batch(spark, batch_df, table, curated_root).items():
            totals[key] += value  # availableNow can produce several micro-batches

    (
        read_raw_stream(spark, table, raw_root)
        .writeStream
        .option("checkpointLocation", checkpoint_path(curated_root, table))  # which files are already processed
        .trigger(availableNow=True)  # process what is available now, then stop
        .foreachBatch(handle_micro_batch)
        .start()
        .awaitTermination()
    )

    return totals


process = process_stream if reader == "stream" else process_batch

# COMMAND ----------

metrics = {table: process(table) for table in tables}

for table, counts in metrics.items():
    print(f"{table}: {counts}")

# COMMAND ----------

# Hand the counts back to the caller. Airflow will write them to pipeline_audit.
dbutils.notebook.exit(json.dumps(metrics))