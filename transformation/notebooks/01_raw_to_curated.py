# Databricks notebook source
# MAGIC %md
# MAGIC # Raw to curated
# MAGIC
# MAGIC Reads one JSON zone per source table, validates it, and writes two Delta
# MAGIC outputs: valid rows into `curated/<table>`, rejected rows into
# MAGIC `curated/_dead_letter`. All logic lives in `transformation/`, which is
# MAGIC unit tested. This notebook only wires it together and reports counts.

# COMMAND ----------

import json
import sys
from pathlib import Path

REPO_ROOT = str(Path.cwd().parents[1])  # this notebook sits in <repo>/transformation/notebooks
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from transformation.curated_writer import curated_path, merge_into_curated
from transformation.dead_letter import write_dead_letter
from transformation.dedupe import dedupe
from transformation.dq_rules import apply_dq_rules, split_valid_rejected
from transformation.raw_payload import add_raw_payload
from transformation.raw_reader import read_raw
from transformation.spark_session import apply_required_configs
from transformation.type_casting import cast_to_target
from transformation.config.table_config import TABLE_CONFIGS

# COMMAND ----------

# Parameters, so Airflow can pass different zones or a subset of tables
dbutils.widgets.text("raw_root", "abfss://raw@retailpipelinedevx7k.dfs.core.windows.net")
dbutils.widgets.text("curated_root", "abfss://curated@retailpipelinedevx7k.dfs.core.windows.net")
dbutils.widgets.text("tables", "")

raw_root = dbutils.widgets.get("raw_root")
curated_root = dbutils.widgets.get("curated_root")
requested = [t.strip() for t in dbutils.widgets.get("tables").split(",") if t.strip()]
tables = requested or sorted(TABLE_CONFIGS)  # the registry is the source of truth

apply_required_configs(spark)  # ANSI and UTC, ADR-015 and ADR-017

# COMMAND ----------

def process(table: str) -> dict[str, int]:
    """
    Run one source table from raw to curated and dead-letter.

    Args:
        table: Source table name.

    Returns:
        Row counts for this table, for the audit record.
    """
    checked = apply_dq_rules(
        cast_to_target(add_raw_payload(read_raw(spark, table, raw_root)), table),
        table,
    )
    valid, rejected = split_valid_rejected(checked)
    deduped = dedupe(valid, table)

    # count before writing: the same DataFrames are reused below
    counts = {
        "rows_read": checked.count(),
        "rows_valid": deduped.count(),
        "rows_rejected": rejected.count(),
    }

    merge_into_curated(spark, deduped, table, curated_root)
    write_dead_letter(spark, rejected, table, curated_root)

    return counts

# COMMAND ----------

metrics = {table: process(table) for table in tables}

for table, counts in metrics.items():
    print(f"{table}: {counts}")

# COMMAND ----------

# Hand the counts back to the caller. Airflow will write them to pipeline_audit.
dbutils.notebook.exit(json.dumps(metrics))