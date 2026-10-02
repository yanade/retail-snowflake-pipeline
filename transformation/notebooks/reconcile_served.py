# Databricks notebook source
# MAGIC %md
# MAGIC # Reconcile served against curated
# MAGIC
# MAGIC Replays every file the manifest lists for each table and compares the
# MAGIC newest row per key with curated, in both directions. A verification
# MAGIC tool, not a pipeline stage. All logic lives in
# MAGIC `transformation/served_reconcile.py`. See ADR-020.

# COMMAND ----------

import json
import sys
from pathlib import Path

REPO_ROOT = str(Path.cwd().parents[1])  # this notebook sits in <repo>/transformation/notebooks
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from transformation.config.table_config import TABLE_CONFIGS
from transformation.served_reconcile import reconcile
from transformation.spark_session import apply_required_configs

# COMMAND ----------

# No defaults: a missing parameter must fail, not quietly run against dev
for name in ("curated_root", "served_root", "tables"):
    dbutils.widgets.text(name, "")

curated_root = dbutils.widgets.get("curated_root")
served_root = dbutils.widgets.get("served_root")

if not curated_root or not served_root:
    raise ValueError("curated_root and served_root are required")

requested = [t.strip() for t in dbutils.widgets.get("tables").split(",") if t.strip()]
tables = requested or sorted(TABLE_CONFIGS)

print(f"curated_root={curated_root}, served_root={served_root}, tables={len(tables)}")

apply_required_configs(spark)  # ANSI and UTC, ADR-015 and ADR-017

# COMMAND ----------

results = {table: reconcile(spark, curated_root, served_root, table) for table in tables}

for table, counts in results.items():
    print(f"{table}: {counts}")

mismatched = [t for t, c in results.items() if c["only_in_served"] or c["only_in_curated"]]
if mismatched:
    raise ValueError(f"served does not rebuild curated for: {mismatched}")

# COMMAND ----------

dbutils.notebook.exit(json.dumps(results))
