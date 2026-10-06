# Databricks notebook source
# MAGIC %md
# MAGIC # Curated to served
# MAGIC
# MAGIC Exports what changed in each curated table, and in dead_letter, since its
# MAGIC last export as Parquet into the served zone, and records each export in the
# MAGIC manifest. All logic lives in `transformation/served_pipeline.py`. See ADR-020.l logic lives in `transformation/served_pipeline.py`. See ADR-020.

# COMMAND ----------

import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from uuid import uuid4

REPO_ROOT = str(Path.cwd().parents[1])  # this notebook sits in <repo>/transformation/notebooks
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from transformation.config.table_config import TABLE_CONFIGS
from transformation.dead_letter import DEAD_LETTER_TABLE
from transformation.served_pipeline import export_table, parse_flag
from transformation.spark_session import apply_required_configs

# COMMAND ----------

# No defaults: a missing parameter must fail, not quietly run against dev
for name in ("curated_root", "served_root", "run_id", "run_date", "tables", "full_reload"):
    dbutils.widgets.text(name, "")

curated_root = dbutils.widgets.get("curated_root")
served_root = dbutils.widgets.get("served_root")
run_id = dbutils.widgets.get("run_id")

if not curated_root or not served_root or not run_id:
    raise ValueError("curated_root, served_root and run_id are required")

run_date = date.fromisoformat(dbutils.widgets.get("run_date"))  # YYYY-MM-DD, fails otherwise
full_reload = parse_flag("full_reload", dbutils.widgets.get("full_reload"))

requested = [t.strip() for t in dbutils.widgets.get("tables").split(",") if t.strip()]
tables = requested or [*sorted(TABLE_CONFIGS), DEAD_LETTER_TABLE]  # dead_letter last: 01 writes it, 02 exports it

print(
    f"run_id={run_id}, run_date={run_date}, full_reload={full_reload}, "
    f"curated_root={curated_root}, served_root={served_root}, tables={len(tables)}"
)

apply_required_configs(spark)  # ANSI and UTC, ADR-015 and ADR-017

# COMMAND ----------

summaries = {}
for table in tables:
    summaries[table] = export_table(
        spark, curated_root, served_root, table,
        run_id=run_id,
        run_date=run_date,
        full_reload=full_reload,
        written_at=datetime.now(timezone.utc),
        export_id=str(uuid4()),  # new per attempt, so a retry never collides
    )
    print(f"{table}: {summaries[table] or 'no new versions'}")

# COMMAND ----------

# Airflow will write these to pipeline_audit
dbutils.notebook.exit(json.dumps(summaries))
