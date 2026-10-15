# Incremental Run Runbook

How to simulate a day of source activity and watch the incremental pipeline
handle it, from Postgres to the dbt marts. This exercises the watermark, the
lookback window, `dedupe()`, the MERGE guard, dead-letter deduplication, the
CDF export, the manifest-driven COPY, staging's change-log dedupe, the
incremental `fact_sales` merge and the DVT reconciliation on real data.

`rebuild-runbook.md` covers the cold start. This one assumes everything is
already deployed and loaded at least once, including one `full_reload` export.

Expect 45 to 60 minutes, most of it waiting on ADF and Databricks jobs.

---

## Prerequisites

- Postgres running:

  ```bash
  az postgres flexible-server start --name retail-pipeline-dev-pg --resource-group retail-pipeline-dev-rg
  ```

- `source session.sh` **in the terminal you run the commands from**, and
  `.env` pointing at Azure rather than local Docker. A bare `psql "$DATABASE_URL"`
  with an empty variable silently connects to localhost instead of failing.

- `venv` active, and `.env` holding `DATABRICKS_HOST`, `DATABRICKS_HTTP_PATH`,
  `DATABRICKS_TOKEN` and `SNOWFLAKE_CONNECTION_NAME` for step 6. The token
  expires after 30 days: an HTTP 403 on the manifest read means renew it.

- `venv-dbt` built once, separate from `venv` because dbt needs protobuf 6 and
  PySpark does not, and `SNOWFLAKE_PRIVATE_KEY_PATH` in `.env` for step 7.
  `python` is 3.12 from `.python-version` (ADR-028):

  ```bash
  python -m venv venv-dbt && ./venv-dbt/bin/pip install -r dbt/requirements.txt
  ```

- `venv-dvt` built once, separate because DVT 8.10 pins numpy below 2 and
  pyarrow below 15 (ADR-025):

  ```bash
  python -m venv venv-dvt && ./venv-dvt/bin/pip install -r validation/requirements.txt
  ```

## 1. Create new and changed rows

```bash
SEED=11 ./scripts/simulate_source_changes.sh
```

Defaults: every day from the newest order up to today at about 20 orders per
day, plus 25 existing orders whose status changes. A second run on the same
day tops up today. Pass `orders_per_day updates` to change that. Use a seed
not used before, so the run is a fresh draw.

The script prints a before and after snapshot, and fails if any order before
today ends up without an FX rate for its date. Today's orders are not
checked: their rate is published tomorrow (ADR-027). The rows are already
committed when that check fails, so **do not run the script again**. Fetch
only the missing dates:

```bash
./scripts/bootstrap_fx_rates.sh <first uncovered date> <yesterday>
```

Without arguments the script refetches every order date up to yesterday. That is safe,
because a rate that has not changed keeps its `updated_at`, but it costs one
API call per date, so an explicit range is quicker.

**Why both kinds of change matter.** New rows exercise INSERT in the MERGE.
Updated rows exercise `dedupe()` picking the newer version and the guard
`source.updated_at > target.updated_at` allowing it. Unchanged rows arrive
anyway, through the lookback window, and exercise the no-op path.

Before moving on, count the changed rows per table (`updated_at` later than the
previous `last_watermark`). Every later step is checked against that ledger.

## 2. Run ADF

Check the current watermarks first. The serverless watermark database may
refuse the first login while it resumes. Retry after a minute.

```bash
sqlcmd -S retail-pipeline-dev-sql.database.windows.net -d watermark-db -U sqladmin -P "$TF_VAR_sql_admin_password" -Q "SELECT pipeline_name, rows_loaded, last_watermark FROM dbo.pipeline_watermark_control ORDER BY pipeline_name;"
```

ADF Studio, `pl_load_data`, Add trigger, Trigger now. Wait for it in
Monitor, then run the same query again.

Expected: every `last_watermark` moves to one shared time, because
`set_window_end` freezes the window once per run. `rows_loaded` covers the
whole lookback window, not only the changed rows. `exchange_rates` loads only
new rates, because its `lookback_days` is 0.

Raw now holds one more file per table, under a new `day=` partition. A table
that copied 0 rows still gets a file: 3 bytes, only a UTF-8 BOM. The
transformation drops it before validation.

**A second trigger is queued**, not run in parallel: `pl_load_data` has
concurrency 1. It starts when the first run finishes and reads its new
watermarks, so it only re-copies the lookback window. Monitor's Run start for
it is the trigger time, including the wait; its `window_end` is the real start.

## 3. Run the transformation

Check the job parameters first: `raw_to_curated`, View as code. A key shown in
quotes (`"reader "`) has a hidden character. Databricks matches parameters to
widgets by exact name and silently ignores the rest, so the widget stays empty.

Then run the job with `reader = batch`.

Expected shape, per table:

- `rows_read` is the sum across all raw files, so it includes the duplicates
  the lookback window re-copied
- `rows_valid` is the distinct primary keys that passed the DQ rules
- the curated row count ends up equal to the source row count after DQ rules
- BOM-only files count nowhere: not in `rows_read`, not in `rows_rejected`

Batch reads every raw file, so a table skipped in an earlier run catches up
here: its missing rows arrive as inserts.

## 4. Verify curated

```sql
SELECT count(*) FROM retail_dev.curated.orders;     -- equals the source count
SELECT count(*) FROM retail_dev.ops.dead_letter;    -- grows only by new bad rows, never on a rerun
```

Each bad source row version is recorded once: `count(*)` equals
`count(DISTINCT record_id)`, and rerunning the job with no new data inserts
nothing (ADR-016 amendment).

```sql
DESCRIBE HISTORY retail_dev.curated.orders LIMIT 3;
```

In the MERGE metrics, `numTargetRowsInserted` equals the new orders and
`numTargetRowsMatchedUpdated` equals the updated ones. Zero updates would mean
either `dedupe()` kept the older version or the guard rejected the newer one.

Expect extra versions you did not write. A MERGE that changes nothing still
commits a version, and Databricks may add an `OPTIMIZE` commit right after a
MERGE.

## 5. Export to served

Check `Curated_to_served` with View as code, as in step 3, and confirm
`full_reload = false`. With `true` the run exports full snapshots and the CDF
path is never tested.

Run the job. Expected per table:

- changed tables: `cdf`, `start_version` to `end_version`, `row_count` equal to
  inserts plus updates from step 4
- unchanged tables: `cdf N -> N`, `row_count 0`, because the no-op MERGE
  still committed a version
- `dead_letter`: `cdf`, `row_count` equal to the new rejects; `cdf N -> N`
  with 0 rows when no new bad rows arrived
- the manifest gains exactly one row per table

```sql
SELECT table_name, export_mode, start_version, end_version, row_count, written_at
FROM retail_dev.ops.served_manifest
ORDER BY written_at DESC
LIMIT 13;
```

Served files hold source columns only, no `_change_type`. Updates carry
today's `updated_at`, so a preimage would show up as an old one.

**Reconcile served against curated.** Run
`transformation/notebooks/reconcile_served.py` with `curated_root` and
`served_root` (`tables` empty for the 12 source tables; `dead_letter` has no
source key and is checked by the loader in step 6). It replays every file the
manifest lists, keeps the latest row per primary key, and compares the result with
curated in both directions. It fails if any table has a difference, so a clean
finish means served rebuilds curated exactly.

## 6. Load into Snowflake raw

Steps 6 to 8 can also run as one Airflow DAG run, see Steps 6 to 8 through
Airflow below.

```bash
python -m loading.load_raw --dry-run
```

```bash
python -m loading.load_raw
```

The dry run reads the manifest through the `retail_loader` SQL warehouse and
lists the files per table without touching Snowflake. The real run checks the
raw schema, COPYs every export of the last 7 days by name, then counts each
export's rows in raw by `_source_file`.

Expected:

- tables changed in step 1: `N files loaded`, rows equal to their CDF export
- every earlier file: `LOAD_SKIPPED`, which is a rerun, not an error
- one `OK` line per manifest row in the window, and no exception

Running it twice changes nothing: the second run shows `0 files loaded` for
every table.

**If it fails:**

- `SchemaDrift(...)`: a raw table differs from `RAW_COLUMNS`. Nothing was
  loaded. Fix the table or redeploy `create_raw_tables.sql`, then rerun.
- `exports not in raw exactly once`: `actual` 0 is a missing load, half is a
  partial one, double is a FORCE reload. Query raw by `_source_file` for the
  named run.
- an export older than 7 days is never loaded: load within the window
  (ADR-020).

## 7. Build dbt staging and marts

```bash
set -a; source .env; set +a
```

```bash
cd dbt && ../venv-dbt/bin/dbt build
```

dbt reads `.env` only through the shell, so export it in the same terminal.
`dbt build` runs every layer in dependency order, each model followed by its
tests: staging views in `DBT_DEV_STAGING`, intermediate views in
`DBT_DEV_INTERMEDIATE`, and the marts as tables in `DBT_DEV_MARTS`. Raw is the
change log; staging dedupes it on `(pk, updated_at)` (ADR-020 History).
`fact_sales` is incremental: it merges only the lines whose line, order or
rate was loaded after its newest `source_loaded_at` (ADR-024).

Expected: `PASS=143`. The count is fixed; the row counts behind it change
with every run.

**Check the increment:**

```sql
select count(*), max(loaded_at), max(source_loaded_at)
from ecommerce_db.dbt_dev_marts.fact_sales;
```

Rows with this run's `loaded_at` must be the new lines plus every line of a
changed order. With no source changes, `loaded_at` does not move.

Today's lines are in `fact_sales_rejected` as `missing_fx_rate`, every
currency, GBP included: a day is published whole once its rates arrive
(ADR-027). After the next day's FX fetch and run they move to `fact_sales`.

**If it fails:**

- `Env var required but not provided`: `.env` not exported in this terminal,
  or the variable is missing from `.env` (not `.env.example`).
- a source `not_null` test: raw has a row without its key or `updated_at`.
  The loader is at fault, not dbt.
- a grain test (`unique`, `unique_version`, `one_current_version`): its SQL in
  `target/compiled/` returns the offending keys; run it in Snowsight.
- `relationships` on `fact_sales.date_key`: an order outside `dim_date`. Widen
  `dim_date_end` in `dbt_project.yml`.
- `assert_every_order_line_lands_once`: a join lost or doubled a line between
  `int_order_lines` and the two marts.
- `invalid identifier` on a `fact_sales` column after a model change: the
  existing table predates it. Run `dbt build --select fact_sales --full-refresh`.
- `syntax error ... unexpected 'select'` after editing the macro: unbalanced
  brackets. `dbt/logs/dbt.log` holds the exact SQL sent.

## 8. Validate with DVT

Read the window end of the ADF run in step 2. The watermark database pauses
after 60 idle minutes; the first connection then fails while it resumes, so
retry after a minute.

```sql
SELECT pipeline_name, last_watermark FROM pipeline_watermark_control ORDER BY pipeline_name;
```

All 12 rows share one value. Pass it as the window end:

```bash
set -a; source .env; set +a
```

```bash
./venv-dvt/bin/python -m validation.run_validations --window-end '<last_watermark>'; echo "exit code: $?"
```

It compares PostgreSQL up to that point with Snowflake staging (ADR-025):
row counts for the 9 tables without reject rules, and for `order_items`,
`payments` and `orders` the newest accepted or rejected version per key, with
sums in integer cents, per currency where there is one. Every result row is
saved to `validation/results/<run_id>.json`.

Expected: `32 validations, 0 failed` and `exit code: 0`.

Do not change the source between step 2 and this step. PostgreSQL keeps no
old versions, so a row updated after the window shows as a sum mismatch.

**If it fails:**

- `exit code: 1` with `ERROR` lines: a mismatch. A count with `source` above
  `target` lost rows between PostgreSQL and staging: check step 6's reconcile
  and `stg_dead_letter`. Equal counts with different sums: a value changed in
  transit, or the source changed after the window.
- every check fails with `source 0` or `None`: the window end is too early or
  not UTC.
- `Could not create DVT connection pg_source`: Postgres stopped, or `.env` not
  exported. `sf_dev`: a `SNOWFLAKE_*` variable or the key path is wrong.
- `DVT returned no result for`: the traceback holds DVT's stderr.
- `Unable to create map UDFs` and `Unknown cast types`: warnings, not errors.

## 9. Stop the source

```bash
az postgres flexible-server stop --name retail-pipeline-dev-pg --resource-group retail-pipeline-dev-rg
```

The SQL watermark database pauses itself after 60 minutes idle, and serverless
compute stops when the run ends.

## Steps 6 to 8 through Airflow

`retail_pipeline` runs `load_raw`, `dbt build` and DVT for one window and
writes a `pipeline_audit` row per task (ADR-029).

Once per machine:

- Docker Desktop running, at least 4 GB memory.
- `private_key_file` in `~/.snowflake/connections.toml` starts with `~`, so the
  same file works in the container.
- `orchestration/.env` with the compose secrets:

  ```bash
  printf "AIRFLOW_UID=%s\nAIRFLOW_JWT_SECRET=%s\nAIRFLOW_FERNET_KEY=%s\n" "$(id -u)" "$(openssl rand -hex 32)" "$(./venv-orchestration/bin/python -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())')" > orchestration/.env
  ```

- `venv-orchestration`, Airflow as in the image, plus pytest for the DAG tests:

  ```bash
  python -m venv venv-orchestration && ./venv-orchestration/bin/pip install "apache-airflow==3.3.2" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.12.txt"
  ```

  ```bash
  ./venv-orchestration/bin/pip install "apache-airflow==3.3.2" -r orchestration/requirements.txt pytest==9.1.1
  ```

Start Airflow, UI at http://localhost:8080, `airflow` / `airflow`:

```bash
docker compose -f orchestration/docker-compose.yml up -d
```

Trigger with the window end from step 8:

```bash
docker compose -f orchestration/docker-compose.yml exec airflow-scheduler airflow dags trigger retail_pipeline --conf '{"window_end": "<last_watermark>"}'
```

Expected in about two minutes: the run `success`, and three audit rows:
`load_raw` SUCCESS, `dbt_build` SUCCESS, `dvt` SUCCESS with MATCH.
`rows_ingested` is 0 until Airflow runs the export itself (Phase C).

**If it fails:**

- `ParamValidationError` at trigger: `window_end` missing or not
  `YYYY-MM-DD HH:MM:SS`. No run is created.
- `dvt` FAILED with MISMATCH: the data differ, see the failed checks in
  `details`. FAILED with no `dvt_status`: no usable result, see the task log.
- Task logs are also in `orchestration/logs/`.
- Do not run dbt on the laptop during a run: both write `dbt/target/`.

Stop Airflow, keeping its history:

```bash
docker compose -f orchestration/docker-compose.yml down
```

---

## Reset run, 2026-10-13

First run after a full data reset (rebuild runbook, Resetting the data), on 90
days ending 2026-10-13. Window end `2026-10-13 20:05:18` UTC.

| stage | value |
|---|---|
| source | 4562 orders, 13883 lines, 2026-07-16 to 2026-10-13 |
| exchange_rates | 356 rows, 2026-07-16 to 2026-10-12, 24 orders awaiting a rate |
| raw_to_curated | order_items 1884 rejected, payments 43 |
| curated_to_served | 13 snapshots, dead_letter 1927 |
| load_raw | 13 exports, every one OK |
| `dbt build --full-refresh` | `PASS=143` |
| DVT | 32 validations, 0 failed |
| fact_sales | 11907 rows, 2026-07-16 to 2026-10-12 |
| fact_sales_rejected | 92 `missing_fx_rate`, all 2026-10-13: EUR 39, GBP 36, CAD 17 |

## Marts build, 2026-10-06

First build of the intermediate and mart layers, over the raw data of the
2026-10-02 run. No source change between the full refresh and the incremental
build, so `fact_sales` merged nothing and `loaded_at` stayed put.

| stage | value |
|---|---|
| `dbt build` | `PASS=137` |
| orders | 2025-01-01 to 2025-02-03 |
| int_order_lines | 4102 lines, 0 orphans |
| fact_sales | 4102 rows |
| fact_sales_rejected | 0 rows, every line has an order and a rate |
| dim_date | 1095 days, 2025-01-01 to 2027-12-31 |
| dim_product, dim_store | 10, 5 |

## Reference run, 2026-10-02

32 new orders and 25 updated ones, so 57 changed rows in the source. ADF was
triggered twice by accident and the runs overlapped.

| stage | value |
|---|---|
| ADF `rows_loaded`, orders | 89 per run, 178 in raw today |
| ADF `rows_loaded`, exchange_rates | 16 per run |
| raw, orders | 4 files, 3220 rows |
| after `dedupe()` | 1569 |
| MERGE | 32 inserted, 25 updated, 57 output rows |
| curated orders | 1569, equal to the source |
| served, orders | `cdf 12 -> 13`, 57 rows, no preimages |
| served, static tables | `cdf N -> N`, 0 rows |
| served replay vs curated | 1594 rows, 1569 keys, 0 differences |
| raw (Snowflake), first load | 22 files, 18 exports, all `OK` |
| raw (Snowflake), rerun | 22 `LOAD_SKIPPED`, 18 `OK` |
| raw, orders | 1594 rows: 1537 snapshot + 57 CDF |
| staging, orders | 1594 versions, 25 closed, 1569 current |
| staging, customers | 1451 versions, 1451 current |
| staging, products | 10 versions, 10 current |
| staging, payments | 1547 versions, 1547 current |
| `dbt build` | `PASS=77` |
| dead_letter, before the fixes | 624 to 665: 14 new bad rows stored twice, plus 13 BOM rows |
| dead_letter, rebuilt after them | 638 (616 order_items + 22 payments), still 638 on a second run |

`customer_addresses` exported 71 rows, not 36: an earlier run had skipped the
table, and batch reading inserted the 35 missing rows.

This run exposed three dead-letter bugs, all fixed the same day: duplicates
within one batch, BOM files recorded as rejects, and a `record_id` that hashed
unstable payload text (a rerun with no new data added 111 rows). `dead_letter`
was then emptied and rebuilt from raw.

## Reference run, 2026-09-25

32 new orders and 25 updated ones, so 57 changed rows in the source.

| stage | value |
|---|---|
| ADF `rows_loaded`, orders | 1537, the whole lookback window |
| ADF `rows_loaded`, exchange_rates | 0 |
| raw, orders | 2 files, 3042 rows |
| after `dedupe()` | 1537 |
| MERGE | 32 inserted, 25 updated, 57 output rows |
| curated orders | 1537, equal to the source |
| dead_letter | 611 to 624, with no duplicates of the earlier rows |

Absolute numbers will differ next time. The relationships should not:
`rows_read` equals the sum of the files, `rows_valid` equals the distinct keys,
inserted plus updated equals the rows that actually changed in the source,
a rerun adds nothing to dead_letter, and the served replay equals curated.
