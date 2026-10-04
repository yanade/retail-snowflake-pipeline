# Incremental Run Runbook

How to simulate a day of source activity and watch the incremental pipeline
handle it, from Postgres to dbt staging. This exercises the watermark, the
lookback window, `dedupe()`, the MERGE guard, dead-letter deduplication, the
CDF export, the manifest-driven COPY and staging's change-log dedupe on real data.

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
  PySpark does not, and `SNOWFLAKE_PRIVATE_KEY_PATH` in `.env` for step 7:

  ```bash
  python3.13 -m venv venv-dbt && ./venv-dbt/bin/pip install -r dbt/requirements.txt
  ```

## 1. Create new and changed rows

```bash
SEED=11 ./scripts/simulate_source_changes.sh
```

Defaults: 2 new days at about 20 orders per day, plus 25 existing orders whose
status changes. Pass `days orders_per_day updates` to change that. Use a seed
not used before, so the run is a fresh draw.

The script prints a before and after snapshot, and fails if any order ends up
without an FX rate for its date. The rows are already committed when that
check fails, so **do not run the script again**. Fetch only the missing dates:

```bash
./scripts/bootstrap_fx_rates.sh 2025-02-03 2025-02-06   # first uncovered date, last order date + 3
```

Without arguments the script refetches every order date. That is safe,
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
- the manifest gains exactly one row per table

```sql
SELECT table_name, export_mode, start_version, end_version, row_count, written_at
FROM retail_dev.ops.served_manifest
ORDER BY written_at DESC
LIMIT 12;
```

Served files hold source columns only, no `_change_type`. Updates carry
today's `updated_at`, so a preimage would show up as an old one.

**Reconcile served against curated.** Run
`transformation/notebooks/reconcile_served.py` with `curated_root` and
`served_root` (`tables` empty for all 12). It replays every file the manifest
lists, keeps the latest row per primary key, and compares the result with
curated in both directions. It fails if any table has a difference, so a clean
finish means served rebuilds curated exactly.

## 6. Load into Snowflake raw

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

## 7. Build dbt staging

```bash
set -a; source .env; set +a
```

```bash
cd dbt && ../venv-dbt/bin/dbt build
```

dbt reads `.env` only through the shell, so export it in the same terminal.
`dbt build` runs the source tests, then for each staging model its unit test,
the view itself in `DBT_DEV_STAGING`, and its grain tests. Raw is the change
log; staging dedupes it on `(pk, updated_at)` and derives SCD2 intervals with
`lead()` (ADR-020 History).

Expected: `PASS=77` (24 source tests, 1 unit test, 16 views, 36 grain tests).
The count is fixed; the row counts behind it change with every run.

**If it fails:**

- `Env var required but not provided`: `.env` not exported in this terminal,
  or the variable is missing from `.env` (not `.env.example`).
- a source `not_null` test: raw has a row without its key or `updated_at`.
  The loader is at fault, not dbt.
- a grain test (`unique`, `unique_version`, `one_current_version`): its SQL in
  `target/compiled/` returns the offending keys; run it in Snowsight.
- `syntax error ... unexpected 'select'` after editing the macro: unbalanced
  brackets. `dbt/logs/dbt.log` holds the exact SQL sent.

## 8. Stop the source

```bash
az postgres flexible-server stop --name retail-pipeline-dev-pg --resource-group retail-pipeline-dev-rg
```

The SQL watermark database pauses itself after 60 minutes idle, and serverless
compute stops when the run ends.

---

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
