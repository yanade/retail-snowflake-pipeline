# Incremental Run Runbook

How to simulate a day of source activity and watch the incremental pipeline
handle it, from Postgres to the served zone. This exercises the watermark, the
lookback window, `dedupe()`, the MERGE guard, dead-letter deduplication and the
CDF export on real data.

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

An explicit range matters. With no arguments the script refetches every date,
and the unconditional upsert refreshes `updated_at` on every existing rate, so
every layer downstream sees fake changes.

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

ADF Studio, `pl_load_data`, Add trigger, Trigger now, **once**. Wait for it in
Monitor, then run the same query again.

Expected: every `last_watermark` moves to one shared time, because
`set_window_end` freezes the window once per run. `rows_loaded` covers the
whole lookback window, not only the changed rows. `exchange_rates` loads only
new rates, because its `lookback_days` is 0.

Raw now holds one more file per table, under a new `day=` partition. A table
that copied 0 rows still gets a file: 3 bytes, only a UTF-8 BOM. The
transformation drops it before validation.

**If two runs overlap**, the data stays correct (lookback plus `dedupe()`), but
the bookkeeping does not: `rows_loaded` shows whichever run wrote last, and the
watermarks can end up split between the two `window_end` values.

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

## 6. Stop the source

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
