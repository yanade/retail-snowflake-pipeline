# Incremental Run Runbook

How to simulate a day of source activity and watch the incremental pipeline
handle it. This exercises the watermark, the lookback window, `dedupe()`, the
MERGE guard and dead-letter deduplication on real data.

`rebuild-runbook.md` covers the cold start. This one assumes everything is
already deployed and loaded at least once.

Expect 20 to 30 minutes, most of it waiting on ADF.

---

## Prerequisites

- Postgres running:

  ```bash
  az postgres flexible-server start --name retail-pipeline-dev-pg --resource-group retail-pipeline-dev-rg
  ```

- `source session.sh`, and `.env` pointing at Azure rather than local Docker

## 1. Create new and changed rows

```bash
./scripts/simulate_source_changes.sh
```

Defaults: 2 new days at about 20 orders per day, plus 25 existing orders whose
status changes. Pass `days orders_per_day updates` to change that, or
`SEED=11` for a different draw.

The script prints a before and after snapshot, and fails if any order ends up
without an FX rate for its date. If it does fail that way:

```bash
./scripts/bootstrap_fx_rates.sh
```

**Why both kinds of change matter.** New rows exercise INSERT in the MERGE.
Updated rows exercise `dedupe()` picking the newer version and the guard
`source.updated_at > target.updated_at` allowing it. Unchanged rows arrive
anyway, through the lookback window, and exercise the no-op path.

## 2. Run ADF

ADF Studio, `pl_load_data`, Add trigger, Trigger now. Then:

```bash
sqlcmd -S retail-pipeline-dev-sql.database.windows.net -d watermark-db -U sqladmin -P "$TF_VAR_sql_admin_password" -Q "SELECT pipeline_name, rows_loaded, last_watermark FROM dbo.pipeline_watermark_control ORDER BY pipeline_name;"
```

Expected: every `last_watermark` moves to one shared time, because
`set_window_end` freezes the window once per run. `rows_loaded` covers the
whole lookback window, not only the changed rows. `exchange_rates` loads 0
unless new rates were fetched, because its `lookback_days` is 0.

Raw now holds one more file per table, under a new `day=` partition.

## 3. Run the transformation

Databricks job `raw_to_curated` with `reader = batch`.

Expected shape, per table:

- `rows_read` is the sum across all raw files, so it includes the duplicates
  the lookback window re-copied
- `rows_valid` is the distinct primary keys that passed the DQ rules
- the curated row count ends up equal to the source row count

## 4. Verify

```sql
SELECT count(*) FROM retail_dev.curated.orders;     -- equals the source count
SELECT count(*) FROM retail_dev.ops.dead_letter;    -- grows only by genuinely new bad rows
```

```python
from delta.tables import DeltaTable
display(
    DeltaTable.forPath(spark, curated_path(CURATED_ROOT, "orders"))
    .history().select("version", "operation", "operationMetrics").limit(1)
)
```

In the MERGE metrics, `numTargetRowsInserted` equals the new orders and
`numTargetRowsMatchedUpdated` equals the updated ones. Zero updates would mean
either `dedupe()` kept the older version or the guard rejected the newer one.

```sql
SELECT order_status, count(*)
FROM retail_dev.curated.orders
WHERE order_id IN (SELECT order_id FROM retail_dev.curated.orders ORDER BY order_id LIMIT 25)
GROUP BY order_status;
```

Only the statuses the script set should appear, since it always updates the
lowest 25 order ids.

## 5. Stop the source

```bash
az postgres flexible-server stop --name retail-pipeline-dev-pg --resource-group retail-pipeline-dev-rg
```

The SQL watermark database pauses itself after 60 minutes idle, and serverless
compute stops when the run ends.

---

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
and inserted plus updated equals the rows that actually changed in the source.
