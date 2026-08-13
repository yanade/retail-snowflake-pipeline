-- Unity Catalog objects for the Databricks transformation layer.
-- Run once, in the Databricks SQL Editor, after the prerequisites below.



CREATE CATALOG IF NOT EXISTS retail_dev
MANAGED LOCATION 'abfss://managed@retailpipelinedev.dfs.core.windows.net/'
COMMENT 'Retail pipeline, dev environment. Managed location is deliberately empty: all tables are external. See ADR-014';


-- Schemas -------------------------------------------------------------------

-- Split by lifecycle, not by tidiness. curated is rewritten by every pipeline
-- run, served is rebuilt from curated, ops accumulates and is never rebuilt.
-- That difference is what makes them safe to grant, drop or retain separately.

-- One Delta table per retail_oltp source table, merged on primary key.

CREATE SCHEMA IF NOT EXISTS retail_dev.curated
COMMENT 'One Delta table per retail_oltp source table, merged on primary key';

-- Flat enriched output. Grain is one row per order_items.order_item_id and
-- must never be violated. See ADR-011.
CREATE SCHEMA IF NOT EXISTS retail_dev.served
COMMENT 'Flat enriched output, Snowflake-ready. See ADR-011 for grain';

-- dead_letter lives at curated/_dead_letter/ as Delta until the Snowflake
-- warehouse exists. The schema is a name; the bytes sit in the curated
-- container. See ADR-012.
CREATE SCHEMA IF NOT EXISTS retail_dev.ops
COMMENT 'Dead letter and pipeline audit. See ADR-012';


-- Verification --------------------------------------------------------------

-- Expect four rows: curated, served, ops, and the auto-created default.
SHOW SCHEMAS IN retail_dev;

-- Expect the managed container to be empty. Schemas are metadata only, so
-- nothing above writes a single byte to storage.
LIST 'abfss://managed@retailpipelinedev.dfs.core.windows.net/';
