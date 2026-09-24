-- Register the Delta tables written by 01_raw_to_curated.py as external tables
-- in Unity Catalog, so they are queryable by name rather than by path.
-- Run once per deployment, after the first pipeline run has created the paths.
-- Catalog and schemas come from unity_catalog_setup.sql. See ADR-010 and ADR-012.

CREATE TABLE IF NOT EXISTS retail_dev.curated.customers
USING DELTA
LOCATION 'abfss://curated@retailpipelinedevx7k.dfs.core.windows.net/customers';

CREATE TABLE IF NOT EXISTS retail_dev.curated.order_items
USING DELTA
LOCATION 'abfss://curated@retailpipelinedevx7k.dfs.core.windows.net/order_items';

CREATE TABLE IF NOT EXISTS retail_dev.curated.payments
USING DELTA
LOCATION 'abfss://curated@retailpipelinedevx7k.dfs.core.windows.net/payments';

CREATE TABLE IF NOT EXISTS retail_dev.ops.dead_letter
USING DELTA
LOCATION 'abfss://curated@retailpipelinedevx7k.dfs.core.windows.net/_dead_letter';