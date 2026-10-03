-- Snowflake objects for the retail pipeline.
-- Run once as ACCOUNTADMIN, before anything else exists.
-- None of this needs a running warehouse: DDL and GRANT are metadata only.


-- ── Compute ───────────────────────────────────────────────────────────────
CREATE WAREHOUSE IF NOT EXISTS retail_dev_wh
    WAREHOUSE_SIZE      = 'XSMALL'   -- smallest and cheapest
    AUTO_SUSPEND        = 60         -- SECONDS. The default 600 bills ten idle minutes 
    AUTO_RESUME         = TRUE       -- a query restarts it
    INITIALLY_SUSPENDED = TRUE       -- the default is FALSE: it would bill from creation
    COMMENT             = 'Retail pipeline, dev';

-- CREATE ... IF NOT EXISTS skips an existing warehouse silently, so the
-- settings are restated here. They must hold whether we created it or not.
ALTER WAREHOUSE retail_dev_wh SET
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND   = 60
    AUTO_RESUME    = TRUE
    STATEMENT_TIMEOUT_IN_SECONDS = 600;

-- ── Storage ───────────────────────────────────────────────────────────────
CREATE DATABASE IF NOT EXISTS ecommerce_db
    COMMENT = 'Retail pipeline warehouse';

-- Schema names are qualified with the database on purpose: unqualified ones
-- land in whatever database the session happens to have selected.
CREATE SCHEMA IF NOT EXISTS ecommerce_db.raw
    COMMENT = 'Loaded from ADLS as received, no transformation';

CREATE SCHEMA IF NOT EXISTS ecommerce_db.audit
    COMMENT = 'pipeline_audit: one row per run';

-- staging and marts are deliberately NOT created here: dbt owns them,
-- and two definitions of the same schema drift apart. See dbt_project.yml.

-- ── Access ────────────────────────────────────────────────────────────────
-- One working role. ACCOUNTADMIN is for setup only, never for daily work.
CREATE ROLE IF NOT EXISTS retail_dev
    COMMENT = 'Pipeline and dbt role, dev environment';

GRANT USAGE ON WAREHOUSE retail_dev_wh TO ROLE retail_dev;
GRANT USAGE ON DATABASE ecommerce_db TO ROLE retail_dev;

-- dbt creates its own schemas, so the role needs the right to do it
GRANT CREATE SCHEMA ON DATABASE ecommerce_db TO ROLE retail_dev;

GRANT ALL PRIVILEGES ON SCHEMA ecommerce_db.raw   TO ROLE retail_dev;
GRANT ALL PRIVILEGES ON SCHEMA ecommerce_db.audit TO ROLE retail_dev;

GRANT ROLE retail_dev TO USER <% snowflake_user %>;

-- ADR-020: timestamps are UTC, so the pipeline user's sessions must be too
ALTER USER <% snowflake_user %> SET TIMEZONE = 'UTC';

-- ── Cost guard ────────────────────────────────────────────────────────────

CREATE RESOURCE MONITOR IF NOT EXISTS retail_dev_monitor
    WITH CREDIT_QUOTA = 50
    FREQUENCY = MONTHLY
    START_TIMESTAMP = IMMEDIATELY
    NOTIFY_USERS = ( <% snowflake_user %> )
    TRIGGERS
        ON 75  PERCENT DO NOTIFY
        ON 100 PERCENT DO SUSPEND
        ON 110 PERCENT DO SUSPEND_IMMEDIATE;


ALTER ACCOUNT SET RESOURCE_MONITOR = retail_dev_monitor;