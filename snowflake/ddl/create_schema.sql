


-- ============================================================
-- 01_create_schema.sql
-- Run this first — creates the database, schemas, and warehouse
-- ============================================================

-- Database

CREATE DATABASE IF NOT EXISTS ecommerce_db;

USE DATABASE ecommerce_db;

-- Schemas

CREATE SCHEMA IF NOT EXISTS raw;        -- data landed from ADLS exactly as received
CREATE SCHEMA IF NOT EXISTS staging;    -- dbt staging models — light cleaning only
CREATE SCHEMA IF NOT EXISTS marts;      -- final star schema — dimensions and fact table
CREATE SCHEMA IF NOT EXISTS audit;      -- pipeline_audit table — every run logged here
CREATE SCHEMA IF NOT EXISTS dead_letter; -- failed records captured here

-- Warehouse (compute engine)

CREATE WAREHOUSE IF NOT EXISTS compute_wh
  WAREHOUSE_SIZE  = 'X-SMALL'   -- cheapest tier, sufficient for dev workloads
  AUTO_SUSPEND    = 60           -- shuts down after 60 seconds idle: saves trial credits
  AUTO_RESUME     = TRUE         -- starts automatically when a query runs
  COMMENT         = 'Main warehouse for retail pipeline dev workloads';

