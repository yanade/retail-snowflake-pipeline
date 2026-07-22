-- Watermark Control Tables
-- Database: watermark-db (Azure SQL, francecentral)
-- Run once after terraform apply and secret bootstrap.

-- LIMITATION: This pipeline uses InvoiceDate as the watermark column because
-- the UCI dataset is historical CSV data with no ingestion timestamp.
-- In a production system, a server-assigned created_at column would be used
-- to capture late-arriving records reliably.

-- ── Table 1: pipeline_watermark_control ──────────────────────
-- Stores the last successfully processed timestamp for each pipeline.
-- ADF reads this at the start of every run to know where to continue from.

CREATE TABLE pipeline_watermark_control (
    pipeline_name   NVARCHAR(100)  NOT NULL,
    last_watermark  DATETIME2      NOT NULL,
    rows_loaded     INT            NULL,
    updated_at      DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),

    CONSTRAINT pk_pipeline_watermark_control PRIMARY KEY (pipeline_name)
);
GO

-- ── Table 2: pipeline_config ──────────────────────────────────
-- Stores configuration for each pipeline, managed manually by the ops team.
-- ADF reads this at the start of every run alongside the watermark.

CREATE TABLE pipeline_config (
    pipeline_name       NVARCHAR(100)  NOT NULL,
    -- Source type drives ADF pipeline branching — different connectors for different sources.
    source_type         NVARCHAR(50)   NOT NULL,  -- CSV | SQL | PostgreSQL | API
    -- Source column used as the watermark — allows different pipelines to use different columns.
    watermark_column    NVARCHAR(100)  NOT NULL,
    lookback_days       INT            NOT NULL,
    window_size_hours   INT            NOT NULL DEFAULT 24,
    is_active           BIT            NOT NULL DEFAULT 1,
    updated_at          DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),

    CONSTRAINT pk_pipeline_config PRIMARY KEY (pipeline_name)
);
GO

-- ── Stored Procedure ─────────────────────────────────────────
-- Called by ADF after each successful copy to advance the watermark.
-- Advances to @window_end rather than MAX(InvoiceDate) so sparse windows
-- do not cause the next run to overlap incorrectly.
--
-- COUPLING: Advancing to @window_end is only safe because the ADF source
-- query applies a lookback window (pipeline_config.lookback_days) on every run.
-- If the lookback window is removed, late-arriving records in already-processed
-- windows will be permanently skipped.
-- These two design decisions must always be changed together.

CREATE PROCEDURE usp_update_watermark
    @pipeline_name  NVARCHAR(100),
    @window_end     DATETIME2,
    @rows_loaded    INT
AS
BEGIN
    SET NOCOUNT ON;

    IF EXISTS (SELECT 1 FROM pipeline_watermark_control WHERE pipeline_name = @pipeline_name)
        UPDATE pipeline_watermark_control
        SET    last_watermark = @window_end,
               rows_loaded    = @rows_loaded,
               updated_at     = SYSUTCDATETIME()
        WHERE  pipeline_name  = @pipeline_name;
    ELSE
        INSERT INTO pipeline_watermark_control (pipeline_name, last_watermark, rows_loaded, updated_at)
        VALUES (@pipeline_name, @window_end, @rows_loaded, SYSUTCDATETIME());
END;
GO

-- ── Seed Data: State ──────────────────────────────────────────
-- Sets the starting watermark to one second before the first UCI invoice.
-- The first pipeline run loads all records with InvoiceDate > this value.

INSERT INTO pipeline_watermark_control (pipeline_name, last_watermark, rows_loaded, updated_at)
VALUES
    ('uci_retail_sales', '2010-11-30 23:59:59', NULL, SYSUTCDATETIME()),
    ('fx_rates',         '2010-11-30 23:59:59', NULL, SYSUTCDATETIME());
GO

-- ── Seed Data: Config ─────────────────────────────────────────
-- Sets lookback_days = 0 for fx_rates because FX rates do not arrive late.
-- customers row is inactive (is_active = 0) — shows the framework supports
-- future database sources without any schema changes.

INSERT INTO pipeline_config (pipeline_name, source_type, watermark_column, lookback_days, window_size_hours, is_active, updated_at)
VALUES
    ('uci_retail_sales', 'CSV',        'InvoiceDate', 3, 24, 1, SYSUTCDATETIME()),
    ('fx_rates',         'API',        'Date',        0, 24, 1, SYSUTCDATETIME()),
    ('customers',        'PostgreSQL', 'updated_at',  1, 24, 0, SYSUTCDATETIME());
GO
