-- Watermark control tables. Source: PostgreSQL retail_oltp (database/schema.sql).
-- Run once after terraform apply and secret bootstrap.

CREATE TABLE pipeline_watermark_control (
    pipeline_name   NVARCHAR(100)  NOT NULL,
    last_watermark  DATETIME2      NOT NULL,
    rows_loaded     INT            NULL,
    updated_at      DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),

    CONSTRAINT pk_pipeline_watermark_control PRIMARY KEY (pipeline_name)
);
GO

CREATE TABLE pipeline_config (
    pipeline_name       NVARCHAR(100)  NOT NULL,
    source_type         NVARCHAR(50)   NOT NULL,
    watermark_column    NVARCHAR(100)  NOT NULL,
    lookback_days       INT            NOT NULL,
    window_size_hours   INT            NOT NULL DEFAULT 24,
    is_active           BIT            NOT NULL DEFAULT 1,
    updated_at          DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),

    CONSTRAINT pk_pipeline_config PRIMARY KEY (pipeline_name)
);
GO

-- COUPLING: advancing to @window_end is only safe because of the lookback
-- window below. Removing one without the other silently drops late updates.

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

-- Sentinel low-water mark — updated_at is wall-clock time, not a business date.

INSERT INTO pipeline_watermark_control (pipeline_name, last_watermark, rows_loaded, updated_at)
VALUES
    ('customers',          '1900-01-01', NULL, SYSUTCDATETIME()),
    ('customer_addresses', '1900-01-01', NULL, SYSUTCDATETIME()),
    ('product_categories', '1900-01-01', NULL, SYSUTCDATETIME()),
    ('suppliers',          '1900-01-01', NULL, SYSUTCDATETIME()),
    ('products',           '1900-01-01', NULL, SYSUTCDATETIME()),
    ('currencies',         '1900-01-01', NULL, SYSUTCDATETIME()),
    ('stores',             '1900-01-01', NULL, SYSUTCDATETIME()),
    ('employees',          '1900-01-01', NULL, SYSUTCDATETIME()),
    ('orders',             '1900-01-01', NULL, SYSUTCDATETIME()),
    ('order_items',        '1900-01-01', NULL, SYSUTCDATETIME()),
    ('payments',           '1900-01-01', NULL, SYSUTCDATETIME()),
    ('exchange_rates',     '1900-01-01', NULL, SYSUTCDATETIME());
GO

-- Phase 1 slice: customers + products (master data feeding dim_customer/dim_product)
-- plus orders/order_items/payments/exchange_rates (transactional + FX). Enough to
-- build a complete fact_sales + dims end to end without all 12 pipelines up front.
-- Remaining tables seeded but inactive — same pattern scales to them later.
--
-- exchange_rates: lookback_days=0, rates don't arrive late.
-- orders/order_items/payments: lookback_days=3, late-arriving status updates.

INSERT INTO pipeline_config (pipeline_name, source_type, watermark_column, lookback_days, window_size_hours, is_active, updated_at)
VALUES
    ('customers',          'PostgreSQL', 'updated_at', 1, 24, 1, SYSUTCDATETIME()),
    ('customer_addresses', 'PostgreSQL', 'updated_at', 1, 24, 0, SYSUTCDATETIME()),
    ('product_categories', 'PostgreSQL', 'updated_at', 1, 24, 0, SYSUTCDATETIME()),
    ('suppliers',          'PostgreSQL', 'updated_at', 1, 24, 0, SYSUTCDATETIME()),
    ('products',           'PostgreSQL', 'updated_at', 1, 24, 1, SYSUTCDATETIME()),
    ('currencies',         'PostgreSQL', 'updated_at', 1, 24, 0, SYSUTCDATETIME()),
    ('stores',             'PostgreSQL', 'updated_at', 1, 24, 0, SYSUTCDATETIME()),
    ('employees',          'PostgreSQL', 'updated_at', 1, 24, 0, SYSUTCDATETIME()),
    ('orders',             'PostgreSQL', 'updated_at', 3, 24, 1, SYSUTCDATETIME()),
    ('order_items',        'PostgreSQL', 'updated_at', 3, 24, 1, SYSUTCDATETIME()),
    ('payments',           'PostgreSQL', 'updated_at', 3, 24, 1, SYSUTCDATETIME()),
    ('exchange_rates',     'PostgreSQL', 'updated_at', 0, 24, 1, SYSUTCDATETIME());
GO
