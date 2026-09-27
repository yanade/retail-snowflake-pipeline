-- Served zone in ADLS. Run after the Azure role assignment has propagated.

-- Named format, so COPY INTO and ad hoc queries read Parquet the same way.
CREATE FILE FORMAT IF NOT EXISTS ecommerce_db.raw.parquet_format
    TYPE = PARQUET;

-- A stage is location + identity + format under one name.
CREATE STAGE IF NOT EXISTS ecommerce_db.raw.served_stage
    STORAGE_INTEGRATION = azure_adls_served
    URL = 'azure://<% adls_account %>.blob.core.windows.net/<% served_container %>/'
    FILE_FORMAT = ecommerce_db.raw.parquet_format;

-- CREATE skips an existing stage, so the settings are restated.
ALTER STAGE ecommerce_db.raw.served_stage SET
    STORAGE_INTEGRATION = azure_adls_served
    URL = 'azure://<% adls_account %>.blob.core.windows.net/<% served_container %>/'
    FILE_FORMAT = ecommerce_db.raw.parquet_format;

-- retail_dev did not create these objects.
GRANT USAGE ON STAGE ecommerce_db.raw.served_stage TO ROLE retail_dev;
GRANT USAGE ON FILE FORMAT ecommerce_db.raw.parquet_format TO ROLE retail_dev;