-- Snowflake's identity in Azure. Values come from .env via snow sql -D.

CREATE STORAGE INTEGRATION IF NOT EXISTS azure_adls_served
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'AZURE'
    ENABLED = TRUE
    AZURE_TENANT_ID = '<% tenant_id %>'
    STORAGE_ALLOWED_LOCATIONS = (
        'azure://<% adls_account %>.blob.core.windows.net/<% served_container %>/'
    );

-- CREATE skips an existing integration, so the settings are restated.
ALTER STORAGE INTEGRATION azure_adls_served SET
    AZURE_TENANT_ID = '<% tenant_id %>'
    ENABLED = TRUE
    STORAGE_ALLOWED_LOCATIONS = (
        'azure://<% adls_account %>.blob.core.windows.net/<% served_container %>/'
    );

GRANT USAGE ON INTEGRATION azure_adls_served TO ROLE retail_dev;