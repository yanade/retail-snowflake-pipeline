# Resource group

output "resource_group_name" {
  description = "Name of the Azure resource group for Azure CLI commands and portal navigation."
  value       = azurerm_resource_group.main.name
}

# ADLS Gen2

output "storage_account_name" {
  description = "Name of the ADLS storage account: used in Databricks notebooks and ADF pipelines."
  value       = module.adls.storage_account_name
}

output "raw_filesystem_name" {
  description = "Raw zone: unprocessed Parquet files."
  value       = module.adls.raw_filesystem_name
}

output "curated_filesystem_name" {
  description = "Curated zone: cleaned and transformed Parquet files."
  value       = module.adls.curated_filesystem_name
}

output "served_filesystem_name" {
  description = "Served zone: Snowflake-ready Parquet, partitioned by date."
  value       = module.adls.served_filesystem_name
}

# Azure Data Factory

output "adf_name" {
  description = "Name of the ADF instance: used in Airflow DAGs to trigger pipelines."
  value       = module.adf.data_factory_name
}

# Databricks

output "databricks_workspace_url" {
  description = "URL to open the Databricks workspace directly in the browser."
  value       = module.databricks.workspace_url
}

output "databricks_workspace_name" {
  description = "Name of the Databricks workspace."
  value       = module.databricks.workspace_name
}

# Azure Monitor

output "log_analytics_workspace_name" {
  description = "Name of the Log Analytics Workspace: used for querying pipeline logs."
  value       = module.monitor.log_analytics_workspace_name
}

# Azure SQL

output "sql_server_fqdn" {
  description = "Fully qualified domain name of the SQL Server: used in bootstrap scripts."
  value       = module.sql.sql_server_fqdn
}

