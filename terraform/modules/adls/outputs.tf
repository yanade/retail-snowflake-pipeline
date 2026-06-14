# Storage account

output "storage_account_name" {
  description = "Name of the storage account, used by ADF and Databricks to connect to ADLS."
  value       = azurerm_storage_account.main.name
}

output "storage_account_id" {
  description = "Azure resource ID of the storage account, used for role assignments."
  value       = azurerm_storage_account.main.id
}

# Data lake zones

output "raw_filesystem_name" {
  description = "Raw zone."
  value       = azurerm_storage_data_lake_gen2_filesystem.raw.name
}

output "curated_filesystem_name" {
  description = "Curated zone, cleaned and validated Parquet."
  value       = azurerm_storage_data_lake_gen2_filesystem.curated.name
}

output "served_filesystem_name" {
  description = "Served zone, Snowflake-ready Parquet, partitioned by date."
  value       = azurerm_storage_data_lake_gen2_filesystem.served.name
}