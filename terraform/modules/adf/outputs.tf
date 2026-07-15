# ADF module outputs

output "data_factory_name" {
  description = "Name of the ADF instance — used in Airflow DAGs and GitHub Actions to reference pipelines."
  value       = azurerm_data_factory.main.name
}

output "data_factory_id" {
  description = "Azure resource ID of ADF — used for role assignments and Monitor diagnostic settings."
  value       = azurerm_data_factory.main.id
}

output "data_factory_principal_id" {
  description = "Object ID of ADF's Managed Identity — passed to Key Vault access policy in main.tf."
  value       = azurerm_data_factory.main.identity[0].principal_id
}
