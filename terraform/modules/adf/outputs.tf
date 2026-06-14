# ADF module outputs

output "data_factory_name" {
  description = "Name of the ADF instance — used in Airflow DAGs and GitHub Actions to reference pipelines."
  value       = azurerm_data_factory.main.name
}

output "data_factory_id" {
  description = "Azure resource ID of ADF — used for role assignments to grant ADF access to ADLS."
  value       = azurerm_data_factory.main.id
}