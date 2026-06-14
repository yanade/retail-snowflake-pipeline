# Databricks module outputs

output "workspace_name" {
  description = "Name of the Databricks workspace — used in Airflow DAGs to trigger PySpark jobs."
  value       = azurerm_databricks_workspace.main.name
}

output "workspace_url" {
  description = "URL to open the Databricks workspace in the browser."
  value       = azurerm_databricks_workspace.main.workspace_url
}

output "workspace_id" {
  description = "Azure resource ID of the Databricks workspace — used for role assignments."
  value       = azurerm_databricks_workspace.main.id
}
