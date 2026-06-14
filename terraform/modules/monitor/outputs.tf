# Monitor module outputs

output "log_analytics_workspace_id" {
  description = "ID of the Log Analytics Workspace — used to connect additional resources to monitoring."
  value       = azurerm_log_analytics_workspace.main.id
}

output "log_analytics_workspace_name" {
  description = "Name of the Log Analytics Workspace — shown in the Azure portal."
  value       = azurerm_log_analytics_workspace.main.name
}
