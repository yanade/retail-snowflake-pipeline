# SQL module outputs

output "sql_server_fqdn" {
  description = "Fully qualified domain name of the SQL Server — used by ADF to connect."
  value       = azurerm_mssql_server.main.fully_qualified_domain_name
}

output "sql_database_name" {
  description = "Name of the watermark control database."
  value       = azurerm_mssql_database.watermark.name
}

output "connection_string" {
  description = "JDBC connection string for ADF Linked Service."
  value       = "Server=tcp:${azurerm_mssql_server.main.fully_qualified_domain_name},1433;Database=${azurerm_mssql_database.watermark.name};User ID=${var.sql_admin_username};Password=${var.sql_admin_password};Encrypt=true;TrustServerCertificate=false;"
  sensitive   = true
}
