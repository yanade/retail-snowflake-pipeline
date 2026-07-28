# PostgreSQL module outputs

output "postgres_server_fqdn" {
  description = "Fully qualified domain name of the PostgreSQL Flexible Server — used by ADF and local scripts to connect."
  value       = azurerm_postgresql_flexible_server.main.fqdn
}

output "postgres_database_name" {
  description = "Name of the retail_source database."
  value       = azurerm_postgresql_flexible_server_database.retail_source.name
}

output "postgres_admin_username" {
  description = "Administrator username — combine with the FQDN and a separately-managed password to build DATABASE_URL."
  value       = var.postgres_admin_username
}