# SQL Server: the container that hosts the database

resource "azurerm_mssql_server" "main" {
  name                         = "${var.project_name}-${var.environment}-sql"
  resource_group_name          = var.resource_group_name
  location                     = var.location
  version                      = "12.0"
  administrator_login          = var.sql_admin_username
  administrator_login_password = var.sql_admin_password
  minimum_tls_version          = "1.2"

  tags = var.tags
}

# Firewall rule: allows Azure services (including ADF) to connect
# Start/end IP of 0.0.0.0 is the Azure convention for "allow Azure-internal traffic"

resource "azurerm_mssql_firewall_rule" "allow_azure_services" {
  name             = "allow-azure-services"
  server_id        = azurerm_mssql_server.main.id
  start_ip_address = "0.0.0.0"
  end_ip_address   = "0.0.0.0"
}

# SQL Database: serverless tier: pauses when idle, costs nothing while paused

resource "azurerm_mssql_database" "watermark" {
  name      = "watermark-db"
  server_id = azurerm_mssql_server.main.id
  sku_name  = "GP_S_Gen5_1"   # General Purpose Serverless, Gen5, 1 vCore
  collation = "SQL_Latin1_General_CP1_CI_AS"

  # Serverless-specific settings
  auto_pause_delay_in_minutes = 60
  min_capacity                = 0.5
  max_size_gb                 = 32

  tags = var.tags
}
