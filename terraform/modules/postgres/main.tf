# PostgreSQL Flexible Server: the container that hosts the database

resource "azurerm_postgresql_flexible_server" "main" {
  name                   = "${var.project_name}-${var.environment}-pg"
  resource_group_name    = var.resource_group_name
  location               = var.location
  version                = "16"
  administrator_login    = var.postgres_admin_username
  administrator_password = var.postgres_admin_password
  sku_name               = "B_Standard_B1ms" # Burstable, cheapest tier
  storage_mb             = 32768             # 32GB, minimum allowed
  zone                   = "1"

  tags = var.tags
}

# Firewall rule: allows Azure services (including ADF) to connect
# Start/end IP of 0.0.0.0 is the Azure convention for "allow Azure-internal traffic"

resource "azurerm_postgresql_flexible_server_firewall_rule" "allow_azure_services" {
  name             = "allow-azure-services"
  server_id        = azurerm_postgresql_flexible_server.main.id
  start_ip_address = "0.0.0.0"
  end_ip_address   = "0.0.0.0"
}

# Database: same name as the local Docker instance so seed.py/generate_data.py
# work unchanged against either — only DATABASE_URL differs.

resource "azurerm_postgresql_flexible_server_database" "retail_source" {
  name      = "retail_source"
  server_id = azurerm_postgresql_flexible_server.main.id
  collation = "en_US.utf8"
  charset   = "UTF8"
}