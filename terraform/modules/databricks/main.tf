# Databricks workspace

resource "azurerm_databricks_workspace" "main" {
  name                        = "${var.project_name}-${var.environment}-dbx"
  location                    = var.location
  resource_group_name         = var.resource_group_name
  sku                         = var.sku
  # Databricks creates its own internal resource group for VMs and networking
  managed_resource_group_name = "${var.project_name}-${var.environment}-dbx-managed"

  tags = var.tags
}

# Access Connector: purpose-built identity for Databricks to access external storage

resource "azurerm_databricks_access_connector" "adls" {
  name                = "${var.project_name}-${var.environment}-dbx-connector"
  resource_group_name = var.resource_group_name
  location            = var.location

  identity {
    type = "SystemAssigned"  # Azure creates and manages this identity automatically
  }

  tags = var.tags
}

# Grant the Access Connector identity permission to read and write ADLS

resource "azurerm_role_assignment" "databricks_adls" {
  scope                = var.storage_account_id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.adls.identity[0].principal_id
}
