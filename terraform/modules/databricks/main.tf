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

# Look up the existing ADLS storage account
data "azurerm_storage_account" "adls" {
  name                = var.storage_account_name
  resource_group_name = var.resource_group_name
}

# Grant Databricks permission to read and write ADLS

resource "azurerm_role_assignment" "databricks_adls" {
  scope                = data.azurerm_storage_account.adls.id
  role_definition_name = "Storage Blob Data Contributor"  # allows read, write, delete on blobs
  principal_id         = azurerm_databricks_workspace.main.storage_account_identity[0].principal_id
}