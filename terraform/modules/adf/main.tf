# Data Factory instance

resource "azurerm_data_factory" "main" {
  name                = "${var.project_name}-${var.environment}-adf"
  location            = var.location
  resource_group_name = var.resource_group_name

  # SystemAssigned identity: Azure creates this automatically.
  # The principal_id is passed to Key Vault to grant ADF permission to read secrets.
  identity {
    type = "SystemAssigned"
  }

  tags = var.tags
}

# Linked service: connects ADF to ADLS Gen2 using Managed Identity

resource "azurerm_data_factory_linked_service_data_lake_storage_gen2" "adls" {
  name                 = "ls_adls_${var.environment}"
  data_factory_id      = azurerm_data_factory.main.id
  url                  = "https://${var.storage_account_name}.dfs.core.windows.net"
  use_managed_identity = true
}

# Linked service: connects ADF to Key Vault for secret retrieval at runtime

resource "azurerm_data_factory_linked_service_key_vault" "keyvault" {
  name            = "ls_key_vault"
  data_factory_id = azurerm_data_factory.main.id
  key_vault_id    = var.key_vault_id
}
