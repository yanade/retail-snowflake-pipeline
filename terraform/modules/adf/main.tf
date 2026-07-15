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

# Linked service — connects ADF to ADLS Gen2

resource "azurerm_data_factory_linked_service_azure_blob_storage" "adls" {
  name              = "ls_adls_${var.environment}"
  data_factory_id   = azurerm_data_factory.main.id
  connection_string = var.storage_account_connection_string
}

# Linked service — connects ADF to Key Vault for secret retrieval at runtime

resource "azurerm_data_factory_linked_service_key_vault" "keyvault" {
  name            = "ls_key_vault"
  data_factory_id = azurerm_data_factory.main.id
  key_vault_id    = var.key_vault_id
}
