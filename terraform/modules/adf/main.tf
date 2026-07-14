# Data Factory instance

resource "azurerm_data_factory" "main" {
  name                = "${var.project_name}-${var.environment}-adf"
  location            = var.location
  resource_group_name = var.resource_group_name

  tags = var.tags
}

# Linked service — connects ADF to ADLS Gen2

resource "azurerm_data_factory_linked_service_azure_blob_storage" "adls" {
  name              = "ls_adls_${var.environment}"  # ls_ prefix is ADF naming convention
  data_factory_id   = azurerm_data_factory.main.id
  connection_string = var.storage_account_connection_string
}