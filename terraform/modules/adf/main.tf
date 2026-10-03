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

  # Git integration was first set up through the ADF Studio UI. It is declared
  # here so Terraform stops treating it as drift and proposing its removal.
  # Publishing in ADF Studio commits the pipeline JSON back to this repo
  # under root_folder.
  github_configuration {
    account_name       = var.github_account_name
    branch_name        = var.github_branch_name
    git_url            = var.github_url
    repository_name    = var.github_repository_name
    root_folder        = var.github_root_folder
    publishing_enabled = true
  }

  tags = var.tags
}

# Linked services, datasets and pipelines are owned by ADF Git (ADR-023).
# These blocks only drop the two Terraform used to create from state; destroy = false keeps them in Azure.

removed {
  from = azurerm_data_factory_linked_service_data_lake_storage_gen2.adls

  lifecycle {
    destroy = false
  }
}

removed {
  from = azurerm_data_factory_linked_service_key_vault.keyvault

  lifecycle {
    destroy = false
  }
}
