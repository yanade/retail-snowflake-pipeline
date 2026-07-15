# Read current Azure session — needed for tenant_id

data "azurerm_client_config" "current" {}

# Key Vault — managed secrets store
# Secrets are NOT provisioned here — they are bootstrapped separately via Azure CLI
# after terraform apply. See README for bootstrap instructions.

resource "azurerm_key_vault" "main" {
  name                       = "${var.project_name}-${var.environment}-kv"
  location                   = var.location
  resource_group_name        = var.resource_group_name
  tenant_id                  = data.azurerm_client_config.current.tenant_id
  sku_name                   = "standard"
  purge_protection_enabled   = false  # allows destroy/recreate in dev
  soft_delete_retention_days = 7      # minimum retention, keeps dev cycle fast

  tags = var.tags
}
