# ── Storage account ───────────────────────────────────────────────────────────

resource "azurerm_storage_account" "main" {
  # Azure requires: lowercase, no hyphens, max 24 characters, globally unique
  name                = lower(replace("${var.project_name}${var.environment}", "-", ""))
  resource_group_name = var.resource_group_name
  location            = var.location
  account_tier        = "Standard"
  account_replication_type = "LRS"   # 3 copies within one datacenter
  account_kind        = "StorageV2"  # required for ADLS Gen2
  is_hns_enabled      = true         # enables hierarchical namespace — this is what makes it ADLS Gen2

  tags = var.tags
}

# ── Data lake zones ───────────────────────────────────────────────────────────

resource "azurerm_storage_data_lake_gen2_filesystem" "raw" {
  name               = "raw"
  storage_account_id = azurerm_storage_account.main.id  # depends on storage account existing first
}

resource "azurerm_storage_data_lake_gen2_filesystem" "curated" {
  name               = "curated"
  storage_account_id = azurerm_storage_account.main.id
}

resource "azurerm_storage_data_lake_gen2_filesystem" "served" {
  name               = "served"
  storage_account_id = azurerm_storage_account.main.id
}