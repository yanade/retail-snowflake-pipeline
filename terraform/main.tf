# Terraform configuration and Azure provider

terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
  }
}

provider "azurerm" {
  features {}
}

# Read current Azure session — needed for Key Vault access policies

data "azurerm_client_config" "current" {}

# ============================================================
# Phase 1 — Infrastructure (no cross-module dependencies)
# ============================================================

# Resource group: shared container for all resources

resource "azurerm_resource_group" "main" {
  name     = "${var.project_name}-${var.environment}-rg"
  location = var.location
  tags     = var.tags
}

# ADLS Gen2: data lake storage with raw/curated/served zones

module "adls" {
  source              = "./modules/adls"
  project_name        = var.project_name
  environment         = var.environment
  location            = var.location
  resource_group_name = azurerm_resource_group.main.name
  tags                = var.tags
}

# Azure SQL Database — watermark control store

module "sql" {
  source              = "./modules/sql"
  project_name        = var.project_name
  environment         = var.environment
  location            = var.sql_location  # francecentral — uksouth blocked on free trial
  resource_group_name = azurerm_resource_group.main.name
  sql_admin_password  = var.sql_admin_password
  tags                = var.tags
}

# Azure Key Vault — secrets management (vault only, no secrets)
# Secrets are bootstrapped after apply via Azure CLI — see README

module "keyvault" {
  source              = "./modules/keyvault"
  project_name        = var.project_name
  environment         = var.environment
  location            = var.location
  resource_group_name = azurerm_resource_group.main.name
  tags                = var.tags
}

# Azure Data Factory — ingestion orchestrator

module "adf" {
  source                            = "./modules/adf"
  project_name                      = var.project_name
  environment                       = var.environment
  location                          = var.location
  resource_group_name               = azurerm_resource_group.main.name
  storage_account_connection_string = module.adls.storage_account_primary_connection_string
  key_vault_id                      = module.keyvault.key_vault_id
  tags                              = var.tags
}

# Databricks — PySpark transformation workspace

module "databricks" {
  source              = "./modules/databricks"
  project_name        = var.project_name
  environment         = var.environment
  location            = var.location
  resource_group_name = azurerm_resource_group.main.name
  storage_account_id  = module.adls.storage_account_id
  tags                = var.tags
}

# ============================================================
# Phase 2 — Wiring (cross-module, depends on Phase 1)
# ============================================================

# Grant Terraform CLI permission to manage Key Vault secrets (bootstrap step)

resource "azurerm_key_vault_access_policy" "terraform" {
  key_vault_id = module.keyvault.key_vault_id
  tenant_id    = data.azurerm_client_config.current.tenant_id
  object_id    = data.azurerm_client_config.current.object_id

  secret_permissions = ["Get", "Set", "Delete", "Purge", "List"]
}

# Grant ADF Managed Identity permission to read secrets from Key Vault

resource "azurerm_key_vault_access_policy" "adf" {
  key_vault_id = module.keyvault.key_vault_id
  tenant_id    = data.azurerm_client_config.current.tenant_id
  object_id    = module.adf.data_factory_principal_id

  secret_permissions = ["Get"]
}

# Azure Monitor — Log Analytics and pipeline failure alerts

module "monitor" {
  source              = "./modules/monitor"
  project_name        = var.project_name
  environment         = var.environment
  location            = var.location
  resource_group_name = azurerm_resource_group.main.name
  alert_email         = var.alert_email
  adf_id              = module.adf.data_factory_id
  tags                = var.tags
}
