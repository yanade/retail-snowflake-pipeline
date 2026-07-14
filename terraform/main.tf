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

# Azure Data Factory: ingestion orchestrator

module "adf" {
  source                           = "./modules/adf"
  project_name                     = var.project_name
  environment                      = var.environment
  location                         = var.location
  resource_group_name              = azurerm_resource_group.main.name
  storage_account_connection_string = module.adls.storage_account_primary_connection_string
  tags                             = var.tags
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
