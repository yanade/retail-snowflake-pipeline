# Databricks module inputs

variable "project_name" {
  description = "Short prefix used to build the Databricks workspace name."
  type        = string
}

variable "environment" {
  description = "Deployment environment: appended to resource names to separate dev and prod."
  type        = string
}

variable "location" {
  description = "Azure region where the Databricks workspace will be created."
  type        = string
}

variable "resource_group_name" {
  description = "Name of the Azure resource group the Databricks workspace belongs to."
  type        = string
}

variable "storage_account_id" {
  description = "Azure resource ID of the ADLS storage account — used for role assignment."
  type        = string
}

variable "sku" {
  description = "Databricks pricing tier. Azure retired the 'standard' SKU, so 'premium' is required for all environments now."
  type        = string
  default     = "premium"
}

variable "tags" {
  description = "Tags applied to the Databricks workspace."
  type        = map(string)
}