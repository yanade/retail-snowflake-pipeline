# ADF module inputs

variable "project_name" {
  description = "Short prefix used to build the ADF instance name."
  type        = string
}

variable "environment" {
  description = "Deployment environment — appended to resource names to separate dev and prod."
  type        = string
}

variable "location" {
  description = "Azure region where ADF will be created."
  type        = string
}

variable "resource_group_name" {
  description = "Name of the Azure resource group ADF belongs to."
  type        = string
}

variable "storage_account_connection_string" {
  description = "Primary connection string of the ADLS storage account."
  type        = string
  sensitive   = true
}

variable "key_vault_id" {
  description = "Azure resource ID of Key Vault — used to create the Key Vault Linked Service in ADF."
  type        = string
}

variable "tags" {
  description = "Tags applied to the ADF instance."
  type        = map(string)
}
