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

variable "storage_account_name" {
  description = "Name of the ADLS storage account Databricks will mount and read from."
  type        = string
}

variable "sku" {
  description = "Databricks pricing tier. Use 'standard' for dev, 'premium' for production."
  type        = string
  default     = "standard"
}

variable "tags" {
  description = "Tags applied to the Databricks workspace."
  type        = map(string)
}