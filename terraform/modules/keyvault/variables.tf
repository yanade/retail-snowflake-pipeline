# Key Vault module inputs

variable "project_name" {
  description = "Short prefix used to build Key Vault resource names."
  type        = string
}

variable "environment" {
  description = "Deployment environment — appended to resource names to separate dev and prod."
  type        = string
}

variable "location" {
  description = "Azure region where Key Vault will be created."
  type        = string
}

variable "resource_group_name" {
  description = "Name of the Azure resource group Key Vault belongs to."
  type        = string
}

variable "tags" {
  description = "Tags applied to all Key Vault resources."
  type        = map(string)
}
