# ── ADLS module inputs ────────────────────────────────────────────────────────

variable "project_name" {
  description = "Short prefix used to build the storage account name."
  type        = string
}

variable "environment" {
  description = "Deployment environment — appended to resource names to separate dev and prod."
  type        = string
}

variable "location" {
  description = "Azure region where the storage account will be created."
  type        = string
}

variable "resource_group_name" {
  description = "Name of the Azure resource group this storage account belongs to."
  type        = string
}

variable "tags" {
  description = "Tags applied to the storage account and all containers."
  type        = map(string)
}
