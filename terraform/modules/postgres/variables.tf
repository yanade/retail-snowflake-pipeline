# PostgreSQL module inputs

variable "project_name" {
  description = "Short prefix used to build PostgreSQL resource names."
  type        = string
}

variable "environment" {
  description = "Deployment environment — appended to resource names to separate dev and prod."
  type        = string
}

variable "location" {
  description = "Azure region where PostgreSQL resources will be created."
  type        = string
}

variable "resource_group_name" {
  description = "Name of the Azure resource group PostgreSQL resources belong to."
  type        = string
}

variable "postgres_admin_username" {
  description = "Administrator login for the PostgreSQL Flexible Server."
  type        = string
  default     = "pgadmin"
}

variable "postgres_admin_password" {
  description = "Administrator password for the PostgreSQL Flexible Server. Must be 8+ chars with upper, lower, number, and symbol."
  type        = string
  sensitive   = true
}

variable "tags" {
  description = "Tags applied to all PostgreSQL resources."
  type        = map(string)
}