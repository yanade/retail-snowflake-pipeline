# SQL module inputs

variable "project_name" {
  description = "Short prefix used to build SQL resource names."
  type        = string
}

variable "environment" {
  description = "Deployment environment — appended to resource names to separate dev and prod."
  type        = string
}

variable "location" {
  description = "Azure region where SQL resources will be created."
  type        = string
}

variable "resource_group_name" {
  description = "Name of the Azure resource group SQL resources belong to."
  type        = string
}

variable "sql_admin_username" {
  description = "Administrator login for the SQL Server. Cannot be 'admin', 'sa', or 'root'."
  type        = string
  default     = "sqladmin"
}

variable "sql_admin_password" {
  description = "Administrator password for the SQL Server. Must be 8+ chars with upper, lower, number, and symbol."
  type        = string
  sensitive   = true
}

variable "tags" {
  description = "Tags applied to all SQL resources."
  type        = map(string)
}
