# Monitor module inputs

variable "project_name" {
  description = "Short prefix used to build monitor resource names."
  type        = string
}

variable "environment" {
  description = "Deployment environment — appended to resource names to separate dev and prod."
  type        = string
}

variable "location" {
  description = "Azure region where monitor resources will be created."
  type        = string
}

variable "resource_group_name" {
  description = "Name of the Azure resource group monitor resources belong to."
  type        = string
}

variable "alert_email" {
  description = "Email address that receives pipeline failure alerts."
  type        = string
}

variable "adf_id" {
  description = "Azure resource ID of ADF: used to connect diagnostic logs to Log Analytics."
  type        = string
}


variable "tags" {
  description = "Tags applied to all monitor resources."
  type        = map(string)
}