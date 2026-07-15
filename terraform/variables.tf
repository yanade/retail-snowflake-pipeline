# Project identity

variable "project_name" {
  description = "Short prefix applied to every resource name."
  type        = string
  default     = "retail-pipeline"
}

variable "environment" {
  description = "Deployment environment. Controls naming and can be used to toggle resource sizes."
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "prod"], var.environment)
    error_message = "environment must be 'dev' or 'prod'."
  }
}

variable "location" {
  description = "Azure region where all resources will be deployed."
  type        = string
  default     = "uksouth"
}

variable "sql_location" {
  description = "Azure region for SQL Server. Differs from main location due to free trial subscription restriction in uksouth. In production all services would be co-located."
  type        = string
  default     = "francecentral"
}

# Snowflake connection

variable "snowflake_account" {
  description = "Snowflake account identifier."
  type        = string
}

variable "snowflake_user" {
  description = "Snowflake username ADF will use to load data."
  type        = string
}

variable "snowflake_password" {
  description = "Snowflake password. Marked sensitive: never printed in logs or terminal output."
  type        = string
  sensitive   = true
}

# SQL Database

variable "sql_admin_password" {
  description = "Administrator password for Azure SQL Server — passed to Key Vault at bootstrap time. Never stored in state after secrets are migrated to CLI-managed bootstrap."
  type        = string
  sensitive   = true
}

# Alerts

variable "alert_email" {
  description = "Email address that receives pipeline failure alerts."
  type        = string
}


# Tags

variable "tags" {
  description = "Tags applied to every Azure resource. Used for cost tracking and filtering in the portal."
  type        = map(string)
  default = {
    project     = "retail-snowflake-pipeline"
    environment = "dev"
    managed_by  = "terraform"
  }
}
