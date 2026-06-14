# ── Project identity ──────────────────────────────────────────────────────────

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

# ── Snowflake connection ──────────────────────────────────────────────────────

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

# ── Tags ─────────────────────────────────────────────────────────────────────

variable "tags" {
  description = "Tags applied to every Azure resource. Used for cost tracking and filtering in the portal."
  type        = map(string)
  default = {
    project     = "retail-snowflake-pipeline"
    environment = "dev"
    managed_by  = "terraform"
  }
}
