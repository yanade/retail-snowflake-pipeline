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
  description = "Azure region."
  type        = string
}

variable "resource_group_name" {
  description = "Name of the Azure resource group ADF belongs to."
  type        = string
}

variable "storage_account_name" {
  description = "Name of the ADLS Gen2 storage account: used to build the DFS endpoint URL."
  type        = string
}

variable "key_vault_id" {
  description = "Azure resource ID of Key Vault."
  type        = string
}

variable "tags" {
  description = "Tags applied to the ADF instance."
  type        = map(string)
}

# Git integration. Defaults describe the repository this project already
# publishes to: change them if you fork the project to your own account.

variable "github_account_name" {
  description = "GitHub account or organisation that owns the pipeline repository."
  type        = string
  default     = "yanade"
}

variable "github_repository_name" {
  description = "Name of the GitHub repository ADF publishes pipeline JSON to."
  type        = string
  default     = "retail-snowflake-pipeline"
}

variable "github_branch_name" {
  description = "Branch ADF Studio collaborates on and publishes from."
  type        = string
  default     = "main"
}

variable "github_root_folder" {
  description = "Folder inside the repository where ADF stores pipeline, dataset and linked service JSON."
  type        = string
  default     = "/ingestion/adf_pipelines"
}

variable "github_url" {
  description = "GitHub base URL. Change only for GitHub Enterprise."
  type        = string
  default     = "https://github.com"
}
