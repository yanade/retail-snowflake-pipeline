# Remote state backend — stores terraform.tfstate in Azure Storage
#
# IMPORTANT: the storage account below must be created ONCE manually before
# running terraform init.

# Run these Azure CLI commands with your Azure account:
#
#   az group create \
#     --name retail-pipeline-tfstate-rg \
#     --location uksouth
#
#   az storage account create \
#     --name retailpipelinetfstatex7k \
#     --resource-group retail-pipeline-tfstate-rg \
#     --location uksouth \
#     --sku Standard_LRS
#
#   az storage container create \
#     --name tfstate \
#     --account-name retailpipelinetfstatex7k

terraform {
  backend "azurerm" {
    resource_group_name  = "retail-pipeline-tfstate-rg" # resource group holding the state storage
    storage_account_name = "retailpipelinetfstatex7k"      # globally unique, no hyphens
    container_name       = "tfstate"                    # blob container inside the storage account
    key                  = "retail-pipeline.tfstate"    # filename of the state file
  }
}
