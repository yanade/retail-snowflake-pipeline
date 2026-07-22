#!/bin/bash
# Assigns Storage Blob Data Contributor role to the current Azure user.
# Run once after terraform apply — only needed if you get AuthorizationPermissionMismatch.
#
# Prerequisites:
#   az login

set -e

STORAGE_ACCOUNT=$(cd terraform && terraform output -raw storage_account_name)
RESOURCE_GROUP=$(cd terraform && terraform output -raw resource_group_name)

STORAGE_ID=$(az storage account show \
  --name "$STORAGE_ACCOUNT" \
  --resource-group "$RESOURCE_GROUP" \
  --query id -o tsv)

MY_ID=$(az ad signed-in-user show --query id -o tsv)

EXISTS=$(az role assignment list \
  --assignee "$MY_ID" \
  --scope "$STORAGE_ID" \
  --query "[?roleDefinitionName=='Storage Blob Data Contributor'] | length(@)" \
  -o tsv)

if [ "$EXISTS" -eq 0 ]; then
    echo "Assigning Storage Blob Data Contributor to $MY_ID..."
    az role assignment create \
      --assignee "$MY_ID" \
      --role "Storage Blob Data Contributor" \
      --scope "$STORAGE_ID"
    echo "Done. Wait ~2 minutes for the role to propagate, then retry the upload."
else
    echo "Role already assigned. Nothing to do."
fi
