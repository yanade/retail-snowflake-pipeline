# Key Vault module outputs

output "key_vault_id" {
  description = "Azure resource ID of the Key Vault — used to create the ADF Linked Service."
  value       = azurerm_key_vault.main.id
}

output "key_vault_uri" {
  description = "Base URI of the Key Vault — used by ADF to fetch secrets at runtime."
  value       = azurerm_key_vault.main.vault_uri
}
