output "storage_account_name" {
  value = azurerm_storage_account.this.name
}

output "storage_account_id" {
  value = azurerm_storage_account.this.id
}

output "primary_dfs_endpoint" {
  value = azurerm_storage_account.this.primary_dfs_endpoint
}

output "container_names" {
  value = [for container in azurerm_storage_container.containers : container.name]
}
