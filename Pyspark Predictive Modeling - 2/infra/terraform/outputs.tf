output "resource_group_name" {
  value = module.resource_group.name
}

output "storage_account_name" {
  value = module.storage.storage_account_name
}

output "data_factory_name" {
  value = module.data_factory.name
}

output "key_vault_name" {
  value = module.key_vault.name
}

output "log_analytics_workspace_name" {
  value = module.monitoring.workspace_name
}
