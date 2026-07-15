resource "random_string" "suffix" {
  length  = 6
  lower   = true
  upper   = false
  numeric = true
  special = false
}

locals {
  clean_prefix   = substr(replace(lower(var.name_prefix), "-", ""), 0, 10)
  workspace_name = substr("law-${local.clean_prefix}-${random_string.suffix.result}", 0, 63)
}

resource "azurerm_log_analytics_workspace" "this" {
  name                = local.workspace_name
  location            = var.location
  resource_group_name = var.resource_group_name
  retention_in_days   = 30

  tags = var.tags
}
