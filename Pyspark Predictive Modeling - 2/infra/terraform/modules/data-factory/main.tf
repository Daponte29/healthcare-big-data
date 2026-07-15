resource "random_string" "suffix" {
  length  = 6
  lower   = true
  upper   = false
  numeric = true
  special = false
}

locals {
  clean_prefix = substr(replace(lower(var.name_prefix), "-", ""), 0, 10)
  factory_name = substr("adf-${local.clean_prefix}-${random_string.suffix.result}", 0, 63)
}

resource "azurerm_data_factory" "this" {
  name                = local.factory_name
  location            = var.location
  resource_group_name = var.resource_group_name

  identity {
    type = "SystemAssigned"
  }

  tags = var.tags
}
