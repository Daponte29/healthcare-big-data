module "resource_group" {
  source = "./modules/resource-group"

  resource_group_name = var.resource_group_name
  location            = var.location
  tags                = local.common_tags
}

module "monitoring" {
  source = "./modules/monitoring"

  name_prefix         = var.name_prefix
  resource_group_name = module.resource_group.name
  location            = module.resource_group.location
  tags                = local.common_tags
}

module "key_vault" {
  source = "./modules/key-vault"

  name_prefix         = var.name_prefix
  resource_group_name = module.resource_group.name
  location            = module.resource_group.location
  tags                = local.common_tags
}

module "data_factory" {
  source = "./modules/data-factory"

  name_prefix         = var.name_prefix
  resource_group_name = module.resource_group.name
  location            = module.resource_group.location
  tags                = local.common_tags
}

module "storage" {
  source = "./modules/storage"

  name_prefix               = var.name_prefix
  resource_group_name       = module.resource_group.name
  location                  = module.resource_group.location
  tags                      = local.common_tags
  data_factory_principal_id = module.data_factory.principal_id
  container_names           = ["raw", "curated", "output"]
}
