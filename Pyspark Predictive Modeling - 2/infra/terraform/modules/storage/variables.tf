variable "name_prefix" {
  description = "Short name prefix used for the storage account."
  type        = string
}

variable "resource_group_name" {
  description = "Resource group name."
  type        = string
}

variable "location" {
  description = "Azure location."
  type        = string
}

variable "tags" {
  description = "Tags applied to storage resources."
  type        = map(string)
  default     = {}
}

variable "container_names" {
  description = "Containers created in the storage account."
  type        = list(string)
  default     = ["raw", "curated", "output"]
}

variable "data_factory_principal_id" {
  description = "Managed identity principal ID for the Data Factory."
  type        = string
  default     = null
}
