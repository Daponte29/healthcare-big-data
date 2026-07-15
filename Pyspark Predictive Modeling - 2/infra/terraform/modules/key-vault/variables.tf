variable "name_prefix" {
  description = "Short name prefix used for the key vault."
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
  description = "Tags applied to the key vault."
  type        = map(string)
  default     = {}
}
