variable "name_prefix" {
  description = "Short name prefix used for the data factory."
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
  description = "Tags applied to the data factory."
  type        = map(string)
  default     = {}
}
