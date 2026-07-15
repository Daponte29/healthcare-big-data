variable "name_prefix" {
  description = "Short prefix used to build Azure resource names."
  type        = string
  default     = "pyspark-predictive-modeling-2"
}

variable "location" {
  description = "Azure region for the prod stack."
  type        = string
  default     = "eastus"
}

variable "resource_group_name" {
  description = "Name of the resource group that contains the stack."
  type        = string
  default     = "rg-pyspark-predictive-modeling-2-prd-eus"
}

variable "tags" {
  description = "Common tags applied to all resources."
  type        = map(string)
  default     = {}
}
