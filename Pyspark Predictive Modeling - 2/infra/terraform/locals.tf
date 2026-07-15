locals {
  common_tags = merge(var.tags, {
    environment = "prod"
    project     = "pyspark-predictive-modeling"
    managed_by  = "terraform"
  })
}
