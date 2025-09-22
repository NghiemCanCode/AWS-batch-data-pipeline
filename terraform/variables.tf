variable "region" {
  description = "Region for AWS resource"
  default = "ap-southeast-1"
  type = string
}

locals {
  data_lake_bucket = "spotify"
}

