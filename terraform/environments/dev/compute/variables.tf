variable "emr_app_name" {
  description = "Name of EMR Serverless application (without -dev suffix)"
  type        = string
}

variable "emr_release_label" {
  description = "Release label for EMR Serverless application"
  type        = string
}

variable "data_lake_bucket_name" {
  description = "Data lake bucket name"
  type        = string
}

variable "code_bucket_name" {
  description = "Code/script bucket uri"
  type        = string
}

variable "custom_image_uri" {
  description = "Custom Docker image URI from ECR for EMR Serverless"
  type        = string
  default     = ""
}

variable "region" {
  description = "AWS Region"
  type        = string
}

variable "is_endpoint" {
  description = "livy endpoint"
}

variable "is_studio_enabled" {
  description = "enable emr serverless studio"
}

# The job's monitoringConfiguration must name this same log group, so the value
# is exported as an output and read back by the deploy scripts / Airflow DAGs
# instead of being written out a second time by hand.
variable "emr_log_group_name" {
  description = "CloudWatch log group EMR Serverless job runs write to"
  type        = string
  default     = ""
}

locals {
  emr_log_group_name = var.emr_log_group_name != "" ? var.emr_log_group_name : "/aws/emr-serverless/${var.emr_app_name}-dev"
}