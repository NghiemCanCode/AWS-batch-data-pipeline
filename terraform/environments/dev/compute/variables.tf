variable "account_id" {
  description = "AWS account id"
  type        = string
}

variable "region" {
  description = "Main region of AWS resources"
  type        = string
}

variable "emr_app_name" {
  description = "Name of EMR Serverless application (without -dev suffix)"
  type        = string
}

variable "emr_release_label" {
  description = "Release label for EMR Serverless application"
  type        = string
}

variable "emr_app_type" {
  description = "Type of EMR Serverless application"
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
