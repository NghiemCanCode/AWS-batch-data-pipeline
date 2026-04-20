variable "account_id" {
  description = "AWS account id"
  type        = string
}

variable "region" {
  description = "Main region of AWS resources"
  type        = string
}

variable "data_lake_bucket_name" {
  description = "Data lake bucket name"
  type        = string
}

variable "code_bucket_name" {
  description = "Code/script bucket name"
  type        = string
}

variable "emr_serverless_application_name" {
  description = "Name of emr serverless application"
  type        = string
}

variable "emr_serverless_release_label" {
  description = "Release label for emr serverless application"
  type        = string
}

variable "emr_serverless_application_type" {
  description = "Type of emr serverless application"
  type        = string
}
