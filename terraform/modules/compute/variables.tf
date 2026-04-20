variable "application_name" {
  description = "Name of emr serverless application"
  type        = string
}

variable "release_label" {
  description = "Release label for emr serverless application"
  type        = string
}

variable "application_type" {
  description = "Type of emr serverless application"
  type        = string
}

variable "s3_artifacts_bucket" {
  description = "S3 bucket for EMR Serverless artifacts"
  type        = string
}
