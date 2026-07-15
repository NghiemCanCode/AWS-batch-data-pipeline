variable "application_name" {
  description = "Name of emr serverless application"
  type        = string
}

variable "release_label" {
  description = "Release label for emr serverless application"
  type        = string
}

variable "s3_artifacts_bucket" {
  description = "S3 bucket for EMR Serverless artifacts"
  type        = string
}

variable "custom_image_uri" {
  description = "Custom Docker image URI from ECR for EMR Serverless (Python 3.12)"
  type        = string
  default     = ""
}

variable "is_endpoint" {
  description = ""  
  type = string
  default = "false"
}

variable "is_studio_enabled" {
  description = ""  
  type = string
  default = "false"
}
