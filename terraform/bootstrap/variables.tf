variable "region" {
  description = "AWS region"
  type        = string
}

variable "state_bucket_name" {
  description = "S3 bucket name for Terraform remote state (must be globally unique)"
  type        = string
}

