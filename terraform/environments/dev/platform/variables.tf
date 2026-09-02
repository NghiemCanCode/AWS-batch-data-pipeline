variable "region" {
  description = "Main region of AWS resources"
  type        = string
}

variable "data_lake_bucket_name" {
  description = "Data lake bucket name (without -dev suffix)"
  type        = string
}

variable "code_bucket_name" {
  description = "Code/script bucket name (without -dev suffix)"
  type        = string
}

variable "ecr_registry_name" {
  description = "ECR repository name"
  type        = string
}

variable "github_repo" {
  description = "GitHub repository in owner/repo format, e.g. NghiemCanCode/AWS-batch-data-pipeline"
  type        = string
}

variable "code_bucket_noncurrent_version_days" {
  description = "Delete noncurrent versions in the code bucket after N days. Keep it long enough to roll back a requirements.txt pinned by MWAA"
  type        = number
  default     = 90
}
