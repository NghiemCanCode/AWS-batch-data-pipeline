variable "bucket_name" {
  description = "Name of bucket"
  type = string
}

variable "bucket_tags" {
  description = "Tag for aws resource (useful for environment check)"
  type = map(string)
  default = {}
}

variable "versioning_state" {
  description = "Enable bucket versioning or not"
  type = string
  default = "Disabled"
}

variable "folder_path" {
  description = "Folder path"
  type = list(string)
}
variable "block_public_access" {
  description = "Block all public access on the bucket. Required by MWAA"
  type        = bool
  default     = true
}

variable "sse_algorithm" {
  description = "Server side encryption algorithm: AES256 (SSE-S3) or aws:kms"
  type        = string
  default     = "AES256"

  validation {
    condition     = contains(["AES256", "aws:kms"], var.sse_algorithm)
    error_message = "sse_algorithm must be AES256 or aws:kms."
  }
}

variable "kms_key_arn" {
  description = "KMS key ARN, only used when sse_algorithm = aws:kms"
  type        = string
  default     = null
}

variable "noncurrent_version_expiration_days" {
  description = "Delete noncurrent object versions after N days. 0 disables the lifecycle rule"
  type        = number
  default     = 0
}

variable "managed_objects" {
  description = "Files uploaded and versioned by Terraform, as { s3_key = local_file_path }. Used for the MWAA requirements.txt / plugins.zip"
  type        = map(string)
  default     = {}
}
