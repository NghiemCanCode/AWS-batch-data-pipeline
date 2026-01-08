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