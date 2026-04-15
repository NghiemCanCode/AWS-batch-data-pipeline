variable "account_id" {
  type        = string
  description = "AWS Account ID, can get in console"
}

variable "region" {
  type        = string
  description = "AWS Region"
}

variable "application_id" {
  type        = string
  description = "Appication id, can get after emr application was created."
}

variable "lakehouse_bucket_name" {
  type        = string
  description = "Lakehouse bucket name, both input, output and logs"
}

variable "codebase_bucket_name" {
  type        = string
  description = "Codebase bucket name, both input, output and logs"
}
