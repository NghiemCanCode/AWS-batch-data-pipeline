variable "region" {
  description = "Main region of AWS resources"
  type = string
}

variable "data_lake_bucket_name" {
  description = "Data lake bucket name"
  type = string
}

variable "code_bucket_name" {
  description = "Code/script bucket name"
  type = string
}