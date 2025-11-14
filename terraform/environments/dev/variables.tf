variable "region" {
  description = "Main region of AWS resources"
  type = string
}

variable "data_lake_bucket_name" {
  description = "Data lake bucket name"
  type = string
}

variable "dags_bucket_name" {
  description = "Dags bucket name"
  type = string
}

variable "pipeline_scripts_bucket_name" {
  description = "ETL script bucket name"
  type = string
}