variable "region" {
  description = "Main region of AWS resources."
  type        = string
}

variable "ressult_bucket_name" {
  description = "Name of bucket which store results of athena's query."
  type        = string
}

variable "data_lake_bucket_name" {
  description = "Data lake bucket name which stores the gold layer data."
  type        = string
}

variable "gold_data_prefix" {
  description = "S3 prefix of the gold Iceberg warehouse inside the data lake bucket."
  type        = string
  default     = "gold/iceberg/"
}

variable "gold_database_name" {
  description = "Name of the gold Glue catalog database exposed to data consumers."
  type        = string
  default     = "gold"
}
