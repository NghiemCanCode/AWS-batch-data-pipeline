variable "region" {
  description = "AWS Region"
  type        = string
}

# ---------- Network ----------

variable "name_prefix" {
  description = "Name prefix for network resources, e.g. finance-transaction"
  type        = string
}

variable "availability_zones" {
  description = "The 2 availability zones MWAA runs in"
  type        = list(string)
}

variable "vpc_cidr" {
  description = "CIDR block of the VPC"
  type        = string
  default     = "10.0.0.0/16"
}

variable "single_nat_gateway" {
  description = "true for one shared NAT gateway (dev), false for one per AZ (prod)"
  type        = bool
  default     = true
}

# ---------- MWAA ----------

variable "mwaa_environment_name" {
  description = "Name of the MWAA environment (without -dev suffix)"
  type        = string
}

variable "airflow_version" {
  description = "Airflow version, must be one MWAA currently supports"
  type        = string
}

variable "mwaa_environment_class" {
  description = "MWAA environment class"
  type        = string
  default     = "mw1.small"
}

variable "mwaa_min_workers" {
  description = "Minimum MWAA workers"
  type        = number
  default     = 1
}

variable "mwaa_max_workers" {
  description = "Maximum MWAA workers"
  type        = number
  default     = 2
}

variable "webserver_access_mode" {
  description = "PUBLIC_ONLY for dev, PRIVATE_ONLY for prod"
  type        = string
  default     = "PUBLIC_ONLY"
}

variable "private_webserver_allowed_cidrs" {
  description = "CIDRs allowed on 443 when webserver_access_mode = PRIVATE_ONLY"
  type        = list(string)
  default     = []
}

variable "requirements_s3_object_version" {
  description = "Version id of requirements.txt, from the platform stack output mwaa_requirements_s3_object_version. null skips requirements entirely"
  type        = string
  default     = null
}

variable "enable_secrets_manager_backend" {
  description = "Read Airflow connections and variables from Secrets Manager"
  type        = bool
  default     = true
}

# Turn these on together with enable_secrets_manager_backend. Leaving them null
# while the backend is on means every lookup - aws_default included - pays a
# Secrets Manager API call before falling back to the metadata database.
variable "connections_lookup_pattern" {
  description = "Regex of connection ids to look up in Secrets Manager, e.g. \"^(redshift|snowflake)_\". null disables filtering"
  type        = string
  default     = null
}

variable "variables_lookup_pattern" {
  description = "Regex of variable keys to look up in Secrets Manager. null disables filtering"
  type        = string
  default     = null
}

variable "secrets_prefix" {
  description = "Secrets Manager path prefix for Airflow connections and variables"
  type        = string
  default     = "airflow"
}

# ---------- Cross-stack values ----------

variable "code_bucket_name" {
  description = "Code bucket name, from the platform stack. Holds dags/ and requirements.txt"
  type        = string
}

variable "data_lake_bucket_name" {
  description = "Data lake bucket name, from the platform stack"
  type        = string
}

variable "emr_application_id" {
  description = "EMR Serverless application id, from the compute stack"
  type        = string
}

variable "emr_execution_role_arn" {
  description = "EMR Serverless execution role ARN, from the compute stack. Airflow passes this role when submitting jobs"
  type        = string
}
