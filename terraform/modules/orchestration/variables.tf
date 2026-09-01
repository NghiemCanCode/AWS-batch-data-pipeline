variable "environment_name" {
  description = "Name of the MWAA environment"
  type        = string
}

variable "airflow_version" {
  description = "Airflow version. Must be one of the versions MWAA currently supports"
  type        = string
}

variable "environment_class" {
  description = "MWAA environment class: mw1.micro, mw1.small, mw1.medium, mw1.large"
  type        = string
  default     = "mw1.small"
}

variable "execution_role_arn" {
  description = "IAM role assumed by the MWAA environment"
  type        = string
}

variable "source_bucket_arn" {
  description = "ARN of the S3 bucket holding dags/, requirements.txt and plugins.zip"
  type        = string
}

variable "dag_s3_path" {
  description = "Prefix inside the source bucket where DAGs live"
  type        = string
  default     = "dags/"
}

variable "requirements_s3_path" {
  description = "Key of requirements.txt inside the source bucket. null disables it"
  type        = string
  default     = null
}

variable "requirements_s3_object_version" {
  description = "S3 object version of requirements.txt. Changing it is what makes MWAA reinstall packages"
  type        = string
  default     = null
}

variable "plugins_s3_path" {
  description = "Key of plugins.zip inside the source bucket. null disables it"
  type        = string
  default     = null
}

variable "plugins_s3_object_version" {
  description = "S3 object version of plugins.zip"
  type        = string
  default     = null
}

variable "startup_script_s3_path" {
  description = "Key of the startup shell script inside the source bucket. null disables it"
  type        = string
  default     = null
}

variable "subnet_ids" {
  description = "Exactly 2 private subnet ids in 2 different availability zones"
  type        = list(string)

  validation {
    condition     = length(var.subnet_ids) == 2
    error_message = "MWAA requires exactly 2 private subnets."
  }
}

variable "security_group_ids" {
  description = "Security groups attached to the MWAA ENIs. Must contain a self-referencing rule"
  type        = list(string)
}

variable "webserver_access_mode" {
  description = "PUBLIC_ONLY or PRIVATE_ONLY"
  type        = string
  default     = "PUBLIC_ONLY"

  validation {
    condition     = contains(["PUBLIC_ONLY", "PRIVATE_ONLY"], var.webserver_access_mode)
    error_message = "webserver_access_mode must be PUBLIC_ONLY or PRIVATE_ONLY."
  }
}

variable "min_workers" {
  description = "Minimum number of workers kept running"
  type        = number
  default     = 1
}

variable "max_workers" {
  description = "Maximum number of workers MWAA scales out to"
  type        = number
  default     = 2
}

variable "schedulers" {
  description = "Number of schedulers. mw1.small supports 2"
  type        = number
  default     = 2
}

variable "weekly_maintenance_window_start" {
  description = "Weekly patching window, format DAY:HH:MM in UTC"
  type        = string
  default     = "SUN:18:00"
}

variable "enable_secrets_manager_backend" {
  description = "Read Airflow connections and variables from Secrets Manager instead of the MWAA metadata database"
  type        = bool
  default     = true
}

# Only names matching these regexes are looked up in Secrets Manager; everything
# else goes straight to the metadata database. null means "look up everything",
# which bills an API call for names that are never stored there - aws_default
# above all. Set them as soon as the backend is enabled.
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
  description = "Secrets Manager path prefix for connections and variables"
  type        = string
  default     = "airflow"
}

variable "airflow_configuration_options" {
  description = "Extra airflow.cfg overrides, merged on top of the secrets backend options"
  type        = map(string)
  default     = {}
}

variable "dag_processing_log_level" {
  description = "Log level for DAG processing logs"
  type        = string
  default     = "WARNING"
}

variable "scheduler_log_level" {
  description = "Log level for scheduler logs"
  type        = string
  default     = "WARNING"
}

variable "task_log_level" {
  description = "Log level for task logs. Keep at INFO, this is where operator output lands"
  type        = string
  default     = "INFO"
}

variable "webserver_log_level" {
  description = "Log level for webserver logs"
  type        = string
  default     = "WARNING"
}

variable "worker_log_level" {
  description = "Log level for worker logs"
  type        = string
  default     = "INFO"
}

variable "tags" {
  description = "Tags applied to the environment"
  type        = map(string)
  default     = {}
}
