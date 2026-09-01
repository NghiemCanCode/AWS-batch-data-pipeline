resource "aws_mwaa_environment" "mwaa" {
  name               = var.environment_name
  airflow_version    = var.airflow_version
  environment_class  = var.environment_class
  execution_role_arn = var.execution_role_arn

  source_bucket_arn = var.source_bucket_arn
  dag_s3_path       = var.dag_s3_path

  requirements_s3_path           = var.requirements_s3_path
  requirements_s3_object_version = var.requirements_s3_object_version
  plugins_s3_path                = var.plugins_s3_path
  plugins_s3_object_version      = var.plugins_s3_object_version
  startup_script_s3_path         = var.startup_script_s3_path

  webserver_access_mode = var.webserver_access_mode

  min_workers = var.min_workers
  max_workers = var.max_workers
  schedulers  = var.schedulers

  weekly_maintenance_window_start = var.weekly_maintenance_window_start

  airflow_configuration_options = merge(
    var.enable_secrets_manager_backend ? {
      "secrets.backend" = "airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend"
      # Without a lookup pattern, EVERY connection and variable lookup calls
      # Secrets Manager first and only then falls back to the metadata database -
      # including aws_default, which every EMR task uses. That is one extra API
      # call and one extra round trip per task. The patterns below restrict which
      # names are worth asking Secrets Manager about.
      "secrets.backend_kwargs" = jsonencode(merge(
        {
          connections_prefix = "${var.secrets_prefix}/connections"
          variables_prefix   = "${var.secrets_prefix}/variables"
        },
        var.connections_lookup_pattern == null ? {} : { connections_lookup_pattern = var.connections_lookup_pattern },
        var.variables_lookup_pattern == null ? {} : { variables_lookup_pattern = var.variables_lookup_pattern },
      ))
    } : {},
    var.airflow_configuration_options
  )

  network_configuration {
    security_group_ids = var.security_group_ids
    subnet_ids         = var.subnet_ids
  }

  logging_configuration {
    dag_processing_logs {
      enabled   = true
      log_level = var.dag_processing_log_level
    }

    scheduler_logs {
      enabled   = true
      log_level = var.scheduler_log_level
    }

    task_logs {
      enabled   = true
      log_level = var.task_log_level
    }

    webserver_logs {
      enabled   = true
      log_level = var.webserver_log_level
    }

    worker_logs {
      enabled   = true
      log_level = var.worker_log_level
    }
  }

  tags = var.tags
}
