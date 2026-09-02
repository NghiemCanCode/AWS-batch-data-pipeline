output "mwaa_environment_name" {
  description = "MWAA environment name."
  value       = aws_mwaa_environment.mwaa.name
}

output "mwaa_environment_arn" {
  description = "MWAA environment ARN."
  value       = aws_mwaa_environment.mwaa.arn
}

output "mwaa_webserver_url" {
  description = "Airflow web UI URL."
  value       = aws_mwaa_environment.mwaa.webserver_url
}

output "mwaa_service_role_arn" {
  description = "Service-linked role MWAA created for the environment."
  value       = aws_mwaa_environment.mwaa.service_role_arn
}
