output "emr_application_id" {
  description = "EMR Serverless application ID used to submit jobs."
  value       = module.emr.emr_sls_app_id
}

output "emr_execution_role_arn" {
  description = "IAM role ARN assumed by EMR Serverless job runs."
  value       = aws_iam_role.emr_execution_role.arn
}

output "emr_operation_role_arn" {
  description = "IAM role ARN for submitting and managing EMR Serverless jobs."
  value       = aws_iam_role.emr_operation_role.arn
}

output "gold_database_name" {
  description = "Glue database for published gold Iceberg tables."
  value       = aws_glue_catalog_database.gold.name
}

output "gold_staging_database_name" {
  description = "Glue database for staging gold Iceberg tables."
  value       = aws_glue_catalog_database.gold_staging.name
}
