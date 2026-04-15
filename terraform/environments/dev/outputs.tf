output "datalake_bucket_name" {
  value = module.data-lake-bucket.bucket_name
}

output "code_bucket_name" {
  value = module.code-bucket.bucket_name
}

output "emr_serverless_application_id" {
  value = module.emr-serverless-application.id
}

output "emr_serverless_iam_role_arn" {
  value = module.emr_execution_role.emr_execution_role_arn
}