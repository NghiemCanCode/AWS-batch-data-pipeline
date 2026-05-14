output "emr_application_id" {
  value = module.emr.emr_sls_app_id
}

output "emr_execution_role_arn" {
  value = module.emr_iam.emr_execution_role_arn
}

output "emr_operation_role_arn" {
  value = module.emr_iam.emr_operation_role_arn
}
