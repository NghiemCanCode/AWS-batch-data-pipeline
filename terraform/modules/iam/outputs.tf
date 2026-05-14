output "emr_execution_role_arn" {
  value = aws_iam_role.emr_execution_role.arn
}

output "emr_operation_role_arn" {
  value = aws_iam_role.emr_operation_role.arn
}