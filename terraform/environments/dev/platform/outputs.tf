output "ecr_repository_url" {
  value = module.ecr.ecr_repository_url
}

output "data_lake_bucket_name" {
  value = module.data_lake.bucket_name
}

output "code_bucket_name" {
  value = module.code_bucket.bucket_name
}

output "github_actions_role_arn" {
  description = "IAM Role ARN for GitHub Actions OIDC — set as secret AWS_GITHUB_ACTIONS_ROLE_ARN in GitHub repo settings"
  value       = aws_iam_role.github_actions_role.arn
}
