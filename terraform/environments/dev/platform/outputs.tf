output "ecr_repository_url" {
  description = "Dev ECR repository URL for EMR custom images."
  value       = module.ecr.ecr_repository_url
}

output "data_lake_bucket_name" {
  description = "Dev data lake S3 bucket name."
  value       = module.data_lake.bucket_name
}

output "data_lake_bucket_uri" {
  description = "Dev data lake S3 bucket URI."
  value       = module.data_lake.bucket_uri
}

output "code_bucket_name" {
  description = "Dev code artifact S3 bucket name."
  value       = module.code_bucket.bucket_name
}

output "code_bucket_uri" {
  description = "Dev code artifact S3 bucket URI."
  value       = module.code_bucket.bucket_uri
}

output "github_actions_role_arn" {
  description = "IAM Role ARN for GitHub Actions OIDC. Set as AWS_GITHUB_ACTIONS_ROLE_ARN in GitHub repo settings."
  value       = aws_iam_role.github_actions_role.arn
}
