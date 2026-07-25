output "state_bucket_name" {
  value       = aws_s3_bucket.terraform_state.id
  description = "S3 bucket name — use this in environments/dev/providers.tf backend config"
}

