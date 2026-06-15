output "bucket_name" {
  description = "S3 bucket name."
  value       = aws_s3_bucket.bucket.id
}

output "bucket_arn" {
  description = "S3 bucket ARN."
  value       = aws_s3_bucket.bucket.arn
}

output "bucket_uri" {
  description = "S3 bucket URI."
  value       = "s3://${aws_s3_bucket.bucket.id}"
}
