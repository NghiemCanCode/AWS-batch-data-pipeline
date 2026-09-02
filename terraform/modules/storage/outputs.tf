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

output "managed_object_version_ids" {
  description = "Version id of each managed object, keyed by S3 key. Feed these into requirements_s3_object_version / plugins_s3_object_version of aws_mwaa_environment."
  value       = { for key, object in aws_s3_object.managed-object : key => object.version_id }
}
