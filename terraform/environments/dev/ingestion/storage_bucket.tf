# Airbyte's own object storage (replaces the in-cluster MinIO): job logs, state, workload output.
# The chart takes bucket names, not prefixes, so it gets a bucket of its own instead of sharing the ops bucket.
# CDC data never lands here: the S3 destination writes straight to the data lake.
resource "aws_s3_bucket" "storage" {
  bucket = "${local.name}-storage-${local.account_id}"

  # Learning setup: terraform destroy deletes the bucket with its objects
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "storage" {
  bucket                  = aws_s3_bucket.storage.id
  block_public_acls       = true
  ignore_public_acls      = true
  block_public_policy     = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_policy" "storage" {
  bucket = aws_s3_bucket.storage.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "DenyInsecureTransport"
      Effect    = "Deny"
      Principal = "*"
      Action    = "s3:*"
      Resource  = [aws_s3_bucket.storage.arn, "${aws_s3_bucket.storage.arn}/*"]
      Condition = { Bool = { "aws:SecureTransport" = "false" } }
    }]
  })
  depends_on = [aws_s3_bucket_public_access_block.storage]
}
