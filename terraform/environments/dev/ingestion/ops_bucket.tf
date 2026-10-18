# deploy/: scripts pushed to the host (step 4.5). backup/: dumps of Airbyte's internal database (step 12).
resource "aws_s3_bucket" "ops" {
  bucket = "${local.name}-ops-${local.account_id}"

  # Learning setup: terraform destroy deletes the bucket with its objects
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "ops" {
  bucket                  = aws_s3_bucket.ops.id
  block_public_acls       = true
  ignore_public_acls      = true
  block_public_policy     = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_policy" "ops" {
  bucket = aws_s3_bucket.ops.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "DenyInsecureTransport"
      Effect    = "Deny"
      Principal = "*"
      Action    = "s3:*"
      Resource  = [aws_s3_bucket.ops.arn, "${aws_s3_bucket.ops.arn}/*"]
      Condition = { Bool = { "aws:SecureTransport" = "false" } }
    }]
  })
  depends_on = [aws_s3_bucket_public_access_block.ops]
}

# Database dumps are plain files (not a pgBackRest repo), so an expiry rule is safe here
resource "aws_s3_bucket_lifecycle_configuration" "ops" {
  bucket = aws_s3_bucket.ops.id
  rule {
    id     = "expire-db-dumps"
    status = "Enabled"
    filter {
      prefix = "backup/"
    }
    expiration {
      days = 30
    }
  }
}
