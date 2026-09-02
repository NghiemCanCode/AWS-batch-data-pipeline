resource "aws_s3_bucket" "bucket" {
  bucket = var.bucket_name
  tags   = var.bucket_tags
}

resource "aws_s3_bucket_versioning" "bucket-versioning" {
  bucket = aws_s3_bucket.bucket.id
  versioning_configuration {
    status = var.versioning_state
  }
}

resource "aws_s3_bucket_public_access_block" "bucket-public-access" {
  count = var.block_public_access ? 1 : 0

  bucket = aws_s3_bucket.bucket.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "bucket-encryption" {
  bucket = aws_s3_bucket.bucket.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = var.sse_algorithm
      kms_master_key_id = var.kms_key_arn
    }
    bucket_key_enabled = var.sse_algorithm == "aws:kms"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "bucket-lifecycle" {
  count = var.noncurrent_version_expiration_days > 0 ? 1 : 0

  bucket = aws_s3_bucket.bucket.id

  rule {
    id     = "expire-noncurrent-versions"
    status = "Enabled"

    filter {}

    noncurrent_version_expiration {
      noncurrent_days = var.noncurrent_version_expiration_days
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }

  depends_on = [aws_s3_bucket_versioning.bucket-versioning]
}

resource "aws_s3_object" "s3-folder-path" {

  for_each = toset(var.folder_path)

  bucket = aws_s3_bucket.bucket.id
  key    = each.value
}

resource "aws_s3_object" "managed-object" {

  for_each = var.managed_objects

  bucket = aws_s3_bucket.bucket.id
  key    = each.key
  source = each.value
  etag   = filemd5(each.value)

  depends_on = [aws_s3_bucket_versioning.bucket-versioning]
}
