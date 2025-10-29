resource "aws_s3_bucket" "bucket" {
  bucket = var.bucket_name

  tags = var.bucket_tags
}

resource "aws_s3_bucket_versioning" "bucket-versioning" {
  bucket = aws_s3_bucket.bucket.id
  versioning_configuration {
    status = var.versioning_state
  }
}