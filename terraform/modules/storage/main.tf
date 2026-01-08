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

resource "aws_s3_object" "s3-folder-path" {

  for_each = toset(var.folder_path)

  bucket = aws_s3_bucket.bucket.id
  key = each.value
}