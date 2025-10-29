module "data-lake-bucket" {
  source = "../../modules/storage"
  bucket_name = "${var.data_lake_bucket_name}-dev"
  versioning_state = "Enabled"
  bucket_tags = {
    "environment": "dev"
  }
}