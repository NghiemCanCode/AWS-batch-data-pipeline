module "data-lake-bucket" {
  source = "../../modules/storage"
  bucket_name = "${var.data_lake_bucket_name}-dev"
  versioning_state = "Enabled"
  bucket_tags = {
    "environment": "dev"
  }
  folder_path = ["bronze/", "silver/", "gold/"]
}

module "code-bucket" {
  source           = "../../modules/storage"
  bucket_name      = "${var.code_bucket_name}-dev"
  versioning_state = "Enabled"
  bucket_tags = {
    "environment" : "dev"
  }
  folder_path = ["etl/", "dags/"]
}