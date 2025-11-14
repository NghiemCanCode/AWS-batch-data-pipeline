module "data-lake-bucket" {
  source = "../../modules/storage"
  bucket_name = "${var.data_lake_bucket_name}-dev"
  versioning_state = "Enabled"
  bucket_tags = {
    "environment": "dev"
  }
}

module "dags-code-bucket" {
  source = "../../modules/storage"
  bucket_name = "${var.dags_bucket_name}-dev"
  versioning_state = "Enabled"
  bucket_tags = {
    "environment": "dev"
  }
}

module "pipeline-code-bucket" {
  source = "../../modules/storage"
  bucket_name = "${var.pipeline_scripts_bucket_name}-dev"
  versioning_state = "Enabled"
  bucket_tags = {
    "environment": "dev"
  }
}