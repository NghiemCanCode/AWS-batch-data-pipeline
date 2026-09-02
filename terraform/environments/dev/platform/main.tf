module "data_lake" {
  source           = "../../../modules/storage"
  bucket_name      = "${var.data_lake_bucket_name}-dev"
  versioning_state = "Enabled"
  bucket_tags      = { environment = "dev" }
  folder_path      = ["bronze/", "silver/", "gold/", "gold/iceberg/", "quarantine/"]
}

# Cung la bucket MWAA doc DAG: dag_s3_path = s3://<code_bucket>/dags/
module "code_bucket" {
  source                             = "../../../modules/storage"
  bucket_name                        = "${var.code_bucket_name}-dev"
  versioning_state                   = "Enabled"
  bucket_tags                        = { environment = "dev" }
  folder_path                        = ["jobs/", "packages/", "dags/", "plugins/", "logs/"]
  noncurrent_version_expiration_days = var.code_bucket_noncurrent_version_days
  managed_objects = {
    "requirements.txt" = "${path.root}/../../../../airflow/requirements.txt"
  }
}

module "ecr" {
  source          = "../../../modules/registry"
  repository_name = "${var.ecr_registry_name}-dev"
}
