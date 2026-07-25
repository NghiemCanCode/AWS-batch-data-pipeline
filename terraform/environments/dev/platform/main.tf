module "data_lake" {
  source           = "../../../modules/storage"
  bucket_name      = "${var.data_lake_bucket_name}-dev"
  versioning_state = "Enabled"
  bucket_tags      = { environment = "dev" }
  folder_path      = ["bronze/", "silver/", "gold/", "gold/iceberg/", "quarantine/"]
}

module "code_bucket" {
  source           = "../../../modules/storage"
  bucket_name      = "${var.code_bucket_name}-dev"
  versioning_state = "Enabled"
  bucket_tags      = { environment = "dev" }
  folder_path      = ["jobs/", "packages/", "dags/", "logs/"]
}

module "ecr" {
  source          = "../../../modules/registry"
  repository_name = "${var.ecr_registry_name}-dev"
}
