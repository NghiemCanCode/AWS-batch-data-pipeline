module "data-lake-bucket" {
  source           = "../../modules/storage"
  bucket_name      = "${var.data_lake_bucket_name}-dev"
  versioning_state = "Enabled"
  bucket_tags = {
    "environment" : "dev"
  }
  folder_path = ["bronze/", "silver/", "gold/", "quarantine/"]
}

module "code-bucket" {
  source           = "../../modules/storage"
  bucket_name      = "${var.code_bucket_name}-dev"
  versioning_state = "Enabled"
  bucket_tags = {
    "environment" : "dev"
  }
  folder_path = ["jobs/", "packages/", "dags/", "logs/"]
}

module "emr-serverless-application" {
  source              = "../../modules/compute"
  application_name    = "${var.emr_serverless_application_name}-dev"
  release_label       = var.emr_serverless_release_label
  application_type    = var.emr_serverless_application_type
  s3_artifacts_bucket = module.code-bucket.bucket_name
}

module "emr_execution_role" {
  account_id            = var.account_id
  region                = var.region
  source                = "../../modules/iam"
  application_id        = module.emr-serverless-application.id
  lakehouse_bucket_name = module.data-lake-bucket.bucket_name
  codebase_bucket_name  = module.code-bucket.bucket_name
}
