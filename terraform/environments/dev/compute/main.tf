module "emr" {
  source              = "../../../modules/compute"
  application_name    = "${var.emr_app_name}-dev"
  release_label       = var.emr_release_label
  application_type    = var.emr_app_type
  s3_artifacts_bucket = var.code_bucket_name
  custom_image_uri    = var.custom_image_uri
}

module "emr_iam" {
  source                = "../../../modules/iam"
  account_id            = var.account_id
  region                = var.region
  application_id        = module.emr.emr_sls_app_id
  lakehouse_bucket_name = var.data_lake_bucket_name
  codebase_bucket_name  = var.code_bucket_name
}
