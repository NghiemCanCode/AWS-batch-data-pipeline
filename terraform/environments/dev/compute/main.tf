module "emr" {
  source              = "../../../modules/compute"
  application_name    = "${var.emr_app_name}-dev"
  release_label       = var.emr_release_label
  application_type    = var.emr_app_type
  s3_artifacts_bucket = var.code_bucket_name
  custom_image_uri    = var.custom_image_uri
}
