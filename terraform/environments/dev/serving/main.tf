resource "aws_athena_workgroup" "power_bi" {
  name        = "power-bi-dev"
  description = "Workgroup used by PowerBI data consumers to query the gold layer."

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "${module.result_bucket.bucket_uri}/power_bi/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }

  tags = { environment = "dev" }
}


module "result_bucket" {
  source           = "../../../modules/storage"
  bucket_name      = "${var.ressult_bucket_name}-dev"
  versioning_state = "Enabled"
  bucket_tags      = { environment = "dev" }
  folder_path      = ["power_bi/"]
}
