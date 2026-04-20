resource "aws_emrserverless_application" "emr_serverless_app" {
  name          = var.application_name
  release_label = var.release_label
  type          = var.application_type

  maximum_capacity {
    cpu    = "5 vCPU"
    memory = "32 GB"
    disk   = "500 GB"
  }

  monitoring_configuration {
    s3_monitoring_configuration {
      log_uri = "s3://${var.s3_artifacts_bucket}/logs/"
    }
  }

}
