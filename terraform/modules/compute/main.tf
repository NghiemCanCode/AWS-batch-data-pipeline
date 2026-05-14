resource "aws_emrserverless_application" "emr_serverless_app" {
  name          = var.application_name
  release_label = var.release_label
  type          = var.application_type

  maximum_capacity {
    cpu    = "12 vCPU"
    memory = "96 GB"
    disk   = "500 GB"
  }

  dynamic "image_configuration" {
    for_each = var.custom_image_uri == "" ? [] : [1]
    content {
      image_uri = var.custom_image_uri
    }
  }

  monitoring_configuration {
    s3_monitoring_configuration {
      log_uri = "s3://${var.s3_artifacts_bucket}/logs/"
    }
  }
}
