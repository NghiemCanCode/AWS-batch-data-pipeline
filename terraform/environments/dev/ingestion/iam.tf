data "aws_iam_policy_document" "ec2_trust" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

# ---------- Session Manager log streaming ----------
data "aws_iam_policy_document" "session_logs" {
  statement {
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents", "logs:DescribeLogStreams"]
    resources = ["arn:aws:logs:${local.region}:${local.account_id}:log-group:${var.session_log_group}:*"]
  }
  statement {
    actions   = ["logs:DescribeLogGroups"]
    resources = ["*"]
  }
}

resource "aws_iam_policy" "session_logs" {
  name   = "${local.name}-ssm-session-logs"
  policy = data.aws_iam_policy_document.session_logs.json
}

# ---------- Airbyte host permissions ----------
locals {
  data_lake_arn = "arn:aws:s3:::${var.data_lake_bucket_name}"
}

data "aws_iam_policy_document" "airbyte" {
  # The S3 destination connector writes with the instance profile: no access key anywhere
  statement {
    sid = "BronzeCdcWrite"
    actions = [
      "s3:PutObject", "s3:GetObject", "s3:DeleteObject",
      "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts",
    ]
    resources = ["${local.data_lake_arn}/${local.cdc_prefix}/*"]
  }
  # No s3:prefix condition here: the connector's check calls HeadBucket, which carries no prefix and would be denied
  statement {
    sid       = "DataLakeList"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation", "s3:ListBucketMultipartUploads"]
    resources = [local.data_lake_arn]
  }
  # Airbyte's own storage (job logs, state, workload output), used by the server and worker pods
  statement {
    sid       = "AirbyteStorageRW"
    actions   = ["s3:PutObject", "s3:GetObject", "s3:DeleteObject"]
    resources = ["${aws_s3_bucket.storage.arn}/*"]
  }
  statement {
    sid       = "AirbyteStorageList"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.storage.arn]
  }
  statement {
    sid       = "DeployRead"
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.ops.arn}/deploy/*"]
  }
  statement {
    sid       = "BackupWrite"
    actions   = ["s3:PutObject", "s3:GetObject"]
    resources = ["${aws_s3_bucket.ops.arn}/backup/*"]
  }
  statement {
    sid       = "OpsList"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.ops.arn]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["deploy", "deploy/*", "backup", "backup/*"]
    }
  }
  # Own admin password, plus the CA cert every source publishes (public material, needed for verify-full tests).
  # Cannot read any source role password: those are typed into the UI by hand (step 9.2).
  statement {
    sid     = "Params"
    actions = ["ssm:GetParameter"]
    resources = [
      aws_ssm_parameter.admin_password.arn,
      "arn:aws:ssm:${local.region}:${local.account_id}:parameter/fds/*/tls/ca-crt",
    ]
  }
  statement {
    sid       = "Metrics"
    actions   = ["cloudwatch:PutMetricData"]
    resources = ["*"]
    condition {
      test     = "StringEquals"
      variable = "cloudwatch:namespace"
      values   = [local.metric_namespace]
    }
  }
}

resource "aws_iam_role" "airbyte" {
  name               = local.name
  assume_role_policy = data.aws_iam_policy_document.ec2_trust.json
}

resource "aws_iam_role_policy_attachment" "airbyte" {
  for_each = {
    ssm          = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
    session_logs = aws_iam_policy.session_logs.arn
  }
  role       = aws_iam_role.airbyte.name
  policy_arn = each.value
}

resource "aws_iam_role_policy" "airbyte" {
  name   = local.name
  role   = aws_iam_role.airbyte.id
  policy = data.aws_iam_policy_document.airbyte.json
}

resource "aws_iam_instance_profile" "airbyte" {
  name = local.name
  role = aws_iam_role.airbyte.name
}
