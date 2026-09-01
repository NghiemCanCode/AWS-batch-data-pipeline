data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

locals {
  # Single source of truth for the environment name. The "-dev" suffix is added
  # here, the same way every other stack suffixes its resources, and main.tf
  # passes this local to the module as environment_name — so the IAM ARNs below
  # cannot drift from the real environment name. var.mwaa_environment_name is
  # the base name and must NOT already carry the suffix.
  mwaa_name = "${var.mwaa_environment_name}-dev"

  # Built as a string instead of referencing the module: the environment needs
  # the role ARN, so referencing the environment here would be a cycle.
  mwaa_environment_arn = "arn:aws:airflow:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:environment/${local.mwaa_name}"
  mwaa_log_group_arn   = "arn:aws:logs:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:log-group:airflow-${local.mwaa_name}-*"
  code_bucket_arn      = "arn:aws:s3:::${var.code_bucket_name}"
}

# ====================================
# MWAA Execution Role
# ====================================

resource "aws_iam_role" "mwaa_execution_role" {
  name = "MWAAExecutionRoleDev"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Sid    = "MWAATrustPolicy"
        Principal = {
          # Both principals are required: airflow-env runs the environment,
          # airflow manages it.
          Service = [
            "airflow-env.amazonaws.com",
            "airflow.amazonaws.com"
          ]
        }
      }
    ]
  })
}

resource "aws_iam_policy" "mwaa_core" {
  name        = "MWAACoreDev"
  description = "Baseline permissions required by the MWAA environment itself"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["airflow:PublishMetrics"]
        Resource = local.mwaa_environment_arn
      },
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:GetObjectVersion",
          "s3:ListBucket",
          "s3:ListBucketVersions",
          "s3:GetBucketPublicAccessBlock"
        ]
        Resource = [
          local.code_bucket_arn,
          "${local.code_bucket_arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogStream",
          "logs:CreateLogGroup",
          "logs:PutLogEvents",
          "logs:GetLogEvents",
          "logs:GetLogRecord",
          "logs:GetLogGroupFields",
          "logs:GetQueryResults"
        ]
        Resource = local.mwaa_log_group_arn
      },
      {
        Effect = "Allow"
        Action = ["logs:DescribeLogGroups"]
        # NOTE (least privilege): logs:DescribeLogGroups does not support
        # resource-level permissions. Scoping it to the log group ARN makes the
        # call fail, which silently breaks MWAA's own logging setup - MWAA lists
        # log groups before it knows their full names. AWS's sample MWAA
        # execution role policy keeps it in a separate statement for this reason.
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = ["cloudwatch:PutMetricData"]
        # NOTE (least privilege): cloudwatch:PutMetricData does not support
        # resource-level permissions.
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "sqs:ChangeMessageVisibility",
          "sqs:DeleteMessage",
          "sqs:GetQueueAttributes",
          "sqs:GetQueueUrl",
          "sqs:ReceiveMessage",
          "sqs:SendMessage"
        ]
        # The Celery queue is created and owned by MWAA in an AWS-managed
        # account, so the account id cannot be pinned here.
        Resource = "arn:aws:sqs:${data.aws_region.current.region}:*:airflow-celery-*"
      },
      {
        Effect = "Allow"
        Action = [
          "kms:Decrypt",
          "kms:DescribeKey",
          "kms:GenerateDataKey*",
          "kms:Encrypt"
        ]
        # No customer managed key here, so these apply to the AWS managed keys
        # MWAA uses for its own queue and log group — hence NotResource on this
        # account's own keys, narrowed to the services that call KMS for MWAA.
        NotResource = "arn:aws:kms:*:${data.aws_caller_identity.current.account_id}:key/*"
        Condition = {
          StringLike = {
            "kms:ViaService" = [
              "sqs.${data.aws_region.current.region}.amazonaws.com",
              "s3.${data.aws_region.current.region}.amazonaws.com"
            ]
          }
        }
      }
    ]
  })
}

# Quyen rieng cua pipeline nay: submit job EMR Serverless va doc ket qua.
resource "aws_iam_policy" "mwaa_pipeline" {
  name        = "MWAAPipelineAccessDev"
  description = "Allow Airflow tasks to drive EMR Serverless and read the Glue catalog"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "emr-serverless:StartApplication",
          "emr-serverless:StopApplication",
          "emr-serverless:GetApplication",
          "emr-serverless:StartJobRun",
          "emr-serverless:GetJobRun",
          "emr-serverless:CancelJobRun",
          "emr-serverless:ListJobRuns",
          # Cho phep operator cua Airflow sinh link sang Spark UI cua job run.
          # Thieu quyen nay thi tu Airflow khong mo duoc UI de debug job hong.
          "emr-serverless:GetDashboardForJobRun"
        ]
        Resource = [
          "arn:aws:emr-serverless:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:/applications/${var.emr_application_id}",
          "arn:aws:emr-serverless:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:/applications/${var.emr_application_id}/jobruns/*"
        ]
      },
      {
        # Bat buoc: EMR Serverless chay job duoi execution role, nen nguoi
        # submit phai duoc phep trao role do.
        Effect   = "Allow"
        Action   = ["iam:PassRole"]
        Resource = var.emr_execution_role_arn
        Condition = {
          StringEquals = {
            "iam:PassedToService" = "emr-serverless.amazonaws.com"
          }
        }
      },
      {
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:GetTable",
          "glue:GetTables",
          "glue:GetPartition",
          "glue:GetPartitions"
        ]
        Resource = [
          "arn:aws:glue:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:catalog",
          "arn:aws:glue:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:database/*",
          "arn:aws:glue:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:table/*/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        Resource = [
          "arn:aws:s3:::${var.data_lake_bucket_name}",
          "arn:aws:s3:::${var.data_lake_bucket_name}/*"
        ]
      }
    ]
  })
}

resource "aws_iam_policy" "mwaa_secrets" {
  count = var.enable_secrets_manager_backend ? 1 : 0

  name        = "MWAASecretsAccessDev"
  description = "Allow Airflow to read connections and variables from Secrets Manager"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue",
          "secretsmanager:DescribeSecret"
        ]
        Resource = "arn:aws:secretsmanager:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:secret:${var.secrets_prefix}/*"
      },
      {
        Effect   = "Allow"
        Action   = ["secretsmanager:ListSecrets"]
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "mwaa_core" {
  role       = aws_iam_role.mwaa_execution_role.name
  policy_arn = aws_iam_policy.mwaa_core.arn
}

resource "aws_iam_role_policy_attachment" "mwaa_pipeline" {
  role       = aws_iam_role.mwaa_execution_role.name
  policy_arn = aws_iam_policy.mwaa_pipeline.arn
}

resource "aws_iam_role_policy_attachment" "mwaa_secrets" {
  count = var.enable_secrets_manager_backend ? 1 : 0

  role       = aws_iam_role.mwaa_execution_role.name
  policy_arn = aws_iam_policy.mwaa_secrets[0].arn
}
