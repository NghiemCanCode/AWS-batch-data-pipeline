data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

# ====================================
# EMR Execution Role
# ====================================

resource "aws_iam_role" "emr_execution_role" {
  name = "EMRExecutionRoleDev"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Sid    = "ServerlessTrustPolicy"
        Principal = {
          Service = "emr-serverless.amazonaws.com"
        }
        Condition = {
          StringEquals = {
            "aws:SourceAccount" = data.aws_caller_identity.current.account_id
            "aws:SourceArn"     = "arn:aws:emr-serverless:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:/applications/${module.emr.emr_sls_app_id}"
          }
        }
      }
    ]
  })
}

resource "aws_iam_policy" "emr_s3_access" {
  name        = "EMRServerlessS3AccessDev"
  description = "Allow EMR Serverless to access specific S3 buckets"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        Resource = [
          "arn:aws:s3:::${var.data_lake_bucket_name}",
          "arn:aws:s3:::${var.data_lake_bucket_name}/*",
          "arn:aws:s3:::${var.code_bucket_name}",
          "arn:aws:s3:::${var.code_bucket_name}/*",
        ]
      }
    ]
  })
}

resource "aws_iam_policy" "emr_ecr_access" {
  name        = "EMRServerlessECRAccessDev"
  description = "Allow EMR Serverless to pull custom images from ECR"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "ecr:GetAuthorizationToken",
          "ecr:BatchGetImage",
          "ecr:GetDownloadUrlForLayer"
        ]
        # NOTE (least privilege): ecr:GetAuthorizationToken requires Resource = "*".
        # ecr:BatchGetImage and ecr:GetDownloadUrlForLayer are included in this
        # statement and therefore also receive wildcard access. For production,
        # split them into a separate statement scoped to the required ECR repository ARN(s).
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_policy" "emr_glue_catalog_access" {
  name        = "EMRServerlessGlueCatalogAccessDev"
  description = "Allow EMR Serverless to use Glue Catalog for Gold Iceberg tables"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "glue:GetCatalog",
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:CreateDatabase",
          "glue:UpdateDatabase",
          "glue:GetTable",
          "glue:GetTables",
          "glue:CreateTable",
          "glue:UpdateTable",
          "glue:DeleteTable",
          "glue:GetPartition",
          "glue:GetPartitions",
          "glue:CreatePartition",
          "glue:UpdatePartition",
          "glue:DeletePartition",
          "glue:BatchCreatePartition",
          "glue:BatchDeletePartition",
          "glue:BatchGetPartition"
        ]
        Resource = [
          "arn:aws:glue:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:catalog",
          "arn:aws:glue:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:database/${aws_glue_catalog_database.gold.name}",
          "arn:aws:glue:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:database/${aws_glue_catalog_database.gold_staging.name}",
          "arn:aws:glue:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:table/${aws_glue_catalog_database.gold.name}/*",
          "arn:aws:glue:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:table/${aws_glue_catalog_database.gold_staging.name}/*"
        ]
      }
    ]
  })
}

# Removed: aws_iam_policy.emr_session ("EMRServerlessSessionLevelAccess").
# It granted GetSession / GetSessionEndpoint / TerminateSession but was attached to
# no role, user or group - in Terraform or on AWS - so it granted nobody anything.
# scripts/gold-dbt/deploy_gold_dbt_dev.sh drives interactive sessions as the human
# operator's own IAM identity, not through a role this stack manages, so those
# permissions belong on that identity. Add the policy back only together with an
# attachment that says who it is for.

resource "aws_iam_policy" "emr_cloudwatch_logs" {
  name        = "EMRServerlessCloudWatchLogsDev"
  description = "Allow EMR Serverless job runs to write driver/executor logs to CloudWatch"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
          "logs:DescribeLogStreams"
        ]
        # The trailing ":*" covers the log streams inside the group - EMR creates
        # one stream per driver/executor. logs:DescribeLogGroups is deliberately
        # NOT in this statement: it does not support resource-level permissions
        # and would be denied here (see the same note in the orchestration stack).
        Resource = "arn:aws:logs:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:log-group:${local.emr_log_group_name}:*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "emr_s3_access" {
  role       = aws_iam_role.emr_execution_role.name
  policy_arn = aws_iam_policy.emr_s3_access.arn
}

resource "aws_iam_role_policy_attachment" "emr_cloudwatch_logs" {
  role       = aws_iam_role.emr_execution_role.name
  policy_arn = aws_iam_policy.emr_cloudwatch_logs.arn
}

resource "aws_iam_role_policy_attachment" "emr_ecr_access" {
  role       = aws_iam_role.emr_execution_role.name
  policy_arn = aws_iam_policy.emr_ecr_access.arn
}

resource "aws_iam_role_policy_attachment" "emr_glue_catalog_access" {
  role       = aws_iam_role.emr_execution_role.name
  policy_arn = aws_iam_policy.emr_glue_catalog_access.arn
}

# ====================================
# EMR Operation Role
# ====================================

resource "aws_iam_role" "emr_operation_role" {
  name = "EMROperationRoleDev"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Sid    = "OperationTrustPolicy"
        Principal = {
          Service = "emr-serverless.amazonaws.com"
        }
        Condition = {
          StringEquals = {
            "aws:SourceAccount" = data.aws_caller_identity.current.account_id
          }
        }
      }
    ]
  })
}

resource "aws_iam_policy" "emr_job_submission" {
  name        = "EMRServerlessJobSubmissionDev"
  description = "Allow submitting and managing EMR Serverless jobs"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "emr-serverless:StartJobRun",
          "emr-serverless:GetJobRun",
          "emr-serverless:CancelJobRun",
          "emr-serverless:ListJobRuns",
          "emr-serverless:GetApplication",
        ]
        # NOTE (least privilege): this permits the listed actions for every EMR
        # Serverless resource. Scope to this environment's application/job-run
        # ARN(s) where the relevant action supports resource-level permissions.
        Resource = "*"
      },
      {
        Effect   = "Allow"
        Action   = ["iam:PassRole"]
        Resource = aws_iam_role.emr_execution_role.arn
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "emr_job_submission" {
  role       = aws_iam_role.emr_operation_role.name
  policy_arn = aws_iam_policy.emr_job_submission.arn
}
