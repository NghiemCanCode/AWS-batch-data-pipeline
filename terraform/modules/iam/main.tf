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
            "aws:SourceAccount" = var.account_id
            "aws:SourceArn"     = "arn:aws:emr-serverless:${var.region}:${var.account_id}:/applications/${var.application_id}"
          }
        }
      },
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
          "arn:aws:s3:::${var.lakehouse_bucket_name}",
          "arn:aws:s3:::${var.lakehouse_bucket_name}/*",

          "arn:aws:s3:::${var.codebase_bucket_name}",
          "arn:aws:s3:::${var.codebase_bucket_name}/*",
        ]
      }
    ]
  })
}


resource "aws_iam_policy_attachment" "emr_execution_role_attachment" {
  name       = "EMRExecutionRolePolicyDev"
  policy_arn = aws_iam_policy.emr_s3_access.arn
  roles      = [aws_iam_role.emr_execution_role.name]
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
            "aws:SourceAccount" = var.account_id
          }
        }
      },
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
        # GetAuthorizationToken does not support resource-level permissions
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_policy_attachment" "emr_ecr_access_attachment" {
  name       = "EMRExecutionRoleECRPolicyDev"
  policy_arn = aws_iam_policy.emr_ecr_access.arn
  roles      = [aws_iam_role.emr_execution_role.name]
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

resource "aws_iam_policy_attachment" "emr_operation_role_attachment" {
  name       = "EMROperationRolePolicyDev"
  policy_arn = aws_iam_policy.emr_job_submission.arn
  roles      = [aws_iam_role.emr_operation_role.name]
}
