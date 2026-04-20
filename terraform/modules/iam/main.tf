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
