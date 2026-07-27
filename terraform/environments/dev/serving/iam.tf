data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

# ====================================
# PowerBI Data Cosumer Role
# ====================================

resource "aws_iam_user" "data_consumer" {
  name = "data-consumer"
}


resource "aws_iam_policy" "data_consumer_glue" {
  name        = "GlueDataConsumerDev"
  description = "Allow data consumer access data table"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "glue:Get*",
        ]
        Resource = [
          "arn:aws:glue:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:catalog",
          "arn:aws:glue:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:database/${var.gold_database_name}",
          "arn:aws:glue:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:table/${var.gold_database_name}/*",
        ]
      }
    ]
  })
}


resource "aws_iam_policy" "data_consumer_athena" {
  name        = "AthenaDataConsumerDev"
  description = "Allow data consumer use athena to execute query."

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "athena:StartQueryExecution",
          "athena:StopQueryExecution",
          "athena:Get*",
          "athena:ListWorkGroups",
          "athena:ListDatabases",
          "athena:ListTableMetadata",
          "athena:CreatePreparedStatement",
          "athena:ListPreparedStatements",
          "athena:DeletePreparedStatement"
        ]
        Resource = [
          aws_athena_workgroup.power_bi.arn,
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "s3:GetBucketLocation",
          "s3:GetObject",
          "s3:ListBucket",
          "s3:ListBucketMultipartUploads",
          "s3:ListMultipartUploadParts",
          "s3:AbortMultipartUpload",
          "s3:PutObject"
        ]
        Resource = [
          module.result_bucket.bucket_arn,
          "${module.result_bucket.bucket_arn}/*",
        ]
      }
    ]
  })
}

resource "aws_iam_policy" "data_consumer_datalake" {
  name        = "DataLakeDataConsumerDev"
  description = "Allow data consumer read gold layer data and Iceberg metadata."

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:GetBucketLocation"]
        Resource = "arn:aws:s3:::${var.data_lake_bucket_name}"
      },
      {
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = "arn:aws:s3:::${var.data_lake_bucket_name}"
        Condition = {
          StringLike = {
            "s3:prefix" = ["${var.gold_data_prefix}*"]
          }
        }
      },
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject"]
        Resource = "arn:aws:s3:::${var.data_lake_bucket_name}/${var.gold_data_prefix}*"
      }
    ]
  })
}

resource "aws_iam_user_policy_attachment" "data_consumer_glue" {
  user       = aws_iam_user.data_consumer.name
  policy_arn = aws_iam_policy.data_consumer_glue.arn
}

resource "aws_iam_user_policy_attachment" "data_consumer_athena" {
  user       = aws_iam_user.data_consumer.name
  policy_arn = aws_iam_policy.data_consumer_athena.arn
}

resource "aws_iam_user_policy_attachment" "data_consumer_datalake" {
  user       = aws_iam_user.data_consumer.name
  policy_arn = aws_iam_policy.data_consumer_datalake.arn
}

