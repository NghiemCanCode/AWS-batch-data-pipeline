{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "SilverPrefixReadOnly",
      "Effect": "Allow",
      "Action": ["s3:GetObject"],
      "Resource": "arn:aws:s3:::${DATALAKE_BUCKET_NAME}/silver/*"
    },
    {
      "Sid": "SilverPrefixListOnly",
      "Effect": "Allow",
      "Action": ["s3:ListBucket"],
      "Resource": "arn:aws:s3:::${DATALAKE_BUCKET_NAME}",
      "Condition": {
        "StringLike": { "s3:prefix": ["silver/*"] }
      }
    },
    {
      "Sid": "GlueCatalogScopedToDatabase",
      "Effect": "Allow",
      "Action": [
        "glue:GetDatabase",
        "glue:GetTable",
        "glue:GetTables",
        "glue:CreateTable",
        "glue:UpdateTable",
        "glue:BatchCreatePartition",
        "glue:GetPartition",
        "glue:GetPartitions",
        "glue:BatchGetPartition"
      ],
      "Resource": [
        "arn:aws:glue:${AWS_REGION}:${AWS_ACCOUNT_ID}:catalog",
        "arn:aws:glue:${AWS_REGION}:${AWS_ACCOUNT_ID}:database/${GLUE_DATABASE_NAME}",
        "arn:aws:glue:${AWS_REGION}:${AWS_ACCOUNT_ID}:table/${GLUE_DATABASE_NAME}/*"
      ]
    },
    {
      "Sid": "CrawlerLogsOnly",
      "Effect": "Allow",
      "Action": ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
      "Resource": "arn:aws:logs:${AWS_REGION}:${AWS_ACCOUNT_ID}:log-group:/aws-glue/crawlers*"
    }
  ]
}
