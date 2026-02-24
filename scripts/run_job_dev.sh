#!/bin/bash
read -p "S3 URI Data Input Path: " input_path
read -p "S3 URI Data Output Path: " output_path
read -p "S3 URI Python Script Path: " script
read -p "S3 URI Python Package Path: " py_package
read -p "EMR Application ID: " application_id
read -p "EMR Execution Role ARN: " execution_role_arn
read -p "Job Name: " name
read -p "S3 URI Log Path: " s3_log_uri

job_driver=$(cat <<EOF
{
  "sparkSubmit": {
    "entryPoint": "$script"
  }
}
EOF
)

config_overrides=$(cat <<EOF
{
  "monitoringConfiguration": {
    "s3MonitoringConfiguration": {
      "logUri": "$s3_log_uri"
    },
    "cloudWatchLoggingConfiguration": {
      "enabled": false
    }
  },
  "applicationConfiguration": [
    {
      "classification": "spark-defaults",
      "properties": {
        "spark.submit.pyFiles": "$py_package",
        "spark.emr-serverless.driverEnv.INPUT_PATH": "$input_path",
        "spark.emr-serverless.driverEnv.OUTPUT_PATH": "$output_path"
      }
    }
  ]
}
EOF
)

aws emr-serverless start-job-run \
  --application-id "$application_id" \
  --execution-role-arn "$execution_role_arn" \
  --name "$name" \
  --job-driver "$job_driver" \
  --configuration-overrides "$config_overrides"