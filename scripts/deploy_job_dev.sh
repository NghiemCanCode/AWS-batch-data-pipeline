#!/bin/bash
set -euo pipefail

# ===========================================
# This script will rebuild ALL, include: python package, EMR application, S3 bucket.
# The script is optimized for the current development stage. Further optimization will be applied once CI/CD is stable.
# ===========================================

# Root path of the project (one level above the scripts/ directory)
ROOT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Flag: only destroy if terraform has been applied successfully
TERRAFORM_APPLIED=false
TERRAFORM_DIR="$ROOT_PATH/terraform/environments/dev"

# Rollback: destroy terraform resources if any step after apply fails
rollback() {
  if [ "$TERRAFORM_APPLIED" = true ]; then
    echo ""
    echo "ERROR: A step failed after terraform apply. Rolling back infrastructure..."
    cd "$TERRAFORM_DIR"
    terraform destroy -auto-approve
    echo "Rollback complete. All terraform resources have been destroyed."
  fi
}
trap rollback ERR

# Read environment variables
cd "$ROOT_PATH"
source scripts/.env

# Build the newest python package
echo "Building the newest python package..."
source .venv/bin/activate
python -m build --no-isolation --wheel
echo "Checking package..."
ls -la "$ROOT_PATH/dist/"

# Terraform build infrastructure
echo "Building the newest infrastructure..."
cd "$TERRAFORM_DIR"
terraform apply -auto-approve
TERRAFORM_APPLIED=true

# Get infrastructure output
echo "Getting infrastructure output..."
datalake_bucket=$(terraform output -raw datalake_bucket_name)
codebase_bucket=$(terraform output -raw code_bucket_name)
execution_role_arn=$(terraform output -raw emr_serverless_iam_role_arn)
application_id=$(terraform output -raw emr_serverless_application_id)

# Upload python package & code entry point to S3

aws s3 cp "$ROOT_PATH/dist/$py_package" s3://$codebase_bucket/packages/$py_package
aws s3 cp "$ROOT_PATH/jobs/$entry_point" s3://$codebase_bucket/jobs/$entry_point

s3_py_package="s3://$codebase_bucket/packages/$py_package"
s3_entry_point="s3://$codebase_bucket/jobs/$entry_point"
s3_log="s3://$codebase_bucket/logs/"
s3_datalake="s3://$datalake_bucket/"

# Run job scripts

job_driver=$(cat <<EOF
{
  "sparkSubmit": {
    "entryPoint": "$s3_entry_point"
  }
}
EOF
)

config_overrides=$(cat <<EOF
{
  "monitoringConfiguration": {
    "cloudWatchLoggingConfiguration": {
      "enabled": false
    }
  },
  "applicationConfiguration": [
    {
      "classification": "spark-defaults",
      "properties": {
        "spark.submit.pyFiles": "$s3_py_package",
        "spark.emr-serverless.driverEnv.INPUT_PATH": "$s3_datalake",
        "spark.emr-serverless.driverEnv.OUTPUT_PATH": "$s3_datalake",
        "spark.emr-serverless.driverEnv.QUARANTINE_PATH": "$s3_datalake",
        "spark.pyspark.python": "/usr/bin/python3.11",
        "spark.pyspark.driver.python": "/usr/bin/python3.11"
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
