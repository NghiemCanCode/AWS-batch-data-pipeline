#!/bin/bash
set -euo pipefail

# ===========================================
# This script will rebuild ALL, include: python package, EMR application, S3 bucket.
# The script is optimized for the current development stage. Further optimization will be applied once CI/CD is stable.
# ===========================================

# ─── STEP 0: Variables & environment ───────────────

# Root path of the project (one level above the scripts/ directory)
ROOT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TR_PLATFORM_DIR="$ROOT_PATH/terraform/environments/dev/platform"
TR_COMPUTE_DIR="$ROOT_PATH/terraform/environments/dev/compute"
ECR_REPO_NAME="emr-serverless-custom"
ECR_IMAGE_URI="emr712-spark356-python312:latest"
GITHUB_REPO=$(gh repo view --json nameWithOwner --jq '.nameWithOwner')

# Read environment variables
cd "$ROOT_PATH"
source scripts/.env

BATCH_LOGICAL_DATE="2036-01-01"
AWS_REGION="ap-southeast-1"

# ─── STEP 1: Build platform infrastructure ───────────────
echo "Building platform infrastructure..."
cd "$TR_PLATFORM_DIR"
terraform apply -var="ecr_registry_name=$ECR_REPO_NAME" -var="github_repo=$GITHUB_REPO" -auto-approve

echo "Getting platform infrastructure output..."
datalake_bucket_name=$(terraform output -raw data_lake_bucket_name)
codebase_bucket_name=$(terraform output -raw code_bucket_name)
ecr_repository_url=$(terraform output -raw ecr_repository_url)
github_actions_role_arn=$(terraform output -raw github_actions_role_arn)

echo "Setting GitHub Actions secret..."
gh secret set AWS_GITHUB_ACTIONS_ROLE_ARN \
  --repo "$GITHUB_REPO" \
  --body "$github_actions_role_arn"

# ─── STEP 2: Build Spark image & push to ECR (via GitHub Actions) ───────────────
full_ecr_image_uri="${ecr_repository_url%/}:${ECR_IMAGE_URI##*:}"

echo "Triggering GitHub Actions workflow on repo: $GITHUB_REPO ..."
gh workflow run docker-build-push-ecr.yml \
  --repo "$GITHUB_REPO" \
  --ref refactor-spark \
  -f image_tag="latest"

# Small delay to let GitHub register the triggered run
sleep 50  

RUN_ID=$(gh run list \
  --repo "$GITHUB_REPO" \
  --workflow=docker-build-push-ecr.yml \
  --limit=1 \
  --json databaseId \
  --jq '.[0].databaseId')

echo "Waiting for workflow run #$RUN_ID to complete..."
gh run watch "$RUN_ID" --repo "$GITHUB_REPO" --exit-status

echo "Image available at: $full_ecr_image_uri"

# ─── STEP 3: Build & upload package & EMR job code ───────────────

# -- Building phase --
cd "$ROOT_PATH"
echo "Build the newest python package..."
source .venv/bin/activate
python -m build --no-isolation --wheel
echo "Checking package..."
ls -la "$ROOT_PATH/dist/"
# -- Uploading phase --

aws s3 cp "$ROOT_PATH/dist/$py_package" "s3://$codebase_bucket_name/packages/$py_package"
aws s3 cp "$ROOT_PATH/jobs/$entry_point" "s3://$codebase_bucket_name/jobs/$entry_point"

s3_py_package="s3://$codebase_bucket_name/packages/$py_package"
s3_entry_point="s3://$codebase_bucket_name/jobs/$entry_point"

datalake_bucket_uri="s3://$datalake_bucket_name"

# ─── STEP 4: Build compute infrastructure ───────────────
echo "Building compute infrastructure..."
cd "$TR_COMPUTE_DIR"
terraform apply \
  -var="custom_image_uri=$full_ecr_image_uri" \
  -var="data_lake_bucket_name=$datalake_bucket_name" \
  -var="code_bucket_name=$codebase_bucket_name" \
  -auto-approve

echo "Getting compute infrastructure output..."
execution_role_arn=$(terraform output -raw emr_execution_role_arn)
application_id=$(terraform output -raw emr_application_id)

# ─── STEP 5: Start EMR Serverless job ───────────────
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
        "spark.emr-serverless.driverEnv.INPUT_PATH": "$datalake_bucket_uri",
        "spark.emr-serverless.driverEnv.OUTPUT_PATH": "$datalake_bucket_uri",
        "spark.emr-serverless.driverEnv.QUARANTINE_PATH": "$datalake_bucket_uri",
        "spark.emr-serverless.driverEnv.PYSPARK_DRIVER_PYTHON": "/home/hadoop/environment/bin/python",
        "spark.emr-serverless.driverEnv.PYSPARK_PYTHON": "/home/hadoop/environment/bin/python",
        "spark.emr-serverless.executorEnv.PYSPARK_PYTHON": "/home/hadoop/environment/bin/python",
        "spark.emr-serverless.driverEnv.BATCH_LOGICAL_DATE": "$BATCH_LOGICAL_DATE",
        "spark.driver.cores": "2",
        "spark.driver.memory": "8g",
        "spark.executor.cores": "2",
        "spark.executor.memory": "8g",
        "spark.dynamicAllocation.enabled": "true",
        "spark.dynamicAllocation.minExecutors": "1",
        "spark.dynamicAllocation.maxExecutors": "5"
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
