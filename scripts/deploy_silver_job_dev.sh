#!/bin/bash
set -euo pipefail

# ===========================================
# Starts the Bronze -> Silver EMR Serverless job.
# Requires scripts/.env.runtime — run _dev_setup.sh first.
# ===========================================

# ─── STEP 0: Variables & environment ───────────────

ROOT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUNTIME_ENV="$ROOT_PATH/scripts/.env.runtime"

if [ ! -f "$RUNTIME_ENV" ]; then
  echo "Error: $RUNTIME_ENV not found. Run _dev_setup.sh first."
  exit 1
fi

source "$RUNTIME_ENV"

cd "$ROOT_PATH"
if [ -f scripts/.env.runtime ]; then
  source scripts/.env.runtime
fi

entry_point="${SILVER_ENTRY_POINT:-bronze_to_silver_job.py}"
name="${SILVER_JOB_NAME:-bronze-to-silver-dev}"
BATCH_LOGICAL_DATE="${BATCH_LOGICAL_DATE:-2036-01-01}"
PY_PACKAGE="${PY_PACKAGE:-aws_pipeline-0.0.1-py3-none-any.whl}"

# ─── STEP 1: Build wheel python pakage & upload job entry point ───────────────

read -r -p "Rebuild Python package? [y/N]: " _rebuild_input
_rebuild="${_rebuild_input:-N}"

cd "$ROOT_PATH"

if [[ "$_rebuild" =~ ^[Yy]$ ]]; then
  echo "Building the newest Python package..."
  poetry build --format wheel
  echo "Checking package..."
  ls -la "$ROOT_PATH/dist/"

  aws s3 cp "$ROOT_PATH/dist/$PY_PACKAGE" "s3://$CODEBASE_BUCKET_NAME/packages/$PY_PACKAGE"
else
  echo "Skipping package build."
fi

S3_PY_PACKAGE="s3://$CODEBASE_BUCKET_NAME/packages/$PY_PACKAGE"

aws s3 cp "$ROOT_PATH/jobs/$entry_point" "s3://$CODEBASE_BUCKET_NAME/jobs/$entry_point"
s3_entry_point="s3://$CODEBASE_BUCKET_NAME/jobs/$entry_point"

# ─── STEP 2: Start EMR Serverless job ───────────────
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
        "spark.submit.pyFiles": "$S3_PY_PACKAGE",
        "spark.emr-serverless.driverEnv.INPUT_PATH": "$DATALAKE_BUCKET_URI",
        "spark.emr-serverless.driverEnv.OUTPUT_PATH": "$DATALAKE_BUCKET_URI",
        "spark.emr-serverless.driverEnv.QUARANTINE_PATH": "$DATALAKE_BUCKET_URI",
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
  --application-id "$APPLICATION_ID" \
  --execution-role-arn "$EXECUTION_ROLE_ARN" \
  --name "$name" \
  --job-driver "$job_driver" \
  --configuration-overrides "$config_overrides"
