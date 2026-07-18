#!/bin/bash
set -euo pipefail

# ===========================================
# Starts the Silver -> Gold EMR Serverless with dbt.
# ===========================================

log() {
  printf '%(%H:%M:%S)T  %s\n' -1 "$1"
}

# ────── STEP 0: Variables & environment ───────────────

ROOT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUNTIME_ENV="$ROOT_PATH/scripts/.env.runtime"

if [ ! -f "$RUNTIME_ENV" ]; then
  log "Error: $RUNTIME_ENV not found. Run _dev_setup.sh first."
  exit 1
fi

source "$RUNTIME_ENV"

# As of July 16, 2026, Terraform does not yet support the session_enabled parameter.
aws emr-serverless update-application \
  --application-id "$APPLICATION_ID" \
  --interactive-configuration sessionEnabled=true

CONFIGURATION_OVERRIDES=$(cat <<'EOF'
{
  "runtimeConfiguration": [
    {
      "classification": "spark-defaults",
      "properties": {
        "spark.jars": "/usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar",
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.catalog.glue_dev": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.glue_dev.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
        "spark.sql.catalog.glue_dev.warehouse": "s3://finance-transaction-datalake-dev/gold/iceberg/",
        "spark.sql.defaultCatalog": "glue_dev",
        "spark.hadoop.hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
      }
    }
  ]
}
EOF
)

# ────── STEP 1: Create spark session ───────────────
SESSION_ID=$(aws emr-serverless start-session \
    --application-id "$APPLICATION_ID" \
    --execution-role-arn "$EXECUTION_ROLE_ARN" \
    --configuration-overrides "$CONFIGURATION_OVERRIDES" \
    --query 'sessionId' \
    --output text
)

log "Session $SESSION_ID starting."

while true; do
  SESSION_STATE=$(aws emr-serverless get-session \
    --application-id "$APPLICATION_ID" \
    --session-id "$SESSION_ID" \
    --query 'session.state' \
    --output text)

  log "Session state: $SESSION_STATE"

  case "$SESSION_STATE" in
    STARTED|IDLE)
      break
      ;;
    FAILED|TERMINATED)
      STATE_DETAILS=$(aws emr-serverless get-session \
        --application-id "$APPLICATION_ID" \
        --session-id "$SESSION_ID" \
        --query 'session.stateDetails' \
        --output text)
      log "Session failed: $STATE_DETAILS" >&2
      exit 1
      ;;
  esac

  sleep 5
done

# Session is up: from here on, always terminate it on exit (success,
# failure, or interrupt) regardless of what happens in the steps below.
terminate_session() {
  aws emr-serverless terminate-session \
      --application-id "$APPLICATION_ID" \
      --session-id "$SESSION_ID"
  log "Session $SESSION_ID terminated."
}
trap terminate_session EXIT

# ────── STEP 2: Get SPARK_REMOTE_URL ───────────────
SESSION_ENDPOINT_JSON=$(aws emr-serverless get-session-endpoint \
  --application-id "$APPLICATION_ID" \
  --session-id "$SESSION_ID")

ENDPOINT=$(echo "$SESSION_ENDPOINT_JSON" | jq -r '.endpoint')
TOKEN=$(echo "$SESSION_ENDPOINT_JSON" | jq -r '.authToken')

URL="sc://${ENDPOINT#https://}:443/;use_ssl=true;x-aws-proxy-auth=${TOKEN}"

DBT_PROJECT_DIR="$ROOT_PATH/dbt"
export SPARK_REMOTE="$URL"

# ────── STEP 3: Run static dimension ───────────────

poetry run dbt seed --project-dir "$DBT_PROJECT_DIR" --select us_holidays

poetry run dbt run --project-dir "$DBT_PROJECT_DIR" --select dim_date
poetry run dbt run --project-dir "$DBT_PROJECT_DIR" --select dim_time
