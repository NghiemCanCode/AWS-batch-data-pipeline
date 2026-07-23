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

# ────── STEP 0b: Ensure the Glue "default" database exists ───────────────
# Spark's Hive support (spark.sql.catalogImplementation=hive, needed below so
# SparkSessionCatalog can fall back to Glue for non-Iceberg tables) always probes a
# database named "default" on startup — a Hive convention, unrelated to any database
# this pipeline actually uses. A plain Glue Data Catalog has no such database by default
# (unlike a real Hive metastore, which auto-creates it), so without this the session
# fails at boot with "not authorized to perform: glue:GetDatabase on resource:
# database/default" (or CreateDatabase, if the execution role isn't allowed to create
# it).
# Creating it once, here, keeps the execution role read-only.
if aws glue get-database --name default >/dev/null 2>&1; then
  log "Glue database 'default' already exists. Skipping."
else
  log "Creating Glue database 'default' (required by Spark's Hive support)..."
  aws glue create-database --database-input '{"Name":"default"}'
fi

# ────── STEP 0c: Ensure execution role can read finance_silver (temporary) ───────────────
# finance_silver is a Glue-Crawler-created database (scripts/glue_crawler.sh), not yet
# covered by terraform/environments/dev/compute/iam.tf. Grant read-only Glue access via
# a temporary inline policy on the execution role until that Terraform change is applied
# for real.
AWS_REGION="${AWS_REGION:-ap-southeast-1}"
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
ROLE_NAME="${EXECUTION_ROLE_ARN##*/}"
TEMP_POLICY_NAME="TEMP-finance-silver-read"

# put-role-policy always overwrites in place, so just call it unconditionally
# every run instead of checking first — avoids silently keeping a stale
# policy body around if a previous run's cleanup trap never got to run.
log "Attaching temporary inline policy '$TEMP_POLICY_NAME' to $ROLE_NAME..."
FINANCE_SILVER_POLICY=$(cat <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "glue:GetDatabase",
        "glue:GetTable",
        "glue:GetTables",
        "glue:GetPartition",
        "glue:GetPartitions"
      ],
      "Resource": [
        "arn:aws:glue:${AWS_REGION}:${AWS_ACCOUNT_ID}:catalog",
        "arn:aws:glue:${AWS_REGION}:${AWS_ACCOUNT_ID}:database/default",
        "arn:aws:glue:${AWS_REGION}:${AWS_ACCOUNT_ID}:database/finance_silver",
        "arn:aws:glue:${AWS_REGION}:${AWS_ACCOUNT_ID}:table/finance_silver/*"
      ]
    }
  ]
}
EOF
)
aws iam put-role-policy \
  --role-name "$ROLE_NAME" \
  --policy-name "$TEMP_POLICY_NAME" \
  --policy-document "$FINANCE_SILVER_POLICY"

log "Waiting for IAM policy propagation..."
sleep 5

# As of July 16, 2026, Terraform does not yet support the session_enabled parameter.
UPDATE_RESULT=$(aws emr-serverless update-application \
  --application-id "$APPLICATION_ID" \
  --interactive-configuration sessionEnabled=true \
  --query 'application.{applicationId:applicationId,sessionEnabled:interactiveConfiguration.sessionEnabled}' \
  --output text
)
log "Application updated: $UPDATE_RESULT"

# spark_catalog is set to SparkSessionCatalog (not SparkCatalog): SparkCatalog only
# resolves Iceberg tables, so it can't see the non-Iceberg Silver tables from the Glue
# Crawler. SparkSessionCatalog falls back to the Hive/Glue metastore for those, letting
# Silver (Hive) and Gold (Iceberg) both resolve under the same default catalog.
CONFIGURATION_OVERRIDES=$(cat <<'EOF'
{
  "runtimeConfiguration": [
    {
      "classification": "spark-defaults",
      "properties": {
        "spark.jars": "/usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar",
        "spark.sql.catalogImplementation": "hive",
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.catalog.spark_catalog": "org.apache.iceberg.spark.SparkSessionCatalog",
        "spark.sql.catalog.spark_catalog.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
        "spark.sql.catalog.spark_catalog.warehouse": "s3://finance-transaction-datalake-dev/gold/iceberg/",
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

# Session is up: from here on, always terminate it and revoke the temporary
# finance_silver policy on exit (success, failure, or interrupt) regardless of
# what happens in the steps below.
cleanup() {
  aws emr-serverless terminate-session \
      --application-id "$APPLICATION_ID" \
      --session-id "$SESSION_ID"
  log "Session $SESSION_ID terminated."

  aws iam delete-role-policy \
      --role-name "$ROLE_NAME" \
      --policy-name "$TEMP_POLICY_NAME" 2>/dev/null || true
  log "Temporary inline policy '$TEMP_POLICY_NAME' removed from $ROLE_NAME."

  aws emr-serverless stop-application \
      --application-id "$APPLICATION_ID"
  log "Application $APPLICATION_ID stopped."
}
trap cleanup EXIT

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
# All commands below are commented out. Uncomment the one you want to run.

# poetry run dbt seed --project-dir "$DBT_PROJECT_DIR" --select us_holidays

# poetry run dbt run --project-dir "$DBT_PROJECT_DIR" --select dim_date
# poetry run dbt run --project-dir "$DBT_PROJECT_DIR" --select dim_time

# poetry run dbt build --project-dir "$DBT_PROJECT_DIR" \
#   --select stg_geo dim_geo

# poetry run dbt build --project-dir "$DBT_PROJECT_DIR" \
#   --select stg_customers snapshot_customers dim_customers

# poetry run dbt build --project-dir "$DBT_PROJECT_DIR" \
#   --select stg_cards snapshot_cards dim_cards

# poetry run dbt build --project-dir "$DBT_PROJECT_DIR" \
#   --select stg_mcc dim_merchant

