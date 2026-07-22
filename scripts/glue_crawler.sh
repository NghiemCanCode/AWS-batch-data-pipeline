#!/bin/bash
set -euo pipefail

# ===========================================
# The Silver layer has not yet been migrated to Iceberg, which makes dbt difficult to use.
# Use a Glue Crawler until the Silver layer is migrated to Iceberg.
# Provisioned via AWS CLI (not Terraform) since this is temporary infra —
# drop this script once the Silver layer migrates to Iceberg.
# ===========================================

# ─── STEP 0: Variables & environment ───────────────
ROOT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUNTIME_ENV="$ROOT_PATH/scripts/.env.runtime"
AWS_REGION="${AWS_REGION:-ap-southeast-1}"

if [ ! -f "$RUNTIME_ENV" ]; then
  echo "Error: $RUNTIME_ENV not found. Run _dev_setup.sh first."
  exit 1
fi

source "$RUNTIME_ENV"

DEFAULT_GLUE_CRAWLER_NAME="finance_silver_crawler"
read -r -p "Glue Crawler name to create [$DEFAULT_GLUE_CRAWLER_NAME]: " GLUE_CRAWLER_NAME
GLUE_CRAWLER_NAME="${GLUE_CRAWLER_NAME:-$DEFAULT_GLUE_CRAWLER_NAME}"

if [[ -z "$GLUE_CRAWLER_NAME" ]]; then
  echo "Glue Crawler name cannot be empty." >&2
  exit 1
fi

DEFAULT_GLUE_DATABASE_NAME="finance_silver"
read -r -p "Glue database name for the crawler to publish into [$DEFAULT_GLUE_DATABASE_NAME]: " GLUE_DATABASE_NAME
GLUE_DATABASE_NAME="${GLUE_DATABASE_NAME:-$DEFAULT_GLUE_DATABASE_NAME}"

DEFAULT_IAM_ROLE_NAME="GlueCrawlerRoleDev"
read -r -p "IAM role name to create for the crawler [$DEFAULT_IAM_ROLE_NAME]: " IAM_ROLE_NAME
IAM_ROLE_NAME="${IAM_ROLE_NAME:-$DEFAULT_IAM_ROLE_NAME}"

DEFAULT_GLUE_TABLE_PREFIX="silver_"
read -r -p "Table name prefix for crawled tables [$DEFAULT_GLUE_TABLE_PREFIX]: " GLUE_TABLE_PREFIX
GLUE_TABLE_PREFIX="${GLUE_TABLE_PREFIX:-$DEFAULT_GLUE_TABLE_PREFIX}"

SILVER_S3_PATH="s3://$DATALAKE_BUCKET_NAME/silver/"
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

# ─── STEP 1: Create Glue Crawler (if it does not exist) ───────
if aws glue get-crawler --name "$GLUE_CRAWLER_NAME" >/dev/null 2>&1; then
  echo "Glue Crawler '$GLUE_CRAWLER_NAME' already exists. Skipping to Step 2."
else

# ─── STEP A: Create IAM role for the crawler (if it does not exist) ───────
  # Brand-new, least-privilege role: scoped to the Silver prefix and the target Glue database only — no reuse 
  # of the EMR execution role.
  if aws iam get-role --role-name "$IAM_ROLE_NAME" >/dev/null 2>&1; then
    echo "IAM role '$IAM_ROLE_NAME' already exists. Skipping creation."
  else
    echo "Creating IAM role '$IAM_ROLE_NAME'..."

    POLICIES_DIR="$ROOT_PATH/scripts/policies"
    rendered_permissions_policy="$(mktemp)"
    trap 'rm -f "$rendered_permissions_policy"' EXIT

    aws iam create-role \
      --role-name "$IAM_ROLE_NAME" \
      --assume-role-policy-document "file://$POLICIES_DIR/glue_crawler_trust_policy.json" \
      --description "Least-privilege role for the temporary Silver-layer Glue Crawler" \
      >/dev/null

    DATALAKE_BUCKET_NAME="$DATALAKE_BUCKET_NAME" \
    AWS_REGION="$AWS_REGION" \
    AWS_ACCOUNT_ID="$AWS_ACCOUNT_ID" \
    GLUE_DATABASE_NAME="$GLUE_DATABASE_NAME" \
      envsubst '${DATALAKE_BUCKET_NAME} ${AWS_REGION} ${AWS_ACCOUNT_ID} ${GLUE_DATABASE_NAME}' \
      < "$POLICIES_DIR/glue_crawler_permissions_policy.json.tpl" \
      > "$rendered_permissions_policy"

    aws iam put-role-policy \
      --role-name "$IAM_ROLE_NAME" \
      --policy-name "${IAM_ROLE_NAME}-least-privilege" \
      --policy-document "file://$rendered_permissions_policy"

    rm -f "$rendered_permissions_policy"
    trap - EXIT

    echo "Waiting for IAM role propagation..."
    sleep 10
  fi

  GLUE_CRAWLER_ROLE_ARN=$(aws iam get-role --role-name "$IAM_ROLE_NAME" --query 'Role.Arn' --output text)

  # ─── STEP B: Configure the Glue database and table prefix ───────
  # The crawler's --database-name must already exist, so ensure it here first.
  if aws glue get-database --name "$GLUE_DATABASE_NAME" >/dev/null 2>&1; then
    echo "Glue database '$GLUE_DATABASE_NAME' already exists. Skipping creation."
  else
    echo "Creating Glue database '$GLUE_DATABASE_NAME'..."
    aws glue create-database --database-input "{\"Name\":\"$GLUE_DATABASE_NAME\"}"
  fi

  # ─── STEP C: Create Crawler ───────────────────────
  aws glue create-crawler \
    --name "$GLUE_CRAWLER_NAME" \
    --role "$GLUE_CRAWLER_ROLE_ARN" \
    --database-name "$GLUE_DATABASE_NAME" \
    --table-prefix "$GLUE_TABLE_PREFIX" \
    --targets "{\"S3Targets\":[{\"Path\":\"$SILVER_S3_PATH\"}]}"
fi

# ─── STEP 2: Run Crawler ───────────────────────────────────────
