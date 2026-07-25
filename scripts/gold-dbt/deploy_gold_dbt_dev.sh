#!/bin/bash
set -euo pipefail

# ==============================================================================
# Starts the Silver -> Gold EMR Serverless with dbt.
#
# Operating manual — why each step looks the way it does, every warning, and the
# run history: scripts/gold-dbt/README.md. Section numbers below point into it.
# All dbt commands are commented out by default: uncomment the block you want.
# ==============================================================================

log() {
  printf '%(%H:%M:%S)T  %s\n' -1 "$1"
}

# ==============================================================================
# STEP 0: Variables & environment — README.md §2
# ==============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_PATH="$(cd "$SCRIPT_DIR/../.." && pwd)"
RUNTIME_ENV="$ROOT_PATH/scripts/.env.runtime"
SQL_DIR="$SCRIPT_DIR/sql"

if [ ! -f "$RUNTIME_ENV" ]; then
  log "Error: $RUNTIME_ENV not found. Run _dev_setup.sh first."
  exit 1
fi

source "$RUNTIME_ENV"

# ==============================================================================
# STEP 0b: Ensure the Glue "default" database exists — README.md §3
# ==============================================================================

if aws glue get-database --name default >/dev/null 2>&1; then
  log "Glue database 'default' already exists. Skipping."
else
  log "Creating Glue database 'default' (required by Spark's Hive support)..."
  aws glue create-database --database-input '{"Name":"default"}'
fi

# ==============================================================================
# STEP 0c: Glue grants that live outside Terraform — README.md §4
#   FinanceSilverRead              — temporary, dies with the silver->Iceberg migration
#   DbtSnapshotStagingViewInDefaultDb — permanent by decision, NOT going into Terraform
# ==============================================================================

AWS_REGION="${AWS_REGION:-ap-southeast-1}"
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
ROLE_NAME="${EXECUTION_ROLE_ARN##*/}"
TEMP_POLICY_NAME="TEMP-finance-silver-read"

log "Attaching temporary inline policy '$TEMP_POLICY_NAME' to $ROLE_NAME..."
TEMP_POLICY_DOC=$(cat <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "FinanceSilverRead",
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
    },
    {
      "Sid": "DbtSnapshotStagingViewInDefaultDb",
      "Effect": "Allow",
      "Action": [
        "glue:GetDatabase",
        "glue:GetTable",
        "glue:GetTables",
        "glue:CreateTable",
        "glue:UpdateTable",
        "glue:DeleteTable",
        "glue:GetPartition",
        "glue:GetPartitions",
        "glue:BatchGetPartition"
      ],
      "Resource": [
        "arn:aws:glue:${AWS_REGION}:${AWS_ACCOUNT_ID}:catalog",
        "arn:aws:glue:${AWS_REGION}:${AWS_ACCOUNT_ID}:database/default",
        "arn:aws:glue:${AWS_REGION}:${AWS_ACCOUNT_ID}:table/default/*"
      ]
    }
  ]
}
EOF
)
aws iam put-role-policy \
  --role-name "$ROLE_NAME" \
  --policy-name "$TEMP_POLICY_NAME" \
  --policy-document "$TEMP_POLICY_DOC"

log "Waiting for IAM policy propagation..."
sleep 5

# ==============================================================================
# STEP 1: Create spark session — README.md §5
# ==============================================================================

UPDATE_RESULT=$(aws emr-serverless update-application \
  --application-id "$APPLICATION_ID" \
  --interactive-configuration sessionEnabled=true \
  --query 'application.{applicationId:applicationId,sessionEnabled:interactiveConfiguration.sessionEnabled}' \
  --output text
)
log "Application updated: $UPDATE_RESULT"

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

# ==============================================================================
# Cleanup trap — README.md §6
# ==============================================================================

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

# ==============================================================================
# STEP 2: Get SPARK_REMOTE_URL — README.md §7
# ==============================================================================

SESSION_ENDPOINT_JSON=$(aws emr-serverless get-session-endpoint \
  --application-id "$APPLICATION_ID" \
  --session-id "$SESSION_ID")

ENDPOINT=$(echo "$SESSION_ENDPOINT_JSON" | jq -r '.endpoint')
TOKEN=$(echo "$SESSION_ENDPOINT_JSON" | jq -r '.authToken')

URL="sc://${ENDPOINT#https://}:443/;use_ssl=true;x-aws-proxy-auth=${TOKEN}"

DBT_PROJECT_DIR="$ROOT_PATH/dbt"
export SPARK_REMOTE="$URL"

# ==============================================================================
# STEP 3: Steady-state commands — README.md §8
# ==============================================================================

# poetry run dbt seed --project-dir "$DBT_PROJECT_DIR" --select us_holidays

# poetry run dbt run --project-dir "$DBT_PROJECT_DIR" --select dim_date
# poetry run dbt run --project-dir "$DBT_PROJECT_DIR" --select dim_times

# poetry run dbt build --project-dir "$DBT_PROJECT_DIR" \
#   --select stg_geo dim_geo

# poetry run dbt build --project-dir "$DBT_PROJECT_DIR" \
#   --select stg_customers snapshot_customers dim_customers

# poetry run dbt build --project-dir "$DBT_PROJECT_DIR" \
#   --select stg_cards snapshot_cards dim_cards

# poetry run dbt build --project-dir "$DBT_PROJECT_DIR" \
#   --select stg_mcc dim_merchant

# poetry run dbt build --project-dir "$DBT_PROJECT_DIR" \
#   --select stg_transactions fact_transactions trans_error_bridge

# poetry run dbt build --project-dir "$DBT_PROJECT_DIR" \
#   --select fact_user_monthly_snapshot

# card_owner_factless — README.md §8.2 (moved to STEP 7, see §14)
# poetry run dbt build --project-dir "$DBT_PROJECT_DIR" \
#   --select card_owner_factless

# The two incremental facts, steady state — README.md §8.1
# poetry run dbt build --project-dir "$DBT_PROJECT_DIR" \
#   --select fact_daily_transaction_trend \
#   --vars '{batch_logical_date: 2026-07-23}'

# poetry run dbt build --project-dir "$DBT_PROJECT_DIR" \
#   --select fact_customer_activity_daily \
#   --vars '{batch_logical_date: 2026-07-23}'

# ==============================================================================
# STEP 4: Point-in-time chain, two stages — README.md §9
# ==============================================================================

# Stage 4a — prerequisites (README.md §9.1)
# poetry run dbt build --project-dir "$DBT_PROJECT_DIR" --indirect-selection cautious \
#   --select us_holidays dim_dates dim_times stg_geo dim_geo stg_mcc dim_merchant \
#            stg_customers stg_cards snapshot_customers snapshot_cards

# Stage 4b — restate the chain (README.md §9.2)
# poetry run dbt build --project-dir "$DBT_PROJECT_DIR" --full-refresh --indirect-selection cautious \
#   --select stg_transactions dim_customers dim_cards fact_transactions \
#            trans_error_bridge fact_user_monthly_snapshot

# ==============================================================================
# STEP 5: First build of fact_daily_transaction_trend — README.md §10
# ==============================================================================

# poetry run dbt build --project-dir "$DBT_PROJECT_DIR" --full-refresh \
#   --select fact_daily_transaction_trend

# ==============================================================================
# STEP 6: First build of fact_customer_activity_daily — README.md §11
# ==============================================================================

# poetry run dbt build --project-dir "$DBT_PROJECT_DIR" --full-refresh \
#   --select fact_customer_activity_daily

# ==============================================================================
# STEP 6b: Incremental smoke test — README.md §12
# ==============================================================================

# poetry run dbt show --project-dir "$DBT_PROJECT_DIR" \
#   --inline "$(cat "$SQL_DIR/fact_transactions_date_bounds.sql")" \
#   || true

# log "STEP 6b — fingerprint BEFORE incremental run 1 (baseline = STEP 6 full refresh):"
# poetry run dbt show --project-dir "$DBT_PROJECT_DIR" \
#   --inline "$(cat "$SQL_DIR/activity_partition_fingerprint.sql")"

# poetry run dbt build --project-dir "$DBT_PROJECT_DIR" \
#   --select fact_customer_activity_daily \
#   --vars '{batch_logical_date: 2019-10-31}'

# log "STEP 6b — fingerprint after incremental run 1 (must equal baseline):"
# poetry run dbt show --project-dir "$DBT_PROJECT_DIR" \
#   --inline "$(cat "$SQL_DIR/activity_partition_fingerprint.sql")"

# poetry run dbt build --project-dir "$DBT_PROJECT_DIR" \
#   --select fact_customer_activity_daily \
#   --vars '{batch_logical_date: 2019-10-31}'

# log "STEP 6b — fingerprint after incremental run 2 (idempotency, must equal run 1):"
# poetry run dbt show --project-dir "$DBT_PROJECT_DIR" \
#   --inline "$(cat "$SQL_DIR/activity_partition_fingerprint.sql")"

# Steady-state incremental run. NEVER pass 2036-01-01 — README.md §17.1
# poetry run dbt build --project-dir "$DBT_PROJECT_DIR" \
#   --select fact_customer_activity_daily \
#   --vars '{batch_logical_date: <max data day, yyyy-mm-dd>}'

# ==============================================================================
# STEP 6c: First build + incremental smoke test of rpt_card_portfolio
#          — README.md §13
# ==============================================================================

# Full refresh WITH --vars, unlike STEP 5 / STEP 6 — README.md §13 explains why:
# one of this model's singular tests recomputes from dim_cards, which has
# versions in effect on every date including today, so the macro's
# current_date() default would compare an empty partition against the whole card
# portfolio and fail a run that did nothing wrong.
# poetry run dbt build --project-dir "$DBT_PROJECT_DIR" --full-refresh \
#   --select rpt_card_portfolio \
#   --vars '{batch_logical_date: 2019-10-31}'

# log "STEP 6c — fingerprint BEFORE incremental run 1 (baseline = full refresh):"
# poetry run dbt show --project-dir "$DBT_PROJECT_DIR" \
#   --inline "$(cat "$SQL_DIR/card_portfolio_partition_fingerprint.sql")"

# poetry run dbt build --project-dir "$DBT_PROJECT_DIR" \
#   --select rpt_card_portfolio \
#   --vars '{batch_logical_date: 2019-10-31}'

# log "STEP 6c — fingerprint after incremental run 1 (must equal baseline):"
# poetry run dbt show --project-dir "$DBT_PROJECT_DIR" \
#   --inline "$(cat "$SQL_DIR/card_portfolio_partition_fingerprint.sql")"

# poetry run dbt build --project-dir "$DBT_PROJECT_DIR" \
#   --select rpt_card_portfolio \
#   --vars '{batch_logical_date: 2019-10-31}'

# log "STEP 6c — fingerprint after incremental run 2 (idempotency, must equal run 1):"
# poetry run dbt show --project-dir "$DBT_PROJECT_DIR" \
#   --inline "$(cat "$SQL_DIR/card_portfolio_partition_fingerprint.sql")"

# Diagnostic, only worth running once: the delta term of the Active Card
# cross-check. Spec Open Question #4 expects 0. Not a gate — the singular test
# above is the gate; this just tells you the number when it isn't 0.
# log "STEP 6c — Active Card cross-check terms (residual must be 0, delta expected 0):"
# poetry run dbt show --project-dir "$DBT_PROJECT_DIR" \
#   --inline "$(cat "$SQL_DIR/card_portfolio_cross_check_delta.sql")" \
#   --vars '{batch_logical_date: 2019-10-31}' \
#   || true

# Steady-state incremental run. NEVER pass 2036-01-01 — README.md §17.1
# poetry run dbt build --project-dir "$DBT_PROJECT_DIR" \
#   --select rpt_card_portfolio \
#   --vars '{batch_logical_date: <max data day, yyyy-mm-dd>}'

# ==============================================================================
# STEP 6d: First build + incremental smoke test of rpt_merchant_error_daily
#          — README.md §13.1
# ==============================================================================

# Depends on STEP 5, not on STEP 6: fact_daily_transaction_trend is this model's
# ONLY source (spec section 4). It does not need fact_customer_activity_daily or
# rpt_card_portfolio at all — the ordering after 6c is convenience, not a
# dependency.

# Full refresh WITH --vars, same as STEP 6c but for a different reason —
# README.md §13.1. The var changes nothing about the BUILD (is_incremental() is
# false on a full refresh, so the model never calls the macro); it decides which
# day the TESTS look at. Without it the macro falls back to current_date(),
# which against 2019 data selects an empty partition, and the reconciliation
# test then compares 0 against 0 and passes having checked nothing. That is the
# single most important test of this table, so letting it run vacuously is worse
# than not running it.
poetry run dbt build --project-dir "$DBT_PROJECT_DIR" --full-refresh \
  --select rpt_merchant_error_daily \
  --vars '{batch_logical_date: 2019-10-31}'

log "STEP 6d — fingerprint BEFORE incremental run 1 (baseline = full refresh):"
poetry run dbt show --project-dir "$DBT_PROJECT_DIR" \
  --inline "$(cat "$SQL_DIR/merchant_error_partition_fingerprint.sql")"

poetry run dbt build --project-dir "$DBT_PROJECT_DIR" \
  --select rpt_merchant_error_daily \
  --vars '{batch_logical_date: 2019-10-31}'

log "STEP 6d — fingerprint after incremental run 1 (must equal baseline):"
poetry run dbt show --project-dir "$DBT_PROJECT_DIR" \
  --inline "$(cat "$SQL_DIR/merchant_error_partition_fingerprint.sql")"

poetry run dbt build --project-dir "$DBT_PROJECT_DIR" \
  --select rpt_merchant_error_daily \
  --vars '{batch_logical_date: 2019-10-31}'

log "STEP 6d — fingerprint after incremental run 2 (idempotency, must equal run 1):"
poetry run dbt show --project-dir "$DBT_PROJECT_DIR" \
  --inline "$(cat "$SQL_DIR/merchant_error_partition_fingerprint.sql")"

# The numbers to READ, not just to diff — README.md §13.1 has the table. Green on
# every test still leaves the possibility that the parameters no longer fit the
# data, which is what the warn-severity band test is for; and baseline_min /
# baseline_max drifting far from 0.016090 is the signature of a wrong mcc
# aggregation, the fault this whole table is most exposed to (spec section 6).

# Steady-state incremental run. NEVER pass 2036-01-01 — README.md §17.1
# poetry run dbt build --project-dir "$DBT_PROJECT_DIR" \
#   --select rpt_merchant_error_daily \
#   --vars '{batch_logical_date: <max data day, yyyy-mm-dd>}'

# ==============================================================================
# STEP 7: Card owner factless, LAST — README.md §14 (model docs: §8.2)
# ==============================================================================

# poetry run dbt build --project-dir "$DBT_PROJECT_DIR" \
#   --select card_owner_factless

# ==============================================================================
# STEP 8: Snapshot stability check — README.md §15
# ==============================================================================

# log "STEP 8 — snapshot state BEFORE re-run:"
# poetry run dbt show --project-dir "$DBT_PROJECT_DIR" \
#   --inline "$(cat "$SQL_DIR/snapshot_stability.sql")"

# poetry run dbt snapshot --project-dir "$DBT_PROJECT_DIR" \
#   --select snapshot_customers snapshot_cards

# log "STEP 8 — snapshot state AFTER re-run (must be identical to BEFORE):"
# poetry run dbt show --project-dir "$DBT_PROJECT_DIR" \
#   --inline "$(cat "$SQL_DIR/snapshot_stability.sql")"

# ==============================================================================
# STEP 9: Calibrate the Abnormal Error Rate parameters — README.md §16
# ==============================================================================

# log "STEP 9 measure 1 — portfolio baseline error rate:"
# poetry run dbt show --project-dir "$DBT_PROJECT_DIR" \
#   --inline "$(cat "$SQL_DIR/calibrate_abnormal_error_rate_1_baseline.sql")"

# log "STEP 9 measure 2 — merchant volume distribution and floor cost:"
# poetry run dbt show --project-dir "$DBT_PROJECT_DIR" \
#   --inline "$(cat "$SQL_DIR/calibrate_abnormal_error_rate_2_volume_distribution.sql")"

# log "STEP 9 measure 3 — error rate distribution above the volume floor:"
# poetry run dbt show --project-dir "$DBT_PROJECT_DIR" \
#   --inline "$(cat "$SQL_DIR/calibrate_abnormal_error_rate_3_error_distribution.sql")"

# log "STEP 9 measure 4 — merchants flagged at each candidate threshold:"
# poetry run dbt show --project-dir "$DBT_PROJECT_DIR" \
#   --inline "$(cat "$SQL_DIR/calibrate_abnormal_error_rate_4_threshold_flags.sql")"

# log "STEP 9 measure 4b — top 20 merchants by error rate (diagnostic):"
# poetry run dbt show --project-dir "$DBT_PROJECT_DIR" --limit 20 \
#   --inline "$(cat "$SQL_DIR/calibrate_abnormal_error_rate_4b_top_merchants.sql")"

# log "STEP 9 measure 5 — is there any merchant-level signal at all (dispersion):"
# poetry run dbt show --project-dir "$DBT_PROJECT_DIR" \
#   --inline "$(cat "$SQL_DIR/calibrate_abnormal_error_rate_5_dispersion.sql")"
