#!/bin/bash
set -euo pipefail

# ===========================================
# Starts the Silver -> Gold EMR Serverless job.
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
if [ -f scripts/.env ]; then
  source scripts/.env
fi

entry_point="${GOLD_ENTRY_POINT:-silver_to_gold_job.py}"
name="${GOLD_JOB_NAME:-silver-to-gold-dev}"

# Optional Silver -> Gold runtime controls.
# Leave DATASET empty to run all configured Gold dimensions/facts.
BATCH_LOGICAL_DATE="${BATCH_LOGICAL_DATE:-2036-01-01T00:00:00}"
DATASET="${DATASET:-}"
DATE_START="${DATE_START:-}"
DATE_END="${DATE_END:-}"
WINDOW_START="${WINDOW_START:-}"
WINDOW_END="${WINDOW_END:-}"
CURRENCIES="${CURRENCIES:-}"

prompt_with_default() {
  local prompt="$1"
  local current_value="$2"
  local user_input

  read -r -p "$prompt [$current_value]: " user_input
  printf '%s' "${user_input:-$current_value}"
}

prompt_optional() {
  local prompt="$1"
  local current_value="$2"
  local display_value="${current_value:-blank}"
  local user_input

  read -r -p "$prompt [$display_value]: " user_input
  printf '%s' "${user_input:-$current_value}"
}

echo "Configure Silver -> Gold runtime parameters..."
BATCH_LOGICAL_DATE="$(prompt_with_default "Batch logical date" "$BATCH_LOGICAL_DATE")"
DATASET="$(prompt_optional "Dataset name (blank = run all)" "$DATASET")"
DATE_START="$(prompt_optional "Date dimension start date, YYYY-MM-DD (blank = infer)" "$DATE_START")"
DATE_END="$(prompt_optional "Date dimension end date, YYYY-MM-DD (blank = infer)" "$DATE_END")"
WINDOW_START="$(prompt_optional "Dashboard window start, default. 2010-01-01T00:00:00 (blank = skip windowed facts)" "$WINDOW_START")"
WINDOW_END="$(prompt_optional "Dashboard window end, e.g. 2010-01-01T00:05:00 (blank = skip windowed facts)" "$WINDOW_END")"
CURRENCIES="$(prompt_optional "Currencies CSV, e.g. USD,EUR (blank = default)" "$CURRENCIES")"

# ─── STEP 1: Upload job entry point ───────────────
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

config_overrides=$(jq -n \
  --arg s3_py_package "$S3_PY_PACKAGE" \
  --arg datalake_bucket_uri "$DATALAKE_BUCKET_URI" \
  --arg batch_logical_date "$BATCH_LOGICAL_DATE" \
  --arg dataset "$DATASET" \
  --arg date_start "$DATE_START" \
  --arg date_end "$DATE_END" \
  --arg window_start "$WINDOW_START" \
  --arg window_end "$WINDOW_END" \
  --arg currencies "$CURRENCIES" \
  '
  def optional_property($key; $value):
    if $value == "" then {} else {($key): $value} end;

  {
    monitoringConfiguration: {
      cloudWatchLoggingConfiguration: {
        enabled: false
      }
    },
    applicationConfiguration: [
      {
        classification: "spark-defaults",
        properties: ({
          "spark.submit.pyFiles": $s3_py_package,
          "spark.emr-serverless.driverEnv.INPUT_PATH": $datalake_bucket_uri,
          "spark.emr-serverless.driverEnv.OUTPUT_PATH": $datalake_bucket_uri,
          "spark.emr-serverless.driverEnv.PYSPARK_DRIVER_PYTHON": "/home/hadoop/environment/bin/python",
          "spark.emr-serverless.driverEnv.PYSPARK_PYTHON": "/home/hadoop/environment/bin/python",
          "spark.emr-serverless.executorEnv.PYSPARK_PYTHON": "/home/hadoop/environment/bin/python",
          "spark.emr-serverless.driverEnv.BATCH_LOGICAL_DATE": $batch_logical_date,
          "spark.jars": "/usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar",
          "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
          "spark.sql.catalog.glue_catalog": "org.apache.iceberg.spark.SparkCatalog",
          "spark.sql.catalog.glue_catalog.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
          "spark.sql.catalog.glue_catalog.warehouse": ($datalake_bucket_uri + "/gold/iceberg/"),
          "spark.hadoop.hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
          "spark.driver.cores": "2",
          "spark.driver.memory": "8g",
          "spark.executor.cores": "2",
          "spark.executor.memory": "8g",
          "spark.dynamicAllocation.enabled": "true",
          "spark.dynamicAllocation.minExecutors": "1",
          "spark.dynamicAllocation.maxExecutors": "5"
        }
        + optional_property("spark.emr-serverless.driverEnv.DATASET"; $dataset)
        + optional_property("spark.emr-serverless.driverEnv.DATE_START"; $date_start)
        + optional_property("spark.emr-serverless.driverEnv.DATE_END"; $date_end)
        + optional_property("spark.emr-serverless.driverEnv.WINDOW_START"; $window_start)
        + optional_property("spark.emr-serverless.driverEnv.WINDOW_END"; $window_end)
        + optional_property("spark.emr-serverless.driverEnv.CURRENCIES"; $currencies))
      }
    ]
  }
  ')

aws emr-serverless start-job-run \
  --application-id "$APPLICATION_ID" \
  --execution-role-arn "$EXECUTION_ROLE_ARN" \
  --name "$name" \
  --job-driver "$job_driver" \
  --configuration-overrides "$config_overrides"
