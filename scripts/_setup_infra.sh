#!/bin/bash
set -euo pipefail

# ===========================================
# Shared dev setup: provisions platform + compute infrastructure,
# seeds bronze data, builds/pushes the Spark image, and writes runtime vars.
# Run this once before running any job script (deploy_silver_job_dev.sh
# or deploy_gold_job_dev.sh). Outputs runtime variables to scripts/.env.runtime.
# ===========================================

# ─── STEP 0: Variables & environment ───────────────
ROOT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TR_PLATFORM_DIR="$ROOT_PATH/terraform/environments/dev/platform"
TR_COMPUTE_DIR="$ROOT_PATH/terraform/environments/dev/compute"
ECR_REPO_NAME="emr-serverless-custom"
GITHUB_REPO=$(gh repo view --json nameWithOwner --jq '.nameWithOwner')

cd "$ROOT_PATH"
if [ -f scripts/.env ]; then
  source scripts/.env
fi

AWS_REGION="${AWS_REGION:-ap-southeast-1}"
TF_BACKEND_BUCKET="${TF_BACKEND_BUCKET:-finance-transaction-tf-state-dev-0306}"
TF_BACKEND_REGION="${TF_BACKEND_REGION:-$AWS_REGION}"
IMAGE_TAG="${IMAGE_TAG:-latest}"

require_remote_backend_config() {
  if [ -z "${TF_BACKEND_BUCKET:-}" ]; then
    echo "Missing TF_BACKEND_BUCKET. Run terraform/bootstrap first, then export the state bucket name."
    exit 1
  fi
}

terraform_init_remote_backend() {
  local stack_key="$1"

  require_remote_backend_config
  terraform init \
    -input=false \
    -reconfigure \
    -backend-config="bucket=$TF_BACKEND_BUCKET" \
    -backend-config="key=$stack_key" \
    -backend-config="region=$TF_BACKEND_REGION" \
    -backend-config="use_lockfile=true" \
    -backend-config="encrypt=true"
}

# ─── STEP 1: Build platform infrastructure ───────────────
echo "Building platform infrastructure..."
cd "$TR_PLATFORM_DIR"
terraform_init_remote_backend "dev/platform/terraform.tfstate"
terraform apply -var="ecr_registry_name=$ECR_REPO_NAME" -var="github_repo=$GITHUB_REPO" -auto-approve

echo "Getting platform infrastructure output..."
datalake_bucket_name=$(terraform output -raw data_lake_bucket_name)
codebase_bucket_name=$(terraform output -raw code_bucket_name)
ecr_repository_url=$(terraform output -raw ecr_repository_url)
github_actions_role_arn=$(terraform output -raw github_actions_role_arn)

# If you use Github Codespace, you need create
echo "Setting GitHub Actions secret..."
gh secret set AWS_GITHUB_ACTIONS_ROLE_ARN \
  --repo "$GITHUB_REPO" \
  --body "$github_actions_role_arn"

# ─── STEP 2: Seed bronze data ───────────────
SEED_BUCKET="s3://central-dev-data-0703/finance-transaction/"
echo "Seeding bronze data from $SEED_BUCKET ..."
aws s3 sync "$SEED_BUCKET" "s3://$datalake_bucket_name/bronze/" --no-progress

# ─── STEP 3: Build Spark image & push to ECR (via GitHub Actions) ───────────────
full_ecr_image_uri="${ecr_repository_url%/}:$IMAGE_TAG"

echo "Triggering GitHub Actions workflow on repo: $GITHUB_REPO ..."

CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)

gh workflow run docker-build-push-ecr.yml \
  --repo "$GITHUB_REPO" \
  --ref "$CURRENT_BRANCH" \
  -f image_tag="$IMAGE_TAG"

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

# ─── STEP 4: Build compute infrastructure ───────────────
echo "Building compute infrastructure..."
cd "$TR_COMPUTE_DIR"
terraform_init_remote_backend "dev/compute/terraform.tfstate"
terraform apply \
  -var="custom_image_uri=$full_ecr_image_uri" \
  -var="data_lake_bucket_name=$datalake_bucket_name" \
  -var="code_bucket_name=$codebase_bucket_name" \
  -auto-approve


# ─── Write runtime variables ───────────────
bash "$ROOT_PATH/scripts/refresh_runtime_env_dev.sh"
echo "Setup complete. Run deploy_silver_job_dev.sh or deploy_gold_job_dev.sh to start an EMR job."
