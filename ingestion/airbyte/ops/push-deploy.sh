#!/usr/bin/env bash
# Step 4.5: push the files the Airbyte host needs to s3://<ops>/deploy/. Runs from any directory.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../lib/common.sh"

aws s3 sync "$AB_DIR/" "s3://$(tf_out ops_bucket)/deploy/" --delete \
  --exclude '*' --include 'host/*' --include 'tests/*'
