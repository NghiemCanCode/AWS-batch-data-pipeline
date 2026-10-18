#!/usr/bin/env bash
# Step 7.2: install (or reconfigure) Airbyte with the pinned chart. Takes 10-20 minutes on the first run.
# Usage: sudo -H bash 071-airbyte-install.sh <storage_bucket>   (tf_out storage_bucket; safe to re-run)
# Normally started from the laptop by ops/install-airbyte.sh, which runs it in the background and streams progress.
set -euo pipefail
BUCKET=${1:?missing storage bucket (tf_out storage_bucket)}
REGION=ap-southeast-1
E=/opt/fds/airbyte/host/etc/airbyte
VALUES=/etc/airbyte/values.yaml

source $E/versions.env
case "$AIRBYTE_CHART_VERSION" in *pin-me*) echo "Set AIRBYTE_CHART_VERSION in versions.env first"; exit 1 ;; esac
mountpoint -q /root/.airbyte || { echo "/root/.airbyte not mounted: run 052 first"; exit 1; }
[ "$HOME" = /root ] || { echo "Run with sudo -H so that abctl uses /root/.airbyte"; exit 1; }

# The instance profile must reach the storage bucket before Airbyte starts, otherwise the server pod crash-loops
aws s3 ls "s3://$BUCKET" --region "$REGION" >/dev/null || { echo "Cannot list s3://$BUCKET: check IAM (step 3.1)"; exit 1; }

install -d -m 0755 /etc/airbyte
sed -e "s|__STORAGE_BUCKET__|$BUCKET|g" -e "s|__REGION__|$REGION|g" "$E/values.yaml.tpl" > "$VALUES"

abctl local install \
  --chart-version "$AIRBYTE_CHART_VERSION" \
  --values "$VALUES" \
  --no-browser

abctl local status
curl -fsS http://localhost:8000/api/v1/health && echo
