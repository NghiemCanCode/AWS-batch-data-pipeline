#!/usr/bin/env bash
# Step 12.1: dump Airbyte's internal database to s3://<ops>/backup/. Run as root on the Airbyte host.
# Usage: airbyte-db-backup.sh <ops_bucket>
set -euo pipefail
BUCKET=${1:?missing ops bucket}
NODE=airbyte-abctl-control-plane
K="docker exec $NODE kubectl --kubeconfig /etc/kubernetes/admin.conf -n airbyte-abctl"
KEY="backup/airbyte-db/$(date -u +%Y%m%dT%H%M%SZ).dump"

# Custom format: compressed, restorable table by table with pg_restore.
# Pod, user and database are the chart defaults: check them once (deploy.md, step 12.1).
$K exec airbyte-db-0 -- pg_dump -U airbyte -d db-airbyte -Fc \
  | aws s3 cp - "s3://$BUCKET/$KEY" --region ap-southeast-1
aws s3 ls "s3://$BUCKET/$KEY" --region ap-southeast-1
