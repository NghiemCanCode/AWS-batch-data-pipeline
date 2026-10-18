#!/usr/bin/env bash
# Step 11.1: push Airbyte host metrics to CloudWatch. Invoked every minute by /etc/cron.d/airbyte-metrics.
set -euo pipefail
export PATH=/usr/local/bin:/usr/bin:/bin     # cron only has /usr/bin:/bin; aws lives in /usr/local/bin
REGION=ap-southeast-1
NS=FDS/Airbyte
DIM='Dimensions=[{Name=Host,Value=finance-transaction-airbyte}]'

data_pct=$(df --output=pcent /root/.airbyte | tail -1 | tr -dc '0-9')
root_pct=$(df --output=pcent / | tail -1 | tr -dc '0-9')
if curl -fsS -m 10 http://localhost:8000/api/v1/health | grep -q '"available":true'; then unhealthy=0; else unhealthy=1; fi

aws cloudwatch put-metric-data --region "$REGION" --namespace "$NS" --metric-data \
  "MetricName=DataDiskUsedPercent,Unit=Percent,Value=$data_pct,$DIM" \
  "MetricName=RootDiskUsedPercent,Unit=Percent,Value=$root_pct,$DIM" \
  "MetricName=ApiUnhealthy,Unit=Count,Value=$unhealthy,$DIM"
