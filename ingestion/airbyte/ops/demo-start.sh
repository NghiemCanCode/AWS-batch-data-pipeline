#!/usr/bin/env bash
# Step 13.2. Start the source first (its own demo-start.sh), then this.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../lib/common.sh"
AB_ID=$(tf_out instance_id)

echo ">> Start Airbyte"
aws ec2 start-instances --instance-ids "$AB_ID" >/dev/null
aws ec2 wait instance-running --instance-ids "$AB_ID"

echo ">> Waiting for the SSM agent"
wait_ssm_online "$AB_ID"

echo ">> Health check (kind and every Airbyte pod come back after a reboot; allow up to 10 minutes)"
run_remote "$AB_ID" '
set -e
mountpoint -q /root/.airbyte || { echo "/root/.airbyte NOT MOUNTED"; exit 1; }
N=airbyte-abctl-control-plane
[ "$(docker inspect -f "{{.State.Running}}" $N)" = true ] || docker start $N
for i in $(seq 1 60); do
  curl -fsS -m 5 http://localhost:8000/api/v1/health | grep -q "\"available\":true" && break
  sleep 10
done
curl -fsS http://localhost:8000/api/v1/health; echo
echo "--- disk"
df -h --output=target,pcent / /root/.airbyte
echo "--- auto-stop"
systemctl list-timers auto-stop.timer --no-pager | head -2
' 900

echo ">> Ready. Open the UI with ops/ui-tunnel.sh and enable the connection(s)."
