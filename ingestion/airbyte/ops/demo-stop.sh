#!/usr/bin/env bash
# Step 13.3. Usage: ./demo-stop.sh [backup|none]   (default: backup)
# Disable the connection(s) in the UI and let the running sync finish BEFORE running this.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../lib/common.sh"
AB_ID=$(tf_out instance_id)
MODE=${1:-backup}

if [ "$MODE" != "none" ]; then
  echo ">> Dump internal database"
  run_remote "$AB_ID" "/usr/local/bin/airbyte-db-backup.sh $(tf_out ops_bucket)" 1800
fi

echo ">> Stop Airbyte"
aws ec2 stop-instances --instance-ids "$AB_ID" >/dev/null
aws ec2 wait instance-stopped --instance-ids "$AB_ID"
echo ">> Stopped."
