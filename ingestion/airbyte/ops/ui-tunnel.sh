#!/usr/bin/env bash
# Step 7.4: forward localhost:8000 on the laptop to port 8000 on the Airbyte host. Ctrl+C to close.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../lib/common.sh"

aws ssm start-session --target "$(tf_out instance_id)" \
  --document-name AWS-StartPortForwardingSession \
  --parameters '{"portNumber":["8000"],"localPortNumber":["8000"]}'
