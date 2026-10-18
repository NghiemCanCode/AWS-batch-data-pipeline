#!/usr/bin/env bash
# Step 4.2: Airbyte admin password. Safe to re-run: an existing value is never overwritten.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../lib/common.sh"

set_if_placeholder "$(tf_out admin_password_param)" "$(openssl rand -base64 48 | tr -d '/+=\n' | cut -c1-32)"
