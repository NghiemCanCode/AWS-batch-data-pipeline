#!/usr/bin/env bash
# Step 11.1: install the metric script, its cron entry and the backup script, then push one sample.
# Usage: sudo bash 111-airbyte-metrics.sh      (safe to re-run)
set -euo pipefail
SRC=/opt/fds/airbyte/host
install -o root -g root -m 0755 $SRC/bin/airbyte-metrics.sh     /usr/local/bin/airbyte-metrics.sh
install -o root -g root -m 0755 $SRC/bin/airbyte-db-backup.sh   /usr/local/bin/airbyte-db-backup.sh
install -o root -g root -m 0644 $SRC/etc/cron.d/airbyte-metrics /etc/cron.d/airbyte-metrics
/usr/local/bin/airbyte-metrics.sh && echo "metrics pushed OK"
