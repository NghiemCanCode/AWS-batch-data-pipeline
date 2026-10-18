#!/usr/bin/env bash
# Step 5.3: kernel limits kind needs.
# Usage: sudo bash 053-kernel.sh
set -euo pipefail
install -o root -g root -m 0644 /opt/fds/airbyte/host/etc/sysctl.d/90-kind.conf /etc/sysctl.d/90-kind.conf
sysctl --system >/dev/null
sysctl fs.inotify.max_user_watches fs.inotify.max_user_instances
