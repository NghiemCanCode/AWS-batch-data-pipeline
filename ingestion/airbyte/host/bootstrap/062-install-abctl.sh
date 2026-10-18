#!/usr/bin/env bash
# Step 6.2: install the pinned abctl binary.
# Usage: sudo bash 062-install-abctl.sh      (safe to re-run)
set -euo pipefail
source /opt/fds/airbyte/host/etc/airbyte/versions.env
case "$ABCTL_VERSION" in *pin-me*) echo "Set ABCTL_VERSION in host/etc/airbyte/versions.env first"; exit 1 ;; esac

if [ "$(abctl version 2>/dev/null | grep -o 'v[0-9.]*' | head -1)" = "$ABCTL_VERSION" ]; then
  echo "abctl $ABCTL_VERSION already installed"; exit 0
fi

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
curl -fsSL "https://github.com/airbytehq/abctl/releases/download/${ABCTL_VERSION}/abctl-${ABCTL_VERSION}-linux-amd64.tar.gz" \
  -o "$tmp/abctl.tar.gz"
tar -xzf "$tmp/abctl.tar.gz" -C "$tmp"
install -o root -g root -m 0755 "$(find "$tmp" -type f -name abctl | head -1)" /usr/local/bin/abctl
abctl version
