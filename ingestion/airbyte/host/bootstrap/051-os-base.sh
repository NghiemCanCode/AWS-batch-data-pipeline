#!/usr/bin/env bash
# Step 5.1: base OS packages and checks.
# Usage: sudo bash 051-os-base.sh
set -euo pipefail
timedatectl set-timezone UTC
export DEBIAN_FRONTEND=noninteractive
apt-get update && apt-get -y full-upgrade
apt-get install -y xfsprogs jq unzip nvme-cli curl postgresql-client   # psql for test 8.3

command -v aws >/dev/null || { echo "AWS CLI missing: do step 4.5 first"; exit 1; }
grep -q '^AWS_DEFAULT_REGION=' /etc/environment || echo 'AWS_DEFAULT_REGION=ap-southeast-1' >> /etc/environment

# SSH was disabled and the auto-stop timer installed by cloud-init at launch (step 3.2). Verify:
cloud-init status --wait
systemctl is-enabled ssh.socket || true  # expected: disabled
systemctl list-timers auto-stop.timer --no-pager
chronyc sources                          # Amazon Time Sync 169.254.169.123

if [ -f /var/run/reboot-required ]; then echo "Reboot..."; reboot; fi
