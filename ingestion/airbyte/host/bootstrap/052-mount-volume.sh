#!/usr/bin/env bash
# Step 5.2: format (only if blank) and mount the data volume at /root/.airbyte. Safe to re-run, never wipes data.
# Usage: sudo bash 052-mount-volume.sh <data_volume_serial>
set -euo pipefail
SERIAL=${1:?missing data_volume_serial (tf_out data_volume_serial)}
DIR=/root/.airbyte
DEV=/dev/disk/by-id/nvme-Amazon_Elastic_Block_Store_${SERIAL}

[ -b "$DEV" ] || { echo "$DEV not found: wrong serial, or the volume is not attached"; exit 1; }

# abctl was already run before this volume was mounted: its data sits on the root volume and would be hidden
if [ -d "$DIR" ] && ! mountpoint -q "$DIR" && [ -n "$(ls -A "$DIR")" ]; then
  echo "$DIR already has files on the root volume. Move them away or run 'abctl local uninstall' first."
  exit 1
fi

if [ -z "$(blkid -s TYPE -o value "$DEV")" ]; then
  mkfs.xfs -L airbyte "$DEV"
fi

uuid=$(blkid -s UUID -o value "$DEV")
install -d -o root -g root -m 0700 "$DIR"
grep -q "UUID=$uuid" /etc/fstab || echo "UUID=$uuid $DIR xfs defaults,noatime,nofail 0 0" >> /etc/fstab

systemctl daemon-reload
mount -a
findmnt "$DIR"
df -h "$DIR"
