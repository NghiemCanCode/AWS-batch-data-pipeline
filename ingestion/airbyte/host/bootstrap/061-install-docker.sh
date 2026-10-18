#!/usr/bin/env bash
# Step 6.1: install Docker from the Ubuntu archive and tie it to the data volume.
# Usage: sudo bash 061-install-docker.sh      (safe to re-run)
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive
mountpoint -q /root/.airbyte || { echo "/root/.airbyte not mounted: run 052 first"; exit 1; }

apt-get install -y docker.io
apt-mark hold docker.io containerd runc    # upgraded deliberately, never in the middle of a demo

install -o root -g root -m 0644 /opt/fds/airbyte/host/etc/docker/daemon.json /etc/docker/daemon.json
install -d -m 0755 /etc/systemd/system/docker.service.d
install -m 0644 /opt/fds/airbyte/host/etc/systemd/system/docker.service.d/override.conf \
  /etc/systemd/system/docker.service.d/override.conf
systemctl daemon-reload
systemctl enable docker
systemctl restart docker

docker info --format '{{.ServerVersion}} {{.LoggingDriver}}'
docker run --rm hello-world | grep -m1 "Hello from Docker"
