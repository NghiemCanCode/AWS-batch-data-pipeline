#!/usr/bin/env bash
# Step 7.2: run 071-airbyte-install.sh in the background on the Airbyte host and stream its progress here.
# Usage: bash install-airbyte.sh
#   - 071 not running: start it, then watch it.
#   - 071 already running (e.g. after Ctrl+C here): just watch it again, never start a second install.
#
# 071 runs as its own systemd unit (airbyte-install), independent of any SSM session or Run Command.
# Ctrl+C here only stops watching; the install keeps going on the host.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../lib/common.sh"

AB_ID=$(tf_out instance_id)
POLL=${POLL_SECONDS:-15}
UNIT=airbyte-install
LOG=/tmp/071.log
RC=/tmp/071.rc

state=$(run_remote "$AB_ID" "systemctl is-active --quiet $UNIT && echo running || echo idle" 120)
if [ "$state" = running ]; then
  echo ">> 071 is already running on $AB_ID: following it (log on the host: $LOG)"
else
  echo ">> Starting 071 on $AB_ID (log on the host: $LOG)"
  # \$? is expanded on the host, when 071 exits
  run_remote "$AB_ID" "rm -f $LOG $RC
systemctl reset-failed $UNIT 2>/dev/null || true
systemd-run --unit=$UNIT --setenv=HOME=/root /bin/bash -c \
  '/bin/bash /opt/fds/airbyte/host/bootstrap/071-airbyte-install.sh $(tf_out storage_bucket) > $LOG 2>&1; echo \$? > $RC'" 120 >/dev/null
fi

# Runs on the host every poll: new log lines since __SEEN__, a one-line pod summary, the exit code once 071 is done.
# Every output line carries a @@ tag so the loop below can tell them apart. Plain sh (dash): no bash-only syntax.
# Run Command output is cut at ~24,000 characters, so the log is cleaned and capped HERE, before it is sent:
#   - abctl draws spinners with \r, so one log "line" can hold thousands of frames: keep only the last frame
#   - strip colour codes, cut every line to 300 characters, send at most 60 lines per poll
read -r -d '' STATUS_CMD <<'EOF' || true
LOG=/tmp/071.log
n=0
[ -f "$LOG" ] && n=$(wc -l < "$LOG")
start=$((__SEEN__ + 1))
if [ "$n" -ge "$start" ]; then
  if [ $((n - start)) -ge 60 ]; then
    echo "@@LOG ($((n - start - 59)) earlier lines skipped, full log in $LOG on the host)"
    start=$((n - 59))
  fi
  sed -n "${start},${n}p" "$LOG" \
    | awk -F '\r' '{ print $NF }' \
    | sed 's/\x1b\[[0-9;?]*[A-Za-z]//g' \
    | cut -c1-300 \
    | grep -v '^[[:space:]]*$' \
    | sed 's/^/@@LOG /'
fi
echo "@@LINES $n"
pods=$(docker exec airbyte-abctl-control-plane kubectl --kubeconfig /etc/kubernetes/admin.conf \
  get pods -A --no-headers 2>/dev/null || true)
if [ -n "$pods" ]; then
  echo "$pods" | awk '{ total++; split($3, r, "/") }
    $4 == "Completed" || ($4 == "Running" && r[1] == r[2]) { ok++; next }
    $4 ~ /CrashLoopBackOff|Error|ImagePullBackOff|ErrImagePull|OOMKilled|CreateContainerConfigError/ {
      bad = bad " " $2 " (" $4 ", " $5 " restarts)" }
    END { printf "@@PODS %d/%d ready", ok, total; if (bad != "") printf " | problems:%s", bad; print "" }' \
    | cut -c1-1000
else
  echo "@@PODS cluster not up yet"
fi
[ -f /tmp/071.rc ] && echo "@@RC $(cat /tmp/071.rc)"
if systemctl is-active --quiet airbyte-install; then echo "@@UNIT active"; else echo "@@UNIT inactive"; fi
true
EOF

trap 'echo; echo ">> Stopped watching. 071 keeps running on the host; run this script again to follow it."; exit 130' INT

t0=$(date +%s)
seen=0
last_pods=""
while :; do
  if ! out=$(run_remote "$AB_ID" "${STATUS_CMD//__SEEN__/$seen}" 120); then
    echo ">> status poll failed, retrying in ${POLL}s"
    sleep "$POLL"
    continue
  fi

  rc="" unit=""
  stamp=$(printf '%02d:%02d' $(( ($(date +%s) - t0) / 60 )) $(( ($(date +%s) - t0) % 60 )))
  while IFS= read -r line; do
    case $line in
      "@@LOG "*)   echo "[$stamp] ${line#@@LOG }" ;;
      "@@LINES "*) seen=${line#@@LINES } ;;
      "@@PODS "*)  pods=${line#@@PODS }
                   if [ "$pods" != "$last_pods" ]; then echo "[$stamp] pods: $pods"; last_pods=$pods; fi ;;
      "@@RC "*)    rc=${line#@@RC } ;;
      "@@UNIT "*)  unit=${line#@@UNIT } ;;
    esac
  done <<<"$out"

  if [ -n "$rc" ]; then
    echo ">> 071 finished with exit code $rc after $stamp (full log: $LOG on the host)"
    exit "$rc"
  fi
  if [ "$unit" = inactive ]; then
    echo ">> 071 stopped without an exit code (killed, or the host rebooted mid-install). Run this script again."
    exit 1
  fi
  sleep "$POLL"
done
