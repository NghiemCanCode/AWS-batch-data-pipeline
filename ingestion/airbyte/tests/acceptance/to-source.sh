#!/usr/bin/env bash
# Step 8.3: check the path from the Airbyte host to one PostgreSQL source, as the cdc_airbyte user.
# Usage: bash to-source.sh <source-name> <host-ip> <database>     e.g. core-banking 10.101.10.10 core_banking
# The password is typed at the prompt (from the source team); it never appears in history or SSM.
set -euo pipefail
NAME=${1:?source name}; HOST=${2:?host ip}; DB=${3:?database}
CA=$(mktemp); trap 'rm -f "$CA"' EXIT

aws ssm get-parameter --name "/fds/$NAME/tls/ca-crt" --query Parameter.Value --output text > "$CA"
read -rsp "cdc_airbyte password for $NAME: " PGPASSWORD; echo
export PGPASSWORD
C="host=$HOST dbname=$DB user=cdc_airbyte sslmode=verify-full sslrootcert=$CA connect_timeout=10"

echo "== 1. TCP 5432 reachable (route + both SGs)"
timeout 5 bash -c "</dev/tcp/$HOST/5432" && echo OK

echo "== 2. TLS verify-full, normal connection"
psql "$C" -XAtc "select 'ssl=' || ssl || ' ' || version from pg_stat_ssl where pid = pg_backend_pid()"

echo "== 3. Replication connection"
psql "$C replication=database" -XAtc "IDENTIFY_SYSTEM;"

echo "== 4. Publication visible, slot not created by this test"
psql "$C" -XAtc "select pubname from pg_publication"

echo "== 5. Cannot write (expect: permission denied)"
out=$(psql "$C" -XAtc "delete from core.mcc_codes where false" 2>&1 || true)
if grep -q "permission denied" <<<"$out"; then
  echo OK
else
  echo "FAIL, psql said: ${out:-<no output>}"
  exit 1
fi
