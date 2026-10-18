#!/usr/bin/env bash
# Step 7.3: set the admin login from Parameter Store.
# Usage: sudo -H bash 073-set-credentials.sh <admin-email> <admin_password_param>
set -euo pipefail
EMAIL=${1:?missing admin email}
PARAM=${2:?missing parameter name (tf_out admin_password_param)}

pw=$(aws ssm get-parameter --name "$PARAM" --with-decryption --query Parameter.Value --output text)
[ "$pw" != "set-by-script" ] || { echo "$PARAM is still a placeholder: run setup/042 on the laptop first"; exit 1; }

abctl local credentials --email "$EMAIL" --password "$pw" >/dev/null
unset pw
abctl local credentials | grep -iv password    # email, client-id, client-secret; never print the password
