# Source this file only; do not execute it directly
AB_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
REPO_ROOT=$(cd "$AB_DIR/../.." && pwd)
TF_DIR="$REPO_ROOT/terraform/environments/dev/ingestion"
export AWS_REGION=${AWS_REGION:-ap-southeast-1}
export AWS_DEFAULT_REGION=$AWS_REGION

# Read a Terraform output of the ingestion stack: tf_out <output>
tf_out() { terraform -chdir="$TF_DIR" output -raw "$1"; }

# Only write a parameter while it still holds the Terraform placeholder
set_if_placeholder() {
  local name=$1 value=$2 cur
  cur=$(aws ssm get-parameter --name "$name" --with-decryption --query Parameter.Value --output text)
  if [ "$cur" = "set-by-script" ]; then
    aws ssm put-parameter --overwrite --name "$name" --value "$value" >/dev/null && echo "set  $name"
  else
    echo "skip $name (already set)"
  fi
}

# Run a command on an instance via SSM and print stdout; on failure print stderr and return 1.
# Never pass a secret here: Run Command parameters are kept in the command history.
run_remote() {
  local id=$1 cmd=$2 timeout=${3:-3600} cid status
  cid=$(aws ssm send-command --instance-ids "$id" --document-name AWS-RunShellScript \
        --parameters "$(jq -n --arg c "$cmd" --arg t "$timeout" '{commands:[$c], executionTimeout:[$t]}')" \
        --query Command.CommandId --output text)
  while :; do
    status=$(aws ssm get-command-invocation --command-id "$cid" --instance-id "$id" \
             --query Status --output text 2>/dev/null || echo Pending)
    case $status in
      Success) break ;;
      Failed|Cancelled|TimedOut|Cancelling)
        aws ssm get-command-invocation --command-id "$cid" --instance-id "$id" \
          --query '[StandardOutputContent,StandardErrorContent]' --output text
        return 1 ;;
    esac
    sleep 5
  done
  aws ssm get-command-invocation --command-id "$cid" --instance-id "$id" \
    --query StandardOutputContent --output text
}

wait_ssm_online() {
  local id=$1
  until [ "$(aws ssm describe-instance-information --filters "Key=InstanceIds,Values=$id" \
             --query 'InstanceInformationList[0].PingStatus' --output text)" = "Online" ]; do
    sleep 10
  done
}
