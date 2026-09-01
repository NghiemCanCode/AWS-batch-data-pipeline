#!/usr/bin/env bash
set -Eeuo pipefail

# ===========================================
# Dựng hạ tầng dev từ số 0 đến trạng thái chạy được job: 3 terraform stack
# (platform → compute → orchestration), seed dữ liệu bronze (optional, không dùng cho prod),
# build Spark image, và ghi scripts/.env.runtime cho các script deploy job đọc.
#
# Chạy TAY, trên máy dev, có người ngồi trước terminal. Máy chạy script này KHÔNG phải
# máy CI: CI thật là GitHub Actions worker, và script chỉ nhờ nó build image (xem
# step_image). Vì vậy script luôn hỏi xác nhận trừ khi có --yes, và không có đường
# cấu hình nào qua biến môi trường - mọi lựa chọn đều là flag.
#
# KHÔNG dùng cho:
#   - cập nhật một stack đơn lẻ      -> terraform apply trong thư mục stack đó
#   - làm mới .env.runtime           -> scripts/refresh_runtime_env_dev.sh
#   - chạy trong pipeline CI/CD      -> đây là script dựng nhanh cho feature dev
# DAG được đẩy lên S3 ngay trong bước orchestration của script này, không có script riêng.

# Flag:
#   --yes: đồng ý mọi thay đổi, không cần confirm lại
#   --dry-run: in kế hoạch + chạy terraform plan cho các stack plan được, rồi
#              dừng. Không tạo/sửa/xoá gì. Stack sau chỉ plan được khi stack
#              trước đã apply (nó nhận -var từ output của stack trước).
#   --seed: seed tập dữ liệu sample vào bronze layer, mặc định là không
#   --skip-image: không build gì, dùng luôn image mới nhất đang có trong ECR (theo
#                 thời điểm push). EMR vẫn được gắn image, chỉ là image đó có thể
#                 không ứng với commit hiện tại. ECR rỗng thì script dừng, vì apply
#                 với image rỗng là GỠ image khỏi application.
#   --force-image: build lại image kể cả khi ECR đã có image của commit này
#   --skip-orchestration: không khởi tạo hạ tầng của orchestration
#   --help: in help
# Chi tiết đọc docs

# Cần có trước:
#   - CLI: terraform, aws, gh, git, jq
#   - AWS credentials tạo được S3/ECR/IAM/EMR Serverless/VPC/MWAA
#   - gh đã đăng nhập, token có scope repo + workflow
#   - repo đã có secret AWS_GITHUB_ACTIONS_ROLE_ARN                     [chỉ khi build image]
#     Script KHÔNG ghi secret này. Đặt tay một lần, giá trị là output
#     github_actions_role_arn của stack platform (ARN chỉ đổi nếu bạn tạo lại stack):
#       gh secret set AWS_GITHUB_ACTIONS_ROLE_ARN --body "$(terraform \
#         -chdir=terraform/environments/dev/platform output -raw github_actions_role_arn)"
#   - TF_VAR_airflow_version phải là version MWAA đang hỗ trợ            [lần đầu tạo MWAA]
#     Không có API nào liệt kê danh sách đó, script không kiểm hộ được. Sai version thì
#     apply chạy 20-30 phút mới báo CREATE_FAILED, và NAT gateway đã tính tiền từ trước
#     đó. Mở console MWAA xem dropdown "Airflow version" trước khi chạy.
#   - terraform/bootstrap đã chạy: bucket state trong TF_BACKEND_BUCKET phải tồn tại
#   - .env ở gốc repo (xem .env.example) với đủ TF_VAR_* của 3 stack
#   - airflow/requirements.txt phải tồn tại: stack platform upload file này bằng
#     filemd5(), thiếu nó là terraform vỡ ngay ở bước plan
#   - branch hiện tại đã push lên origin và docker/ không còn thay đổi chưa commit
#     (CI build từ origin, không phải từ working tree của bạn)
#   - quyền đọc bucket seed: s3://central-dev-data-0703/finance-transaction/  [chỉ với --seed]

# Tạo ra / thay đổi:
#   Trên AWS
#     - S3: <data_lake>-dev, <code_bucket>-dev (kèm dags/, requirements.txt)
#     - ECR: emr-serverless-custom-dev + image được push
#     - IAM: OIDC role cho GitHub Actions, EMR execution/operation role, MWAA execution role
#     - EMR Serverless application + Glue databases (gold, gold_staging)
#     - VPC + NAT gateway + MWAA environment            [bỏ qua với --skip-orchestration]
#   Ngoài AWS
#     - kích hoạt một workflow run docker-build-push-ecr.yml  [bỏ qua với --skip-image]
#     - ghi đè file scripts/.env.runtime ở máy local, xây dựng từ terraform output để làm
#       state nhanh cho các script job. File có header provenance: sinh lúc nào, ở
#       account/region nào, từ branch/commit nào.

# Image đưa vào EMR:
#   Workflow push 2 tag mỗi lần build: :<IMAGE_TAG> (mặc định latest) và :sha-<commit>.
#   Script pin compute vào :sha-<commit của HEAD> chứ không phải :latest, để EMR gắn
#   với đúng một build và terraform mới thật sự no-op khi không có gì đổi.
#   Nếu ECR đã có tag của commit này, bước build được bỏ qua (tiết kiệm ~10 phút);
#   dùng --force-image để ép build lại.

# Thời gian: ~45 phút (MWAA ~30, CI build image ~5-10, còn lại vài phút)
# Chi phí sau khi chạy xong, NẾU ĐỂ NGUYÊN:
#   MWAA mw1.small ~0.49 USD/h + ~0.055 USD/h mỗi worker
#   NAT gateway    ~0.045 USD/h + phí data
#   => ~0.6 USD/h  ≈  ~14 USD/ngày  ≈  ~430 USD/tháng
#   platform + compute ≈ 0 USD lúc nghỉ (EMR Serverless không bật initial capacity)
# Xoá phần tốn tiền: bash scripts/remove_mwaa.sh

# Chạy lại: an toàn, và hầu hết các bước là no-op
#   terraform (3 stack)  : idempotent, không đổi gì thì không apply gì
#   seed bronze          : aws s3 sync, chỉ copy file mới
#   build image          : cùng commit -> tag sha đã có trong ECR -> bỏ qua CI run.
#                          --force-image để ép build. --skip-image thì lấy image mới
#                          nhất có sẵn trong ECR, không kích hoạt CI.
#   GitHub secret        : không đụng tới, chỉ đọc xem đã tồn tại chưa
#   .env.runtime         : ghi đè mỗi lần chạy

# Hỏng giữa chừng:
#   ở platform/compute : chạy lại script, terraform tự hoà giải. Không có gì tính tiền.
#   ở build image      : hạ tầng nguyên vẹn. Sửa lỗi build rồi chạy lại, hoặc chạy lại
#                        với --skip-image nếu ECR đã có image dùng được.
#   ở orchestration    : NGUY HIỂM NHẤT. NAT gateway có thể đã tồn tại và ĐANG tính
#                        tiền, MWAA có thể kẹt ở CREATING/CREATE_FAILED.
#                        Xử lý: bash scripts/remove_mwaa.sh rồi chạy lại.
#   terraform kẹt lock : terraform -chdir=<thư mục stack> force-unlock <lock-id>
#
# ===========================================

# ─── Hằng số & đường dẫn ───────────────────────
SCRIPT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/$(basename "${BASH_SOURCE[0]}")"
ROOT_PATH="$(dirname "$(dirname "$SCRIPT_PATH")")"
TR_PLATFORM_DIR="$ROOT_PATH/terraform/environments/dev/platform"
TR_COMPUTE_DIR="$ROOT_PATH/terraform/environments/dev/compute"
TR_ORCHESTRATION_DIR="$ROOT_PATH/terraform/environments/dev/orchestration"
AIRFLOW_DIR="$ROOT_PATH/airflow"
RUNTIME_ENV="$ROOT_PATH/scripts/.env.runtime"
# Terraform thêm hậu tố "-dev". Tên sau hậu tố phải khớp ECR_REPO_NAME hardcode trong
# .github/workflows/docker-build-push-ecr.yml, nếu không CI push vào một repo khác.
ECR_REPO_NAME="emr-serverless-custom"
WORKFLOW_FILE="docker-build-push-ecr.yml"
SEED_SOURCE="s3://central-dev-data-0703/finance-transaction/"

cd "$ROOT_PATH"

# set -a: Terraform chỉ thấy TF_VAR_* khi chúng được export, mà .env chỉ là các dòng KEY=value.
if [ -f "$ROOT_PATH/.env" ]; then
  set -a
  source "$ROOT_PATH/.env"
  set +a
fi

AWS_REGION="${AWS_REGION:-ap-southeast-1}"
TF_BACKEND_BUCKET="${TF_BACKEND_BUCKET:-finance-transaction-tf-state-dev-0306}"
TF_BACKEND_REGION="${TF_BACKEND_REGION:-$AWS_REGION}"
IMAGE_TAG="${IMAGE_TAG:-latest}"

TF_KEY_PLATFORM="dev/platform/terraform.tfstate"
TF_KEY_COMPUTE="dev/compute/terraform.tfstate"
TF_KEY_ORCHESTRATION="dev/orchestration/terraform.tfstate"

# preflight điền, show_plan và các step_* đọc.
AWS_ACCOUNT=""
CURRENT_BRANCH=""
COMMIT_SHA=""
GITHUB_REPO=""

# step_platform → step_compute → step_orchestration truyền giá trị cho nhau qua đây.
DATALAKE_BUCKET_NAME=""
DATALAKE_BUCKET_URI=""
CODEBASE_BUCKET_NAME=""
ECR_REPOSITORY_URL=""
GH_ACTIONS_ROLE_ARN=""
REQUIREMENTS_VERSION=""
IMAGE_URI=""
EMR_APPLICATION_ID=""
EMR_EXECUTION_ROLE_ARN=""
EMR_LOG_GROUP_NAME=""
MWAA_WEBSERVER_URL=""

# read_stack_states điền, show_plan và step_platform đọc.
STATE_PLATFORM=""
STATE_COMPUTE=""
STATE_ORCHESTRATION=""

AUTO_APPROVE=false
DRY_RUN=false
SEED=false
SKIP_IMAGE=false
FORCE_IMAGE=false
SKIP_ORCHESTRATION=false
RAW_ARGS=("$@")
ERRORS=()

# Bước nào đã chạy tới đâu: summary in ra, và ERR trap dùng để chỉ đúng cách khôi phục.
STEP_ORDER=(platform seed image compute runtime_env orchestration)
declare -A STEP_LABEL=(
  [platform]="terraform platform (S3, ECR, OIDC role)"
  [seed]="seed bronze layer"
  [image]="Spark image trên ECR"
  [compute]="terraform compute (EMR Serverless, Glue)"
  [runtime_env]="scripts/.env.runtime"
  [orchestration]="terraform orchestration (VPC, NAT, MWAA)"
)
declare -A STEP_STATUS=()
CURRENT_STEP=""

# ─── Tiện ích chung ────────────────────────────
usage() { awk '/^# ={10,}/{n++; next} n==1 {sub(/^# ?/, ""); print}' "$SCRIPT_PATH"; }

parse_args() {
  while [ $# -gt 0 ]; do
    case "$1" in
      -y|--yes)             AUTO_APPROVE=true ;;
      --dry-run)            DRY_RUN=true ;;
      --seed)               SEED=true ;;
      --skip-image)         SKIP_IMAGE=true ;;
      --force-image)        FORCE_IMAGE=true ;;
      --skip-orchestration) SKIP_ORCHESTRATION=true ;;
      -h|--help)            usage; exit 0 ;;
      -*)                   echo "Unknown option: $1 (try --help)" >&2; exit 1 ;;
      *)                    echo "Unexpected argument: $1 (try --help)" >&2; exit 1 ;;
    esac
    shift
  done

  if [ "$SKIP_IMAGE" = true ] && [ "$FORCE_IMAGE" = true ]; then
    echo "--skip-image và --force-image loại trừ nhau" >&2
    exit 1
  fi
}

show_args() {
  if [ "${#RAW_ARGS[@]}" -eq 0 ]; then
    echo "Flag đã nhận: (không có)"
  else
    echo "Flag đã nhận: ${RAW_ARGS[*]}"
  fi

  echo "Biến sau khi parse:"
  printf '  %-22s = %s\n' \
    AUTO_APPROVE        "$AUTO_APPROVE" \
    DRY_RUN             "$DRY_RUN" \
    SEED                "$SEED" \
    SKIP_IMAGE          "$SKIP_IMAGE" \
    FORCE_IMAGE         "$FORCE_IMAGE" \
    SKIP_ORCHESTRATION  "$SKIP_ORCHESTRATION"
  echo
}

fail() { ERRORS+=("$1"); }

require_cmd() {
  local cmd
  for cmd in "$@"; do
    command -v "$cmd" >/dev/null 2>&1 || fail "thiếu CLI: $cmd"
  done
}

# Biến không có default trong stack. terraform.tfvars trong thư mục stack thắng .env,
# nên khi file đó tồn tại thì bỏ qua kiểm tra (warn_tfvars đã nhắc trong show_plan).
require_stack_vars() {
  local stack_dir="$1" var
  shift
  [ -f "$stack_dir/terraform.tfvars" ] && return 0
  for var in "$@"; do
    [ -n "${!var:-}" ] || fail "thiếu biến: $var cho stack $(basename "$stack_dir") (đặt trong .env, xem .env.example)"
  done
  return 0
}

begin_step() {
  CURRENT_STEP="$1"
  # Đánh dấu failed trước, finish_step mới đổi thành ok: nếu ERR trap nổ giữa chừng
  # thì bảng trạng thái chỉ đúng bước đang dở.
  STEP_STATUS["$1"]="failed"
  echo "─── ${STEP_LABEL[$1]} ─────────────────────"
}

finish_step() {
  [ -n "$CURRENT_STEP" ] && STEP_STATUS["$CURRENT_STEP"]="ok"
  CURRENT_STEP=""
  echo
  return 0
}

skip_step() {
  STEP_STATUS["$1"]="skipped"
  printf '─── %s: bỏ qua (%s)\n\n' "${STEP_LABEL[$1]}" "$2"
}

print_status_table() {
  local s
  echo "Trạng thái các bước:"
  for s in "${STEP_ORDER[@]}"; do
    printf '  %-14s %-8s %s\n' "$s" "${STEP_STATUS[$s]:-absent}" "${STEP_LABEL[$s]}"
  done
}

on_err() {
  local code=$? line=${BASH_LINENO[0]}
  trap - ERR

  echo >&2
  echo "─── THẤT BẠI ──────────────────────────────────" >&2
  printf '  exit code %s tại dòng %s%s\n\n' "$code" "$line" \
    "${CURRENT_STEP:+, trong bước $CURRENT_STEP}" >&2
  print_status_table >&2
  echo >&2
  echo "Khôi phục:" >&2
  case "$CURRENT_STEP" in
    platform|compute)
      echo "  Chưa có tài nguyên nào tính tiền theo giờ. Sửa nguyên nhân rồi chạy lại" >&2
      echo "  script này, terraform tự hoà giải phần đã tạo." >&2
      ;;
    seed)
      echo "  Hạ tầng nguyên vẹn. Kiểm tra quyền đọc $SEED_SOURCE rồi chạy lại." >&2
      ;;
    image)
      echo "  Hạ tầng nguyên vẹn, chưa có gì tính tiền. Xem log CI:" >&2
      echo "    gh run list --repo $GITHUB_REPO --workflow=$WORKFLOW_FILE" >&2
      echo "  Nếu ECR đã có image dùng được: chạy lại với --skip-image." >&2
      ;;
    runtime_env)
      echo "  Hạ tầng đã xong, chỉ file local hỏng. Sinh lại bằng:" >&2
      echo "    bash scripts/refresh_runtime_env_dev.sh" >&2
      ;;
    orchestration)
      echo "  CẢNH BÁO: NAT gateway và/hoặc MWAA có thể đã tồn tại và ĐANG tính tiền" >&2
      echo "  (~0.6 USD/h), MWAA có thể kẹt ở CREATING/CREATE_FAILED. Dọn trước:" >&2
      echo "    bash scripts/remove_mwaa.sh" >&2
      echo "  rồi chạy lại script này." >&2
      ;;
    *)
      echo "  Lỗi trước khi apply bất kỳ stack nào, không có gì để dọn." >&2
      ;;
  esac
  echo "  Nếu terraform báo kẹt lock: terraform -chdir=<thư mục stack> force-unlock <lock-id>" >&2
  exit "$code"
}

# ─── Preflight ─────────────────────────────────
preflight() {
  echo "─── Preflight ─────────────────────────────────"

  require_cmd terraform aws gh git jq
  [ -f "$ROOT_PATH/.env" ] || fail "không tìm thấy $ROOT_PATH/.env (xem .env.example)"

  # Stack platform upload file này bằng filemd5(): thiếu nó thì cả plan lẫn apply đều vỡ,
  # và thông báo lỗi của terraform không nói được là thiếu file gì.
  [ -f "$AIRFLOW_DIR/requirements.txt" ] \
    || fail "thiếu $AIRFLOW_DIR/requirements.txt (stack platform upload file này, để trống cũng được)"

  local caller="" arn=""
  caller="$(aws sts get-caller-identity --query '[Account,Arn]' --output text 2>/dev/null)" || caller=""
  if [ -z "$caller" ]; then
    fail "AWS credentials không dùng được (aws sts get-caller-identity thất bại)"
  else
    read -r AWS_ACCOUNT arn <<<"$caller"
    printf '  %-16s %s\n' "AWS account:" "$AWS_ACCOUNT"
    printf '  %-16s %s\n' "Identity:" "$arn"
  fi
  printf '  %-16s %s\n' "Region:" "$AWS_REGION"

  # State bucket do terraform/bootstrap tạo, script này không tạo.
  aws s3api head-bucket --bucket "$TF_BACKEND_BUCKET" >/dev/null 2>&1 \
    || fail "state bucket không truy cập được: $TF_BACKEND_BUCKET (đã chạy terraform/bootstrap chưa?)"

  require_stack_vars "$TR_PLATFORM_DIR" TF_VAR_region TF_VAR_data_lake_bucket_name TF_VAR_code_bucket_name
  require_stack_vars "$TR_COMPUTE_DIR" TF_VAR_region TF_VAR_emr_app_name TF_VAR_emr_release_label \
    TF_VAR_is_endpoint TF_VAR_is_studio_enabled
  if [ "$SKIP_ORCHESTRATION" != true ]; then
    require_stack_vars "$TR_ORCHESTRATION_DIR" TF_VAR_region TF_VAR_name_prefix \
      TF_VAR_mwaa_environment_name TF_VAR_airflow_version TF_VAR_availability_zones

    # MWAA chỉ nhận version dạng X.Y.Z (vd 3.0.2). "3" hay "3.2" bị API từ chối,
    # nhưng chỉ sau khi VPC + NAT gateway đã tạo xong và đang tính tiền, rồi phải
    # đợi 20-30 phút mới thấy CREATE_FAILED. Chặn ngay ở đây gần như miễn phí.
    # Không kiểm được version đó CÓ ĐƯỢC HỖ TRỢ không - AWS không có API cho việc đó.
    if [ -n "${TF_VAR_airflow_version:-}" ] \
      && ! [[ "$TF_VAR_airflow_version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
      fail "TF_VAR_airflow_version='$TF_VAR_airflow_version' sai định dạng, MWAA cần X.Y.Z (vd 3.0.2). Đối chiếu dropdown 'Airflow version' trong console MWAA"
    fi
  fi

  # Lệch region nghĩa là terraform tạo tài nguyên ở một region, còn script seed / đọc ECR /
  # kiểm tra MWAA ở region khác. Hỏng theo kiểu rất khó nhìn ra.
  if [ -n "${TF_VAR_region:-}" ] && [ "$TF_VAR_region" != "$AWS_REGION" ]; then
    fail "TF_VAR_region ($TF_VAR_region) khác AWS_REGION ($AWS_REGION), sửa .env cho khớp"
  fi

  if gh auth status >/dev/null 2>&1; then
    GITHUB_REPO="$(gh repo view --json nameWithOwner --jq .nameWithOwner 2>/dev/null)" || GITHUB_REPO=""
    [ -n "$GITHUB_REPO" ] || fail "không xác định được repo qua gh (thư mục này có remote GitHub không?)"
  else
    fail "gh chưa đăng nhập (gh auth login)"
  fi

  CURRENT_BRANCH="$(git rev-parse --abbrev-ref HEAD 2>/dev/null)" || CURRENT_BRANCH=""
  COMMIT_SHA="$(git rev-parse HEAD 2>/dev/null)" || COMMIT_SHA=""

  if [ "$SKIP_IMAGE" != true ]; then
    [ -z "$(git status --porcelain -- docker/)" ] \
      || fail "docker/ còn thay đổi chưa commit (CI build từ origin, không phải máy bạn)"

    if [ "$CURRENT_BRANCH" = HEAD ] || [ -z "$CURRENT_BRANCH" ]; then
      fail "đang ở detached HEAD, hãy checkout một branch rồi push"
    elif ! git rev-parse --verify --quiet "origin/$CURRENT_BRANCH" >/dev/null; then
      fail "branch $CURRENT_BRANCH chưa có trên origin (git push -u origin $CURRENT_BRANCH)"
    elif [ "$COMMIT_SHA" != "$(git rev-parse "origin/$CURRENT_BRANCH")" ]; then
      fail "HEAD khác origin/$CURRENT_BRANCH, CI sẽ build commit cũ (git push, hoặc git fetch nếu vừa push)"
    fi

    # Workflow assume role qua secret này, và script không đặt nó hộ. Thiếu secret thì CI
    # chết ở bước configure-aws-credentials với lỗi OIDC rất khó đọc, sau khi đã tốn
    # vài phút chạy. Đọc cột đầu của output dạng bảng, không dùng --json: gh cũ chưa
    # hỗ trợ --json cho secret list.
    # Chỉ kiểm tra được là secret CÓ TỒN TẠI, không đọc được giá trị (secret là write-only).
    if [ -n "$GITHUB_REPO" ]; then
      gh secret list --repo "$GITHUB_REPO" 2>/dev/null | awk '{print $1}' \
        | grep -qx 'AWS_GITHUB_ACTIONS_ROLE_ARN' \
        || fail "repo chưa có secret AWS_GITHUB_ACTIONS_ROLE_ARN, CI sẽ fail ở bước OIDC. Đặt nó bằng lệnh in ở --help, hoặc chạy với --skip-image"
    fi
  fi

  if [ "$SEED" = true ]; then
    aws s3 ls "$SEED_SOURCE" >/dev/null 2>&1 \
      || fail "không đọc được bucket seed: $SEED_SOURCE"
  fi

  if [ "${#ERRORS[@]}" -gt 0 ]; then
    echo
    echo "Preflight thất bại:" >&2
    printf '  - %s\n' "${ERRORS[@]}" >&2
    exit 1
  fi
  echo "  Preflight OK"
  echo
}

# ─── Terraform & ECR helper ────────────────────
stack_state() {
  if aws s3api head-object --bucket "$TF_BACKEND_BUCKET" --key "$1" >/dev/null 2>&1; then
    echo "CẬP NHẬT"
  else
    echo "TẠO MỚI"
  fi
}

# Đọc một lần, trước khi apply bất cứ thứ gì: show_plan in ra, step_platform dùng để
# biết ARN vừa in có phải ARN mới tinh không. Đọc lại sau apply sẽ luôn ra "CẬP NHẬT".
read_stack_states() {
  STATE_PLATFORM="$(stack_state "$TF_KEY_PLATFORM")"
  STATE_COMPUTE="$(stack_state "$TF_KEY_COMPUTE")"
  STATE_ORCHESTRATION="$(stack_state "$TF_KEY_ORCHESTRATION")"
}

terraform_init_remote_backend() {
  local stack_dir="$1" stack_key="$2"
  terraform -chdir="$stack_dir" init \
    -input=false \
    -reconfigure \
    -backend-config="bucket=$TF_BACKEND_BUCKET" \
    -backend-config="key=$stack_key" \
    -backend-config="region=$TF_BACKEND_REGION" \
    -backend-config="use_lockfile=true" \
    -backend-config="encrypt=true"
}

tf_apply() {
  local stack_dir="$1" stack_key="$2"
  shift 2
  terraform_init_remote_backend "$stack_dir" "$stack_key"
  terraform -chdir="$stack_dir" apply -input=false -auto-approve -lock-timeout=5m "$@"
}

# Trả rỗng thay vì chết: có output optional (requirements version) giá trị null.
tf_out() { terraform -chdir="$1" output -raw "$2" 2>/dev/null || true; }

require_output() {
  [ -n "$2" ] || {
    echo "terraform output '$1' rỗng, không thể đi tiếp (state có bị apply dở không?)" >&2
    return 1
  }
}

# stack_state chỉ cho biết FILE state có tồn tại không, không cho biết tài nguyên
# đã được tạo chưa - một stack đã init nhưng chưa apply vẫn ra "CẬP NHẬT". Với MWAA
# thì phải hỏi thẳng API, vì cảnh báo tốn-thời-gian-và-tiền chỉ đúng lúc tạo mới.
mwaa_env_exists() {
  local found=""
  found="$(aws mwaa list-environments --region "$AWS_REGION" \
    --query "Environments[?@=='$1']" --output text 2>/dev/null)" || found=""
  [ -n "$found" ]
}

ecr_tag_exists() {
  aws ecr describe-images \
    --region "$AWS_REGION" \
    --repository-name "${ECR_REPO_NAME}-dev" \
    --image-ids imageTag="$1" >/dev/null 2>&1
}

# Tag của image được push gần đây nhất. Ưu tiên tag sha-* thay vì :latest, dù cùng
# trỏ một image: CI push đè :latest sau mỗi build, nên đưa :latest vào terraform thì
# state không bao giờ đổi kể cả khi image đã khác hẳn. Tag sha-* là bất biến.
# Trả về rỗng + exit khác 0 khi repo chưa có image nào.
ecr_newest_image_tag() {
  local tags=""
  tags="$(aws ecr describe-images \
    --region "$AWS_REGION" \
    --repository-name "${ECR_REPO_NAME}-dev" \
    --query 'sort_by(imageDetails,&imagePushedAt)[-1].imageTags' \
    --output json 2>/dev/null)" || return 1
  [ -n "$tags" ] && [ "$tags" != null ] || return 1
  jq -er 'map(select(startswith("sha-"))) + map(select(startswith("sha-") | not)) | .[0] // empty' <<<"$tags"
}

warn_tfvars() {
  [ -f "$1/terraform.tfvars" ] && echo "    (chú ý: $1/terraform.tfvars sẽ ghi đè các giá trị trên)"
  return 0
}

# ─── Kế hoạch & xác nhận ───────────────────────
show_plan() {
  read_stack_states

  echo "─── Kế hoạch ──────────────────────────────────"
  printf '  %-16s %s\n' "Account:" "$AWS_ACCOUNT"
  printf '  %-16s %s\n' "Region:" "$AWS_REGION"
  printf '  %-16s %s\n' "Repo:" "$GITHUB_REPO"
  printf '  %-16s %s\n' "Branch/commit:" "$CURRENT_BRANCH @ ${COMMIT_SHA:0:12}"
  printf '  %-16s %s\n' "State bucket:" "$TF_BACKEND_BUCKET"
  echo

  # Dùng :- ở mọi TF_VAR_*: khi stack có terraform.tfvars riêng thì .env không cần khai
  # báo biến đó, và set -u sẽ giết script ngay tại dòng in kế hoạch.
  echo "Trên AWS"
  printf '  platform       %s\n' "$STATE_PLATFORM"
  echo "    - S3: ${TF_VAR_data_lake_bucket_name:-<tfvars>}-dev (bronze/ silver/ gold/ quarantine/)"
  echo "    - S3: ${TF_VAR_code_bucket_name:-<tfvars>}-dev (jobs/ packages/ dags/ plugins/ logs/ + requirements.txt)"
  echo "    - ECR: ${ECR_REPO_NAME}-dev"
  echo "    - IAM: OIDC provider GitHub + role GitHubActionsRoleDev"
  warn_tfvars "$TR_PLATFORM_DIR"

  printf '  compute        %s\n' "$STATE_COMPUTE"
  echo "    - EMR Serverless: ${TF_VAR_emr_app_name:-<tfvars>}-dev (${TF_VAR_emr_release_label:-<tfvars>})"
  echo "    - Glue database: gold, gold_staging"
  echo "    - IAM: EMRExecutionRoleDev, EMROperationRoleDev"
  if [ "$SKIP_IMAGE" = true ]; then
    echo "    - Image: mới nhất trong ECR, không build (--skip-image)"
  else
    echo "    - Image pin vào: ${ECR_REPO_NAME}-dev:sha-${COMMIT_SHA:0:12}..."
  fi
  warn_tfvars "$TR_COMPUTE_DIR"

  if [ "$SKIP_ORCHESTRATION" != true ]; then
    printf '  orchestration  %s\n' "$STATE_ORCHESTRATION"
    echo "    - VPC: ${TF_VAR_name_prefix:-<tfvars>}-dev-vpc + NAT gateway"
    echo "    - MWAA: ${TF_VAR_mwaa_environment_name:-<tfvars>}-dev (mw1.small, Airflow ${TF_VAR_airflow_version:-<tfvars>}, ~30 phút)"
    echo "    - IAM: MWAAExecutionRoleDev"
    warn_tfvars "$TR_ORCHESTRATION_DIR"
  fi
  echo

  echo "Ngoài AWS"
  if [ "$SKIP_IMAGE" != true ]; then
    if [ "$FORCE_IMAGE" != true ] && ecr_tag_exists "sha-$COMMIT_SHA"; then
      echo "    - Build image: BỎ QUA, ECR đã có sha-${COMMIT_SHA:0:12} (--force-image để ép build)"
    else
      echo "    - Kích hoạt $WORKFLOW_FILE trên branch $CURRENT_BRANCH, tag $IMAGE_TAG (~10 phút)"
    fi
  fi
  [ "$SEED" = true ] \
    && echo "    - Seed bronze layer từ $SEED_SOURCE"
  echo "    - GHI ĐÈ scripts/.env.runtime"
  echo

  local skipped=()
  [ "$SEED" != true ]               && skipped+=("seed bronze layer (mặc định tắt, bật bằng --seed)")
  [ "$SKIP_IMAGE" = true ]          && skipped+=("build Spark image (--skip-image)")
  [ "$SKIP_ORCHESTRATION" = true ]  && skipped+=("VPC + MWAA (--skip-orchestration)")
  if [ "${#skipped[@]}" -gt 0 ]; then
    echo "Bỏ qua"
    printf '    - %s\n' "${skipped[@]}"
    echo
  fi

  local warnings=()
  if [ "$SKIP_ORCHESTRATION" != true ]; then
    warnings+=("MWAA + NAT gateway tính tiền theo giờ: ~0.6 USD/h ≈ ~430 USD/tháng nếu để nguyên.")
    warnings+=("  Xoá phần tốn tiền: bash scripts/remove_mwaa.sh")
  fi
  if [ "$SKIP_ORCHESTRATION" != true ] \
    && ! mwaa_env_exists "${TF_VAR_mwaa_environment_name:-}-dev"; then
    warnings+=("MWAA ${TF_VAR_mwaa_environment_name:-<tfvars>}-dev CHƯA TỒN TẠI, lần chạy này sẽ tạo mới.")
    warnings+=("  Đối chiếu airflow_version = ${TF_VAR_airflow_version:-<tfvars>} với dropdown")
    warnings+=("  \"Airflow version\" trong console MWAA trước khi tiếp tục. Preflight chỉ kiểm được")
    warnings+=("  định dạng X.Y.Z, không kiểm được version đó có được hỗ trợ hay không - AWS")
    warnings+=("  không có API cho việc đó. Version sai thì apply chạy 20-30 phút mới báo")
    warnings+=("  CREATE_FAILED, lúc đó NAT gateway đã tạo xong và ĐANG tính tiền.")
  fi
  if [ "$STATE_PLATFORM" = "TẠO MỚI" ]; then
    warnings+=("OIDC provider GitHub là tài nguyên account-global. Nếu account đã có một cái,")
    warnings+=("  apply sẽ dừng với lỗi EntityAlreadyExists.")
  fi
  if [ "$STATE_COMPUTE" = "TẠO MỚI" ]; then
    warnings+=("Glue database đặt tên gold/gold_staging, không có prefix - sẽ đụng database")
    warnings+=("  cùng tên nếu account đã có.")
  fi
  if [ "${#warnings[@]}" -gt 0 ]; then
    echo "Cảnh báo"
    printf '  - %s\n' "${warnings[@]}"
    echo
  fi
}

confirm() {
  if [ "$AUTO_APPROVE" = true ]; then
    echo "Bỏ qua xác nhận (--yes)."
    echo
    return 0
  fi
  local reply=""
  read -r -p "Gõ 'yes' để bắt đầu tạo hạ tầng: " reply
  if [ "$reply" != yes ]; then
    echo "Đã huỷ, không thay đổi gì."
    exit 1
  fi
  echo
}

dry_run_plan() {
  local plan_args=(-lock=false -input=false)

  echo "─── terraform plan ────────────────────────────"
  echo "[platform]"
  terraform_init_remote_backend "$TR_PLATFORM_DIR" "$TF_KEY_PLATFORM"
  terraform -chdir="$TR_PLATFORM_DIR" plan "${plan_args[@]}" \
    -var="ecr_registry_name=$ECR_REPO_NAME" \
    -var="github_repo=$GITHUB_REPO"
  echo

  local datalake_bucket_name="" codebase_bucket_name="" ecr_repository_url=""
  datalake_bucket_name="$(tf_out "$TR_PLATFORM_DIR" data_lake_bucket_name)"
  codebase_bucket_name="$(tf_out "$TR_PLATFORM_DIR" code_bucket_name)"
  ecr_repository_url="$(tf_out "$TR_PLATFORM_DIR" ecr_repository_url)"

  echo "[compute]"
  if [ -z "$datalake_bucket_name" ] || [ -z "$codebase_bucket_name" ]; then
    echo "  bỏ qua plan: cần output của platform (stack chưa apply)"
  else
    # Dry run không build gì, nên plan bằng tag sẽ được pin: khác biệt so với state
    # hiện tại chính là thứ cần nhìn thấy.
    terraform_init_remote_backend "$TR_COMPUTE_DIR" "$TF_KEY_COMPUTE"
    terraform -chdir="$TR_COMPUTE_DIR" plan "${plan_args[@]}" \
      -var="data_lake_bucket_name=$datalake_bucket_name" \
      -var="code_bucket_name=$codebase_bucket_name" \
      -var="custom_image_uri=${ecr_repository_url:+${ecr_repository_url%/}:sha-$COMMIT_SHA}"
  fi
  echo

  echo "[orchestration]"
  if [ "$SKIP_ORCHESTRATION" = true ]; then
    echo "  bỏ qua: --skip-orchestration"
  else
    local emr_application_id="" emr_execution_role_arn="" requirements_version=""
    emr_application_id="$(tf_out "$TR_COMPUTE_DIR" emr_application_id)"
    emr_execution_role_arn="$(tf_out "$TR_COMPUTE_DIR" emr_execution_role_arn)"
    requirements_version="$(tf_out "$TR_PLATFORM_DIR" mwaa_requirements_s3_object_version)"

    if [ -z "$emr_application_id" ] || [ -z "$codebase_bucket_name" ]; then
      echo "  bỏ qua plan: cần output của platform và compute (stack chưa apply)"
    else
      local orch_args=(
        -var="code_bucket_name=$codebase_bucket_name"
        -var="data_lake_bucket_name=$datalake_bucket_name"
        -var="emr_application_id=$emr_application_id"
        -var="emr_execution_role_arn=$emr_execution_role_arn"
      )
      if [ -n "$requirements_version" ]; then
        orch_args+=(-var="requirements_s3_object_version=$requirements_version")
      fi
      terraform_init_remote_backend "$TR_ORCHESTRATION_DIR" "$TF_KEY_ORCHESTRATION"
      terraform -chdir="$TR_ORCHESTRATION_DIR" plan "${plan_args[@]}" "${orch_args[@]}"
    fi
  fi
  echo

  echo "Dry run: không thay đổi gì. Bỏ --dry-run để chạy thật."
}

# ─── Các bước thực thi ─────────────────────────
step_platform() {
  begin_step platform

  tf_apply "$TR_PLATFORM_DIR" "$TF_KEY_PLATFORM" \
    -var="ecr_registry_name=$ECR_REPO_NAME" \
    -var="github_repo=$GITHUB_REPO"

  DATALAKE_BUCKET_NAME="$(tf_out "$TR_PLATFORM_DIR" data_lake_bucket_name)"
  DATALAKE_BUCKET_URI="$(tf_out "$TR_PLATFORM_DIR" data_lake_bucket_uri)"
  CODEBASE_BUCKET_NAME="$(tf_out "$TR_PLATFORM_DIR" code_bucket_name)"
  ECR_REPOSITORY_URL="$(tf_out "$TR_PLATFORM_DIR" ecr_repository_url)"
  GH_ACTIONS_ROLE_ARN="$(tf_out "$TR_PLATFORM_DIR" github_actions_role_arn)"
  # Version id của airflow/requirements.txt: đổi nó là thứ khiến MWAA cài lại package.
  # Optional: bucket chưa bật versioning thì rỗng, orchestration bỏ qua -var này.
  REQUIREMENTS_VERSION="$(tf_out "$TR_PLATFORM_DIR" mwaa_requirements_s3_object_version)"

  require_output data_lake_bucket_name "$DATALAKE_BUCKET_NAME"
  require_output data_lake_bucket_uri "$DATALAKE_BUCKET_URI"
  require_output code_bucket_name "$CODEBASE_BUCKET_NAME"
  require_output ecr_repository_url "$ECR_REPOSITORY_URL"
  require_output github_actions_role_arn "$GH_ACTIONS_ROLE_ARN"

  printf '  %-22s %s\n' "data lake:" "$DATALAKE_BUCKET_NAME"
  printf '  %-22s %s\n' "code bucket:" "$CODEBASE_BUCKET_NAME"
  printf '  %-22s %s\n' "ECR:" "$ECR_REPOSITORY_URL"
  printf '  %-22s %s\n' "requirements version:" "${REQUIREMENTS_VERSION:-(không có)}"
  printf '  %-22s %s\n' "GH Actions role:" "$GH_ACTIONS_ROLE_ARN"

  # Script không ghi secret hộ. ARN chỉ đổi khi stack platform được tạo lại, nên chỉ
  # nhắc đúng lúc đó - preflight đã chặn trường hợp secret chưa tồn tại.
  if [ "$STATE_PLATFORM" = "TẠO MỚI" ] && [ "$SKIP_IMAGE" != true ]; then
    echo "  platform vừa được tạo mới -> ARN ở trên là ARN mới. Nếu secret đang giữ giá trị"
    echo "  cũ, CI sẽ fail ở bước OIDC. Cập nhật bằng:"
    echo "    gh secret set AWS_GITHUB_ACTIONS_ROLE_ARN --repo $GITHUB_REPO --body '$GH_ACTIONS_ROLE_ARN'"
  fi
  finish_step
}

step_seed() {
  if [ "$SEED" != true ]; then
    skip_step seed "mặc định tắt, bật bằng --seed"
    return 0
  fi
  begin_step seed

  echo "  $SEED_SOURCE -> s3://$DATALAKE_BUCKET_NAME/bronze/"
  aws s3 sync "$SEED_SOURCE" "s3://$DATALAKE_BUCKET_NAME/bronze/" --no-progress
  finish_step
}

# Id của run mới nhất cho đúng commit này. Dùng để phân biệt run vừa dispatch với
# run cũ của cùng commit: databaseId tăng dần nên chỉ cần so lớn hơn.
# Luôn trả về một số: mọi lỗi gh/jq đều quy về 0 để phép so sánh số không vỡ.
latest_run_id_for_commit() {
  local out=""
  out="$(gh run list \
    --repo "$GITHUB_REPO" \
    --workflow="$WORKFLOW_FILE" \
    --branch "$CURRENT_BRANCH" \
    --event workflow_dispatch \
    --limit 30 \
    --json databaseId,headSha \
    --jq "[.[] | select(.headSha == \"$COMMIT_SHA\") | .databaseId] | max // 0" 2>/dev/null)" || out=""
  [[ "$out" =~ ^[0-9]+$ ]] || out=0
  echo "$out"
}

step_image() {
  local pinned_tag="sha-$COMMIT_SHA"

  # --skip-image = bỏ qua vòng build CI, dùng luôn image mới nhất đang có trong ECR.
  # EMR vẫn được gắn image, chỉ là image đó không nhất thiết ứng với commit hiện tại.
  if [ "$SKIP_IMAGE" = true ]; then
    CURRENT_STEP="image"
    local newest_tag=""
    if ! newest_tag="$(ecr_newest_image_tag)"; then
      # Không được đi tiếp với chuỗi rỗng: module bỏ luôn block image_configuration
      # khi custom_image_uri="", tức là GỠ image khỏi application chứ không phải
      # giữ nguyên. Mà job Spark cần venv /home/hadoop/environment trong image riêng.
      STEP_STATUS[image]="failed"
      echo "ECR ${ECR_REPO_NAME}-dev chưa có image nào để dùng." >&2
      echo "Bỏ --skip-image để build image đầu tiên." >&2
      return 1
    fi
    IMAGE_URI="${ECR_REPOSITORY_URL%/}:$newest_tag"
    CURRENT_STEP=""
    skip_step image "--skip-image, dùng image mới nhất trong ECR: $newest_tag"
    return 0
  fi

  begin_step image
  IMAGE_URI="${ECR_REPOSITORY_URL%/}:$pinned_tag"

  if [ "$FORCE_IMAGE" != true ] && ecr_tag_exists "$pinned_tag"; then
    echo "  ECR đã có image của commit này, bỏ qua CI run (--force-image để ép build)"
    echo "  $IMAGE_URI"
    finish_step
    return 0
  fi

  local before_id=0 run_id=0
  before_id="$(latest_run_id_for_commit)"
  run_id="$before_id"

  echo "  dispatch $WORKFLOW_FILE (ref=$CURRENT_BRANCH, image_tag=$IMAGE_TAG)"
  gh workflow run "$WORKFLOW_FILE" \
    --repo "$GITHUB_REPO" \
    --ref "$CURRENT_BRANCH" \
    -f image_tag="$IMAGE_TAG"

  # GitHub mất vài giây mới đăng ký run. Bám theo commit sha + id lớn hơn id trước khi
  # dispatch, để không bao giờ watch nhầm một run cũ.
  local deadline=$((SECONDS + 120))
  echo -n "  chờ GitHub đăng ký run"
  while [ "$SECONDS" -lt "$deadline" ]; do
    run_id="$(latest_run_id_for_commit)"
    if [ "$run_id" -gt "$before_id" ]; then
      break
    fi
    echo -n "."
    sleep 5
  done
  echo

  if [ "$run_id" -le "$before_id" ]; then
    echo "Không thấy workflow run mới cho commit ${COMMIT_SHA:0:12} sau 120s." >&2
    echo "Kiểm tra: gh run list --repo $GITHUB_REPO --workflow=$WORKFLOW_FILE" >&2
    return 1
  fi

  echo "  theo dõi run #$run_id"
  gh run watch "$run_id" --repo "$GITHUB_REPO" --exit-status

  # CI xanh nhưng tag không có nghĩa là workflow push vào repo ECR khác (tên repo bị
  # hardcode trong workflow). Bắt ở đây thay vì để EMR fail lúc chạy job.
  ecr_tag_exists "$pinned_tag" || {
    echo "CI xong nhưng ECR không có tag $pinned_tag trong ${ECR_REPO_NAME}-dev." >&2
    echo "Kiểm tra ECR_REPO_NAME trong .github/workflows/$WORKFLOW_FILE có khớp không." >&2
    return 1
  }

  echo "  $IMAGE_URI"
  finish_step
}

step_compute() {
  begin_step compute

  tf_apply "$TR_COMPUTE_DIR" "$TF_KEY_COMPUTE" \
    -var="data_lake_bucket_name=$DATALAKE_BUCKET_NAME" \
    -var="code_bucket_name=$CODEBASE_BUCKET_NAME" \
    -var="custom_image_uri=$IMAGE_URI"

  EMR_APPLICATION_ID="$(tf_out "$TR_COMPUTE_DIR" emr_application_id)"
  EMR_EXECUTION_ROLE_ARN="$(tf_out "$TR_COMPUTE_DIR" emr_execution_role_arn)"
  # Job phải khai đúng log group này trong monitoringConfiguration: policy
  # EMRServerlessCloudWatchLogsDev chỉ cho ghi vào đúng nó.
  EMR_LOG_GROUP_NAME="$(tf_out "$TR_COMPUTE_DIR" emr_log_group_name)"
  require_output emr_application_id "$EMR_APPLICATION_ID"
  require_output emr_execution_role_arn "$EMR_EXECUTION_ROLE_ARN"
  require_output emr_log_group_name "$EMR_LOG_GROUP_NAME"

  printf '  %-22s %s\n' "EMR application:" "$EMR_APPLICATION_ID"
  printf '  %-22s %s\n' "execution role:" "$EMR_EXECUTION_ROLE_ARN"
  printf '  %-22s %s\n' "log group:" "$EMR_LOG_GROUP_NAME"
  finish_step
}

# Chạy TRƯỚC orchestration: MWAA mất ~30 phút và có thể fail, nhưng các script job
# (deploy_silver_job_dev.sh, gold-dbt/*) chỉ cần platform + compute là chạy được.
step_runtime_env() {
  begin_step runtime_env

  # Cùng tên wheel mà refresh_runtime_env_dev.sh và deploy_silver_job_dev.sh dùng.
  local py_pkg="${py_package:-${PY_PACKAGE:-aws_pipeline-0.0.1-py3-none-any.whl}}"

  cat > "$RUNTIME_ENV" <<EOF
# Sinh bởi scripts/_setup_infra.sh lúc $(date -u '+%Y-%m-%dT%H:%M:%SZ')
# account=$AWS_ACCOUNT region=$AWS_REGION branch=$CURRENT_BRANCH commit=$COMMIT_SHA
# Đừng sửa tay trừ khi cố ý override giá trị runtime của dev.
DATALAKE_BUCKET_NAME=$DATALAKE_BUCKET_NAME
CODEBASE_BUCKET_NAME=$CODEBASE_BUCKET_NAME
FULL_ECR_IMAGE_URI=$IMAGE_URI
EXECUTION_ROLE_ARN=$EMR_EXECUTION_ROLE_ARN
APPLICATION_ID=$EMR_APPLICATION_ID
DATALAKE_BUCKET_URI=$DATALAKE_BUCKET_URI
S3_PY_PACKAGE=s3://$CODEBASE_BUCKET_NAME/packages/$py_pkg
EMR_LOG_GROUP_NAME=$EMR_LOG_GROUP_NAME
EOF

  echo "  đã ghi $RUNTIME_ENV"
  finish_step
}

step_orchestration() {
  if [ "$SKIP_ORCHESTRATION" = true ]; then
    skip_step orchestration "--skip-orchestration"
    return 0
  fi
  begin_step orchestration

  # MWAA đọc DAG từ s3://<code_bucket>/dags/, prefix do stack platform tạo.
  # requirements.txt do terraform upload, không đẩy ở đây.
  if [ -d "$AIRFLOW_DIR/dags" ]; then
    echo "  sync DAG -> s3://$CODEBASE_BUCKET_NAME/dags/"
    aws s3 sync "$AIRFLOW_DIR/dags/" "s3://$CODEBASE_BUCKET_NAME/dags/" \
      --exclude "__pycache__/*" \
      --delete \
      --no-progress
  else
    echo "  chưa có $AIRFLOW_DIR/dags, bỏ qua sync DAG"
  fi

  local orch_args=(
    -var="code_bucket_name=$CODEBASE_BUCKET_NAME"
    -var="data_lake_bucket_name=$DATALAKE_BUCKET_NAME"
    -var="emr_application_id=$EMR_APPLICATION_ID"
    -var="emr_execution_role_arn=$EMR_EXECUTION_ROLE_ARN"
  )
  if [ -n "$REQUIREMENTS_VERSION" ]; then
    orch_args+=(-var="requirements_s3_object_version=$REQUIREMENTS_VERSION")
  fi

  echo "  apply VPC + NAT gateway + MWAA (~30 phút, bắt đầu tính tiền từ đây)"
  tf_apply "$TR_ORCHESTRATION_DIR" "$TF_KEY_ORCHESTRATION" "${orch_args[@]}"

  MWAA_WEBSERVER_URL="$(tf_out "$TR_ORCHESTRATION_DIR" mwaa_webserver_url)"
  # Lấy từ output chứ không tự ghép "$TF_VAR_mwaa_environment_name-dev": hậu tố do
  # terraform thêm, output là tên thật của environment.
  local mwaa_env_name=""
  mwaa_env_name="$(tf_out "$TR_ORCHESTRATION_DIR" mwaa_environment_name)"

  # Append chứ không ghi lại cả file: step_runtime_env đã chạy xong từ trước MWAA.
  {
    echo "MWAA_ENVIRONMENT_NAME=$mwaa_env_name"
    echo "MWAA_WEBSERVER_URL=$MWAA_WEBSERVER_URL"
  } >> "$RUNTIME_ENV"

  printf '  %-22s %s\n' "Airflow UI:" "${MWAA_WEBSERVER_URL:-(output rỗng)}"
  finish_step
}

summary() {
  echo "─── Xong ──────────────────────────────────────"
  print_status_table
  echo
  printf '  %-22s %s\n' "Account/region:" "$AWS_ACCOUNT / $AWS_REGION"
  printf '  %-22s %s\n' "EMR application:" "${EMR_APPLICATION_ID:-(không có)}"
  printf '  %-22s %s\n' "Spark image:" "${IMAGE_URI:-(không có)}"
  printf '  %-22s %s\n' "Runtime env:" "$RUNTIME_ENV"
  if [ "$SKIP_ORCHESTRATION" != true ]; then
    printf '  %-22s %s\n' "Airflow UI:" "${MWAA_WEBSERVER_URL:-(không có)}"
  fi
  printf '  %-22s %s\n' "Tổng thời gian:" "$((SECONDS / 60))m $((SECONDS % 60))s"
  echo

  echo "Chạy tiếp:"
  echo "  bash scripts/deploy_silver_job_dev.sh          # bronze -> silver (trả lời y để build wheel)"
  echo "  bash scripts/gold-dbt/deploy_gold_dbt_dev.sh   # silver -> gold"
  if [ "$SKIP_ORCHESTRATION" != true ]; then
    echo
    echo "MWAA + NAT gateway đang tính tiền ~0.6 USD/h. Xong việc thì xoá:"
    echo "  bash scripts/remove_mwaa.sh"
  fi
}

main() {
  parse_args "$@"
  show_args
  preflight
  show_plan

  if [ "$DRY_RUN" = true ]; then
    dry_run_plan
    exit 0
  fi

  confirm

  step_platform
  step_seed
  step_image
  step_compute
  step_runtime_env
  step_orchestration
  summary
}

# Luôn là dòng cuối cùng của file: mọi hàm và biến phải được định nghĩa trước.
trap on_err ERR
main "$@"
