#!/usr/bin/env bash
set -euo pipefail

# Create/release an Alibaba Cloud ECS research machine with the same Python
# dependency profile used by the factor/backtest base image.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
STATE_DIR="${STATE_DIR:-${REPO_ROOT}/.aliyun-research-ecs}"
STATE_FILE="${STATE_FILE:-${STATE_DIR}/instance.env}"
CONFIG_FILE="${CONFIG_FILE:-${STATE_DIR}/config.env}"
LOCAL_BIN_DIR="${LOCAL_BIN_DIR:-${STATE_DIR}/bin}"
PATH="${LOCAL_BIN_DIR}:${PATH}"

if [[ -f "${CONFIG_FILE}" ]]; then
  # shellcheck disable=SC1090
  source "${CONFIG_FILE}"
fi

REGION_ID="${REGION_ID:-cn-hangzhou}"
ZONE_ID="${ZONE_ID:-}"
VPC_ID="${VPC_ID:-vpc-bp1wkj2li6xquauq5ccc5}"
INSTANCE_NAME="${INSTANCE_NAME:-quant-research-$(date +%Y%m%d-%H%M%S)}"
HOST_NAME="${HOST_NAME:-quant-research}"
IMAGE_ID="${IMAGE_ID:-${ALIYUN_IMAGE_ID:-}}"
INSTANCE_TYPE="${INSTANCE_TYPE:-ecs.c9i.8xlarge}"
FALLBACK_INSTANCE_TYPES="${FALLBACK_INSTANCE_TYPES:-ecs.c9i.4xlarge ecs.g8i.8xlarge ecs.g8i.4xlarge ecs.c8i.8xlarge ecs.c8i.4xlarge}"
SECURITY_GROUP_ID="${SECURITY_GROUP_ID:-${ALIYUN_SECURITY_GROUP_ID:-}}"
VSWITCH_ID="${VSWITCH_ID:-${ALIYUN_VSWITCH_ID:-}}"
KEY_PAIR_NAME="${KEY_PAIR_NAME:-}"
SYSTEM_DISK_CATEGORY="${SYSTEM_DISK_CATEGORY:-cloud_essd}"
SYSTEM_DISK_SIZE="${SYSTEM_DISK_SIZE:-200}"
INTERNET_MAX_BANDWIDTH_OUT="${INTERNET_MAX_BANDWIDTH_OUT:-10}"
PASSWORD_INHERIT="${PASSWORD_INHERIT:-false}"
AUTO_RELEASE_TIME="${AUTO_RELEASE_TIME:-}"

BILLING_MODE="${BILLING_MODE:-spot}" # spot | postpaid
SPOT_STRATEGY="${SPOT_STRATEGY:-SpotAsPriceGo}"
SPOT_PRICE_LIMIT="${SPOT_PRICE_LIMIT:-}"

INSTALL_DOCKER="${INSTALL_DOCKER:-true}"
INSTALL_ALIYUN_CLI="${INSTALL_ALIYUN_CLI:-true}"
BACKTEST_IMAGE="${BACKTEST_IMAGE:-172.24.99.176:5000/quant-platform/backtest-base:latest}"
DOCKER_PULL_BACKTEST_IMAGE="${DOCKER_PULL_BACKTEST_IMAGE:-false}"
CREATE_DEV_CONTAINER="${CREATE_DEV_CONTAINER:-false}"
DEV_USER="${DEV_USER:-research}"
DEV_PASSWORD="${DEV_PASSWORD:-}"
GENERATE_DEV_PASSWORD="${GENERATE_DEV_PASSWORD:-true}"
SSH_PASSWORD_AUTH="${SSH_PASSWORD_AUTH:-true}"

# 目录加密：保护 home 下某子目录，磁盘上只存密文，密钥由交易员持有。
# 关键约束：fscrypt 口令绝不进 UserData/cloud-init（UserData 对主账号可见），
# 仅在 create 流程内经 SSH 通道下发到远端 fscrypt，机器上只留 protector 元数据。
ENCRYPT_HOME_SUBDIR="${ENCRYPT_HOME_SUBDIR:-true}"
ENCRYPTED_DIR="${ENCRYPTED_DIR:-/home/${DEV_USER}/private}"
LIFETIME_HOURS="${LIFETIME_HOURS:-}"   # 存活小时数，create 时转成 AUTO_RELEASE_TIME 自动释放

usage() {
  cat <<'EOF'
Usage:
  scripts/aliyun_research_ecs.sh create [--spot|--postpaid]
  scripts/aliyun_research_ecs.sh release
  scripts/aliyun_research_ecs.sh status
  scripts/aliyun_research_ecs.sh ssh
  scripts/aliyun_research_ecs.sh reset-password
  scripts/aliyun_research_ecs.sh init-config

Required environment for create:
  None if aliyun CLI is already configured.
  The script can auto-select Ubuntu 22.04 image, first VSwitch, and first security group.

Common optional environment:
  REGION_ID=cn-hangzhou
  VPC_ID=vpc-bp1wkj2li6xquauq5ccc5
  ZONE_ID=cn-hangzhou-i
  INSTANCE_TYPE=ecs.c9i.8xlarge
  BILLING_MODE=spot|postpaid        default: spot
  SYSTEM_DISK_SIZE=200
  INTERNET_MAX_BANDWIDTH_OUT=10
  AUTO_RELEASE_TIME=2026-06-11T10:00:00Z
  DEV_USER=research
  DEV_PASSWORD='...'                optional fixed login password for DEV_USER
  GENERATE_DEV_PASSWORD=true        generate one when DEV_PASSWORD is empty
  SSH_PASSWORD_AUTH=true            enable SSH password login
  INSTALL_ALIYUN_CLI=true           auto-install aliyun CLI under .aliyun-research-ecs/bin
  INSTALL_DOCKER=true
  DOCKER_PULL_BACKTEST_IMAGE=false
  CREATE_DEV_CONTAINER=false
  BACKTEST_IMAGE=172.24.99.176:5000/quant-platform/backtest-base:latest
  ENCRYPT_HOME_SUBDIR=true          fscrypt-encrypt ~/private at create time
  ENCRYPTED_DIR=/home/$DEV_USER/private   directory to encrypt (passphrase prompted at create)
  LIFETIME_HOURS=                   auto-release after N hours (blank = no auto-release)

Security:
  - UFW firewall blocks SSH from VPC private IPs (10/8, 172.16/12, 192.168/16)
  - SSH server restricts login to DEV_USER only; root login disabled
  - /home/DEV_USER and /opt/quant-platform set to 0700
  - ~/private (ENCRYPTED_DIR) is fscrypt-encrypted at create time; passphrase is yours,
    never written to UserData / config / state — flows via SSH channel only

Examples:
  scripts/aliyun_research_ecs.sh create
  scripts/aliyun_research_ecs.sh ssh
  scripts/aliyun_research_ecs.sh release

Optional one-time defaults file:
  .aliyun-research-ecs/config.env
EOF
}

die() {
  echo "ERROR: $*" >&2
  exit 1
}

prompt_value() {
  local var_name="$1"
  local label="$2"
  local default_value="${3:-}"
  local secret="${4:-false}"
  local value

  if [[ "${secret}" == "true" ]]; then
    read -r -s -p "${label}${default_value:+ [default hidden]}: " value
    echo >&2
  else
    read -r -p "${label}${default_value:+ [${default_value}]}: " value
  fi
  value="${value:-${default_value}}"
  printf -v "${var_name}" '%s' "${value}"
}

save_config() {
  mkdir -p "${STATE_DIR}"
  cat > "${CONFIG_FILE}" <<EOF
REGION_ID=${REGION_ID}
ZONE_ID=${ZONE_ID}
VPC_ID=${VPC_ID}
IMAGE_ID=${IMAGE_ID}
SECURITY_GROUP_ID=${SECURITY_GROUP_ID}
VSWITCH_ID=${VSWITCH_ID}
KEY_PAIR_NAME=${KEY_PAIR_NAME}
BILLING_MODE=${BILLING_MODE}
INSTANCE_TYPE=${INSTANCE_TYPE}
DEV_USER=${DEV_USER}
DEV_PASSWORD=${DEV_PASSWORD}
GENERATE_DEV_PASSWORD=${GENERATE_DEV_PASSWORD}
SSH_PASSWORD_AUTH=${SSH_PASSWORD_AUTH}
AUTO_RELEASE_TIME=${AUTO_RELEASE_TIME}
ENCRYPT_HOME_SUBDIR=${ENCRYPT_HOME_SUBDIR}
ENCRYPTED_DIR=${ENCRYPTED_DIR}
EOF
  chmod 600 "${CONFIG_FILE}"
}

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "missing command: $1"
}

install_aliyun_cli() {
  [[ "${INSTALL_ALIYUN_CLI}" == "true" ]] || return 1
  need_cmd curl
  need_cmd tar

  local os arch url tmp tgz extracted
  case "$(uname -s)" in
    Darwin) os="macosx" ;;
    Linux) os="linux" ;;
    *) die "unsupported OS for auto-installing aliyun CLI: $(uname -s)" ;;
  esac

  case "$(uname -m)" in
    x86_64|amd64) arch="amd64" ;;
    arm64|aarch64) arch="arm64" ;;
    *) arch="amd64" ;;
  esac

  mkdir -p "${LOCAL_BIN_DIR}"
  tmp="$(mktemp -d "${TMPDIR:-/tmp}/aliyun-cli.XXXXXX")"
  tgz="${tmp}/aliyun-cli.tgz"

  url="https://aliyuncli.alicdn.com/aliyun-cli-${os}-latest-${arch}.tgz"
  echo "未找到阿里云 CLI，正在安装到 ${LOCAL_BIN_DIR}..."
  if ! curl -fsSL "${url}" -o "${tgz}"; then
    if [[ "${arch}" != "amd64" ]]; then
      url="https://aliyuncli.alicdn.com/aliyun-cli-${os}-latest-amd64.tgz"
      curl -fsSL "${url}" -o "${tgz}"
    else
      return 1
    fi
  fi

  tar -xzf "${tgz}" -C "${tmp}"
  extracted="$(find "${tmp}" -type f -name aliyun | head -n 1)"
  [[ -n "${extracted}" ]] || die "failed to find aliyun binary in downloaded archive"
  cp "${extracted}" "${LOCAL_BIN_DIR}/aliyun"
  chmod +x "${LOCAL_BIN_DIR}/aliyun"
  rm -rf "${tmp}"
  command -v aliyun >/dev/null 2>&1 || die "aliyun CLI install completed but binary is not on PATH"
}

ensure_aliyun_cli() {
  if ! command -v aliyun >/dev/null 2>&1; then
    install_aliyun_cli || die "aliyun CLI is required and auto-install failed"
  fi
}

ensure_aliyun_config() {
  local ok=false
  if aliyun configure get region >/dev/null 2>&1; then
    ok=true
  elif aliyun ecs DescribeRegions >/dev/null 2>&1; then
    ok=true
  fi
  [[ "${ok}" == "true" ]] && return 0

  echo "阿里云 CLI 还没有配置，请输入阿里云账号的 AK/SK。" >&2
  local access_key_id access_key_secret
  prompt_value access_key_id "请输入 AccessKey ID"
  prompt_value access_key_secret "请输入 AccessKey Secret" "" true
  REGION_ID="${REGION_ID:-cn-hangzhou}"

  [[ -n "${access_key_id}" ]] || die "ALIYUN_ACCESS_KEY_ID is required"
  [[ -n "${access_key_secret}" ]] || die "ALIYUN_ACCESS_KEY_SECRET is required"

  aliyun configure set \
    --profile default \
    --mode AK \
    --region "${REGION_ID}" \
    --access-key-id "${access_key_id}" \
    --access-key-secret "${access_key_secret}"
}

json_get() {
  local expr="$1"
  python3 -c 'import json,sys
data=json.load(sys.stdin)
expr=sys.argv[1]
cur=data
for part in expr.split("."):
    if part.endswith("]"):
        name, idx = part[:-1].split("[", 1)
        cur = cur[name][int(idx)]
    else:
        cur = cur[part]
print(cur)' "$expr"
}

image_size_gb() {
  local image_id="$1"
  aliyun ecs DescribeImages \
    --RegionId "${REGION_ID}" \
    --ImageId "${image_id}" \
  | python3 -c 'import json, math, sys
d=json.load(sys.stdin)
imgs=d.get("Images",{}).get("Image",[])
size=0
if imgs:
    raw=imgs[0].get("Size") or imgs[0].get("ImageSize") or 0
    try:
        size=float(raw)
    except Exception:
        size=0
print(int(math.ceil(size)))'
}

image_info() {
  local image_id="$1"
  aliyun ecs DescribeImages \
    --RegionId "${REGION_ID}" \
    --ImageId "${image_id}" \
  | python3 -c 'import json, sys
d=json.load(sys.stdin)
imgs=d.get("Images",{}).get("Image",[])
if not imgs:
    print("unknown\t0\tunknown")
else:
    img=imgs[0]
    print("{}\t{}\t{}".format(
        img.get("ImageName",""),
        img.get("Size") or img.get("ImageSize") or 0,
        img.get("CreationTime",""),
    ))'
}

save_state() {
  mkdir -p "${STATE_DIR}"
  cat > "${STATE_FILE}" <<EOF
INSTANCE_ID=${INSTANCE_ID}
REGION_ID=${REGION_ID}
PUBLIC_IP=${PUBLIC_IP}
PRIVATE_IP=${PRIVATE_IP}
DEV_USER=${DEV_USER}
DEV_PASSWORD=${DEV_PASSWORD}
INSTANCE_NAME=${INSTANCE_NAME}
EOF
  chmod 600 "${STATE_FILE}"
}

load_state() {
  [[ -f "${STATE_FILE}" ]] || die "state file not found: ${STATE_FILE}"
  # shellcheck disable=SC1090
  source "${STATE_FILE}"
}

validate_create_config() {
  ensure_aliyun_cli
  ensure_aliyun_config
  need_cmd python3
}

select_first_json_value() {
  local path="$1"
  python3 -c 'import json,sys
data=json.load(sys.stdin)
cur=data
for part in sys.argv[1].split("."):
    cur = cur.get(part, {}) if isinstance(cur, dict) else {}
if isinstance(cur, list) and cur:
    item = cur[0]
    if isinstance(item, dict):
        for key in ("ImageId", "VSwitchId", "SecurityGroupId"):
            if key in item:
                print(item[key])
                break
' "$path"
}

list_vswitch_ids() {
  aliyun vpc DescribeVSwitches --RegionId "${REGION_ID}" --VpcId "${VPC_ID}" \
  | python3 -c 'import json,sys
d=json.load(sys.stdin)
items=d.get("VSwitches",{}).get("VSwitch",[])
items=sorted(items, key=lambda x: (x.get("ZoneId",""), x.get("VSwitchId","")))
for item in items:
    vsw=item.get("VSwitchId")
    if vsw:
        print(vsw)'
}

list_security_group_ids() {
  aliyun ecs DescribeSecurityGroups --RegionId "${REGION_ID}" --VpcId "${VPC_ID}" \
  | python3 -c 'import json,sys
d=json.load(sys.stdin)
items=d.get("SecurityGroups",{}).get("SecurityGroup",[])
for item in items:
    text=" ".join(str(item.get(k,"")) for k in (
        "SecurityGroupId", "SecurityGroupName", "Description",
        "ServiceManaged", "ServiceID", "ResourceGroupId"
    ))
    if "ALB" in text.upper() or "SERVICE" in str(item.get("ServiceManaged","")).upper():
        continue
    sg=item.get("SecurityGroupId")
    if sg:
        print(sg)'
}

resolve_create_defaults() {
  prompt_value INSTANCE_TYPE "实例规格" "${INSTANCE_TYPE}"
  prompt_value BILLING_MODE "计费方式：spot 抢占式 / postpaid 按量" "${BILLING_MODE}"
  prompt_value DEV_USER "登录用户名" "${DEV_USER}"
  if [[ -z "${DEV_PASSWORD}" ]]; then
    local password_choice
    prompt_value password_choice "是否自动生成登录密码？y/n" "y"
    if [[ "${password_choice}" =~ ^[Nn] ]]; then
      GENERATE_DEV_PASSWORD=false
      prompt_value DEV_PASSWORD "请输入登录密码" "" true
    fi
  fi

  # 存活时长：到点自动释放（阿里云 AutoReleaseTime，ISO8601 UTC）
  local lifetime_hours
  prompt_value lifetime_hours "存活时长（小时，到时自动释放；留空则不自动释放）" "${LIFETIME_HOURS:-}"
  if [[ -n "${lifetime_hours}" ]]; then
    if [[ "${lifetime_hours}" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
      # 取整小时；macOS 用 date -v，Linux 用 date -d
      local whole_hours
      whole_hours="$(python3 -c "import math,sys; print(int(math.ceil(float(sys.argv[1]))))" "${lifetime_hours}")"
      AUTO_RELEASE_TIME="$(date -u -v+${whole_hours}H +%Y-%m-%dT%H:%M:%SZ 2>/dev/null \
        || date -u -d "+${whole_hours} hours" +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || true)"
      if [[ -n "${AUTO_RELEASE_TIME}" ]]; then
        echo "机器将于 ${AUTO_RELEASE_TIME}（UTC）自动释放" >&2
      else
        echo "WARN: 无法把存活时长转成释放时间，已忽略自动释放" >&2
        AUTO_RELEASE_TIME=""
      fi
    else
      echo "WARN: 存活时长 '${lifetime_hours}' 不是数字，已忽略自动释放" >&2
      AUTO_RELEASE_TIME=""
    fi
  else
    AUTO_RELEASE_TIME=""
  fi

  # 目录加密：保护 home 下某子目录，密钥由交易员持有，绝不进 UserData
  local enc_default="n"
  [[ "${ENCRYPT_HOME_SUBDIR}" == "true" ]] && enc_default="y"
  local enc_choice
  prompt_value enc_choice "是否加密保护目录（fscrypt）？y/n" "${enc_default}"
  if [[ "${enc_choice}" =~ ^[Yy] ]]; then
    ENCRYPT_HOME_SUBDIR=true
    prompt_value ENCRYPTED_DIR "要加密的保护目录" "${ENCRYPTED_DIR}"
    # 校验：必须绝对路径；不能是 home 根目录本身（加密整个 home 会锁死 .ssh/.bashrc，重启后无法登录）
    [[ "${ENCRYPTED_DIR}" == /* ]] || die "保护目录必须是绝对路径（以 / 开头），当前输入：${ENCRYPTED_DIR}"
    case "${ENCRYPTED_DIR}" in
      "/home/${DEV_USER}"|"/home/${DEV_USER}/") \
        die "不能加密整个 home 目录（${ENCRYPTED_DIR}），会锁死 .ssh/.bashrc 导致重启后无法登录；请用 home 下子目录，如 /home/${DEV_USER}/private" ;;
    esac
    # 口令用 read -s 收集，绝不写入 config/state/日志；用完即 unset
    prompt_value FSCRYPT_PASSPHRASE "fscrypt 加密口令（不回显，请妥善备份，丢失无法恢复）" "" true
    [[ -n "${FSCRYPT_PASSPHRASE}" ]] || die "加密口令不能为空"
    local pp_confirm
    prompt_value pp_confirm "再次输入口令确认" "" true
    [[ "${pp_confirm}" == "${FSCRYPT_PASSPHRASE}" ]] || die "两次口令不一致"
    unset pp_confirm
  else
    ENCRYPT_HOME_SUBDIR=false
  fi

  if [[ -n "${IMAGE_ID}" ]]; then
    local existing_image_size
    existing_image_size="$(image_size_gb "${IMAGE_ID}")" || existing_image_size=0
    if [[ "${existing_image_size}" =~ ^[0-9]+$ ]] && (( existing_image_size > 80 )); then
      echo "已配置的镜像 ${IMAGE_ID} 需要 ${existing_image_size}G 系统盘，正在改选更小的基础镜像。" >&2
      IMAGE_ID=""
    fi
  fi

  if [[ -z "${IMAGE_ID}" ]]; then
    echo "正在自动选择 ${REGION_ID} 的 Ubuntu 22.04 20G 基础镜像..." >&2
    IMAGE_ID="$(
      aliyun ecs DescribeImages \
        --RegionId "${REGION_ID}" \
        --ImageOwnerAlias system \
        --OSType linux \
        --Architecture x86_64 \
        --ImageName 'ubuntu_22_04_x64_20G_alibase*' \
      | python3 -c 'import json,sys
d=json.load(sys.stdin)
imgs=d.get("Images",{}).get("Image",[])
def size(img):
    try:
        return float(img.get("Size") or img.get("ImageSize") or 0)
    except Exception:
        return 0
imgs=[img for img in imgs if size(img) <= 80]
imgs=sorted(imgs, key=lambda x: x.get("CreationTime",""), reverse=True)
print(imgs[0].get("ImageId","") if imgs else "")'
    )" || IMAGE_ID=""
    if [[ -z "${IMAGE_ID}" ]]; then
      echo "未找到 20G 基础镜像，正在尝试选择 80G 以下的 Ubuntu 22.04 镜像..." >&2
      IMAGE_ID="$(
        aliyun ecs DescribeImages \
          --RegionId "${REGION_ID}" \
          --ImageOwnerAlias system \
          --OSType linux \
          --Architecture x86_64 \
          --ImageName 'ubuntu_22_04_x64*' \
        | python3 -c 'import json,sys
d=json.load(sys.stdin)
imgs=d.get("Images",{}).get("Image",[])
def size(img):
    try:
        return float(img.get("Size") or img.get("ImageSize") or 0)
    except Exception:
        return 0
imgs=[img for img in imgs if size(img) <= 80]
imgs=sorted(imgs, key=lambda x: x.get("CreationTime",""), reverse=True)
print(imgs[0].get("ImageId","") if imgs else "")'
      )" || IMAGE_ID=""
    fi
    if [[ -z "${IMAGE_ID}" ]]; then
      prompt_value IMAGE_ID "未找到合适的小镜像，请输入 IMAGE_ID"
    fi
  fi

  if [[ -z "${VSWITCH_ID}" ]]; then
    echo "正在自动选择交换机：${REGION_ID}，VPC ${VPC_ID}..." >&2
    VSWITCH_ID="$(aliyun vpc DescribeVSwitches --RegionId "${REGION_ID}" --VpcId "${VPC_ID}" | select_first_json_value 'VSwitches.VSwitch')" || VSWITCH_ID=""
    if [[ -z "${VSWITCH_ID}" ]]; then
      prompt_value VSWITCH_ID "自动选择交换机失败，请输入 VSWITCH_ID"
    fi
  fi

  if [[ -z "${SECURITY_GROUP_ID}" ]]; then
    echo "正在自动选择安全组：${REGION_ID}，VPC ${VPC_ID}..." >&2
    SECURITY_GROUP_ID="$(list_security_group_ids | head -n 1)" || SECURITY_GROUP_ID=""
    if [[ -z "${SECURITY_GROUP_ID}" ]]; then
      prompt_value SECURITY_GROUP_ID "自动选择安全组失败，请输入 SECURITY_GROUP_ID"
    fi
  fi

  [[ -n "${IMAGE_ID}" ]] || die "IMAGE_ID is required; auto-select failed"
  [[ -n "${SECURITY_GROUP_ID}" ]] || die "SECURITY_GROUP_ID is required; auto-select failed"
  [[ -n "${VSWITCH_ID}" ]] || die "VSWITCH_ID is required; auto-select failed"

  local min_image_size
  min_image_size="$(image_size_gb "${IMAGE_ID}")" || min_image_size=0
  local selected_image_info
  selected_image_info="$(image_info "${IMAGE_ID}")" || selected_image_info="unknown	${min_image_size}	unknown"
  echo "已选择镜像：${IMAGE_ID} (${selected_image_info})" >&2
  if [[ "${min_image_size}" =~ ^[0-9]+$ ]] && (( min_image_size > 0 && SYSTEM_DISK_SIZE < min_image_size )); then
    SYSTEM_DISK_SIZE=$((min_image_size + 20))
    echo "镜像需要更大的系统盘，已自动调整为 ${SYSTEM_DISK_SIZE}G。" >&2
  fi
}

make_cloud_init() {
  local out="$1"
  local req_b64
  req_b64="$(base64 < "${REPO_ROOT}/requirements.txt" | tr -d '\n')"

  cat > "${out}" <<EOF
#cloud-config
package_update: false
ssh_pwauth: ${SSH_PASSWORD_AUTH}
chpasswd:
  expire: false
  users:
    - name: ${DEV_USER}
      password: ${DEV_PASSWORD}
      type: text
users:
  - default
  - name: ${DEV_USER}
    groups: sudo,docker
    shell: /bin/bash
    sudo: ["ALL=(ALL) NOPASSWD:ALL"]
    lock_passwd: false
write_files:
  - path: /opt/quant-platform/requirements.txt.b64
    permissions: "0644"
    content: |
      ${req_b64}
  - path: /opt/quant-platform/bootstrap-research-env.sh
    permissions: "0755"
    content: |
      #!/usr/bin/env bash
      set -euxo pipefail
      export DEBIAN_FRONTEND=noninteractive
      mkdir -p /opt/quant-platform /data /results /logs
      cd /opt/quant-platform
      base64 -d requirements.txt.b64 > requirements.txt

      write_apt_sources() {
        if grep -qi ubuntu /etc/os-release; then
          codename="\$(. /etc/os-release && echo "\${VERSION_CODENAME:-jammy}")"
          {
            echo "deb https://mirrors.tuna.tsinghua.edu.cn/ubuntu/ \${codename} main restricted universe multiverse"
            echo "deb https://mirrors.tuna.tsinghua.edu.cn/ubuntu/ \${codename}-updates main restricted universe multiverse"
            echo "deb https://mirrors.tuna.tsinghua.edu.cn/ubuntu/ \${codename}-security main restricted universe multiverse"
          } > /etc/apt/sources.list
        elif grep -qi debian /etc/os-release; then
          codename="\$(. /etc/os-release && echo "\${VERSION_CODENAME:-bookworm}")"
          {
            echo "deb https://mirrors.tuna.tsinghua.edu.cn/debian \${codename} main contrib non-free non-free-firmware"
            echo "deb https://mirrors.tuna.tsinghua.edu.cn/debian \${codename}-updates main contrib non-free non-free-firmware"
          } > /etc/apt/sources.list
        fi
      }

      if command -v apt-get >/dev/null 2>&1; then
        rm -rf /etc/apt/sources.list.d/* || true
        write_apt_sources
        apt-get update
        apt-get install -y --no-install-recommends python3 python3-venv python3-pip python3-dev build-essential curl wget gnupg ca-certificates unzip git fuse libfuse2 ufw fscrypt keyutils
        if [[ "${INSTALL_DOCKER}" == "true" ]]; then
          apt-get install -y --no-install-recommends docker.io
          systemctl enable --now docker || true
        fi
      elif command -v dnf >/dev/null 2>&1; then
        dnf install -y python3 python3-pip python3-devel gcc gcc-c++ make curl wget git unzip fuse
        if [[ "${INSTALL_DOCKER}" == "true" ]]; then
          dnf install -y docker || dnf install -y moby-engine || true
          systemctl enable --now docker || true
        fi
      else
        echo "Unsupported image: apt-get/dnf not found" >&2
        exit 1
      fi

      python3 -m venv /opt/quant-platform/venv
      . /opt/quant-platform/venv/bin/activate
      pip config set global.index-url https://pypi.tuna.tsinghua.edu.cn/simple
      pip config set global.trusted-host pypi.tuna.tsinghua.edu.cn
      pip install --upgrade pip setuptools wheel
      pip install --no-cache-dir -r requirements.txt
      pip install --no-cache-dir "pandas>=2.0,<3.0" aliyun-log-python-sdk "joblib>=1.3,<2" "cloudpickle>=2.2,<4" "scikit-learn>=1.3,<2" "paramiko>=3.0,<4"
      mkdir -p /opt/quant-platform/.duckdb
      export HOME=/opt/quant-platform
      python3 - <<'PY'
      import duckdb
      con = duckdb.connect()
      con.execute("SET home_directory='/opt/quant-platform/.duckdb'")
      con.execute("INSTALL httpfs")
      con.execute("LOAD httpfs")
      con.close()
      PY

      {
        echo 'export PYTHONPATH=/opt/quant-platform'
        echo 'export PYTHONUNBUFFERED=1'
        echo 'export OSS_DATA_BUCKET=quant-historical-data'
        echo 'export OSS_RESULT_BUCKET=stock-mdl-data-result'
        echo 'export OSS_ENDPOINT=oss-cn-hangzhou-internal.aliyuncs.com'
        echo 'export PATH=/opt/quant-platform/venv/bin:\$PATH'
      } >/etc/profile.d/quant-platform.sh

      chown -R ${DEV_USER}:${DEV_USER} /opt/quant-platform /data /results /logs
      if [[ -n "${DEV_PASSWORD}" ]]; then
        echo "${DEV_USER}:${DEV_PASSWORD}" | chpasswd
      fi

      if [[ "${INSTALL_DOCKER}" == "true" ]]; then
        usermod -aG docker ${DEV_USER} || true
        if [[ "${DOCKER_PULL_BACKTEST_IMAGE}" == "true" ]]; then
          docker pull "${BACKTEST_IMAGE}" || true
        fi
        if [[ "${CREATE_DEV_CONTAINER}" == "true" ]]; then
          docker rm -f quant-research-dev >/dev/null 2>&1 || true
          docker run -d --name quant-research-dev --restart unless-stopped \
            -v /opt/quant-platform/workspace:/workspace \
            -v /data:/data -v /results:/results -v /logs:/logs \
            "${BACKTEST_IMAGE}" sleep infinity || true
        fi
      fi

      /opt/quant-platform/venv/bin/python - <<'PY'
      import duckdb, numpy, pandas, pyarrow, sklearn
      print("quant research env ready")
      print("pandas", pandas.__version__)
      print("duckdb", duckdb.__version__)
      PY
      touch /opt/quant-platform/READY

      # ================================================================
      # 权限加固：隔离本机与 VPC 内其他量化研究机器
      # ================================================================

      # 1. 封锁所有 VPC 私网 IP 对本机的 SSH 访问
      #    交易员只能通过公网 IP 连接，VPC 内其他机器无法 SSH 进来
      echo "==> 安全加固：配置 UFW 防火墙..."
      ufw --force reset
      # 先封锁 VPC 私网 SSH（deny 规则优先评估）
      for cidr in 10.0.0.0/8 172.16.0.0/12 192.168.0.0/16; do
        ufw deny proto tcp from "\${cidr}" to any port 22
      done
      # 再放通公网 SSH
      ufw allow 22/tcp comment 'SSH from public internet'
      ufw allow 80/tcp comment 'HTTP'
      ufw allow 443/tcp comment 'HTTPS'
      ufw --force enable
      systemctl enable ufw || true
      echo "==> 安全加固：UFW 防火墙已启用（已封锁所有私网 SSH）"

      # 2. SSH 服务加固
      echo "==> 安全加固：SSH 配置..."
      if grep -q '^AllowUsers' /etc/ssh/sshd_config; then
        sed -i "s/^AllowUsers.*/AllowUsers ${DEV_USER}/" /etc/ssh/sshd_config
      else
        echo "AllowUsers ${DEV_USER}" >> /etc/ssh/sshd_config
      fi
      # 禁止 root SSH 登录（匹配 #PermitRootLogin 或有注释的情况）
      sed -i 's/^#*PermitRootLogin.*/PermitRootLogin no/' /etc/ssh/sshd_config
      # 禁止空密码
      sed -i 's/^#*PermitEmptyPasswords.*/PermitEmptyPasswords no/' /etc/ssh/sshd_config
      # 启用公钥认证（用户可在自己机器上配置免密登录）
      sed -i 's/^#*PubkeyAuthentication.*/PubkeyAuthentication yes/' /etc/ssh/sshd_config
      systemctl restart sshd || systemctl restart ssh || true
      echo "==> 安全加固：SSH 已配置（仅允许 ${DEV_USER}，禁止 root）"

      # 3. 限制 home / workspace 目录访问
      chmod 0700 /home/${DEV_USER}
      chmod 0700 /opt/quant-platform
      echo "==> 安全加固：/home/${DEV_USER} 和 /opt/quant-platform 已设为 0700"

      # ================================================================
      # 4. 目录加密准备：装好 fscrypt 并启用 ext4 encrypt，创建空保护目录
      #    安全约束：口令绝不在此处出现（不进 UserData）；
      #    实际加密由 create 流程经 SSH 通道下发口令完成。
      # ================================================================
      if [[ "${ENCRYPT_HOME_SUBDIR}" == "true" ]]; then
        echo "==> 目录加密：准备 fscrypt..."
        root_fs_type="\$(findmnt -no FSTYPE /)"
        fs_ok=no
        if [[ "\${root_fs_type}" == "ext4" || "\${root_fs_type}" == "f2fs" ]]; then
          root_dev="\$(findmnt -no SOURCE /)"
          if [[ "\${root_fs_type}" == "ext4" ]] && ! tune2fs -l "\${root_dev}" | grep -qi 'Filesystem features:.*encrypt'; then
            tune2fs -O encrypt "\${root_dev}" || echo "WARN: tune2fs -O encrypt 失败" >&2
          fi
          # fscrypt setup（无参数）一次性完成：按本机 CPU 自动调参生成 /etc/fscrypt.conf，
          # 并在根 FS 建 /.fscrypt 元数据目录。唯一交互提示是「是否允许非 root 用户创建
          # 元数据 [y/N]」，用 yes 自动应答 y，这样 DEV_USER 才能跑 fscrypt encrypt。
          # 注意：不能预写 conf——fscrypt 0.1.x 解析缺 options 字段的 conf 会 nil panic。
          yes | fscrypt setup && fs_ok=yes || echo "WARN: fscrypt setup 失败（真实错误见上文）" >&2
        else
          echo "WARN: 根文件系统 \${root_fs_type} 不支持 fscrypt，跳过目录加密准备" >&2
        fi
        if [[ "\${fs_ok}" == "yes" ]]; then
          mkdir -p "${ENCRYPTED_DIR}"
          chown ${DEV_USER}:${DEV_USER} "${ENCRYPTED_DIR}"
          chmod 0700 "${ENCRYPTED_DIR}"
          echo "==> 目录加密：fscrypt 就绪，保护目录 ${ENCRYPTED_DIR}（等待 create 流程下发口令）"
        fi
      fi
runcmd:
  - [bash, /opt/quant-platform/bootstrap-research-env.sh]
EOF
}

encrypt_protected_dir() {
  [[ "${ENCRYPT_HOME_SUBDIR}" == "true" ]] || return 0
  [[ -n "${FSCRYPT_PASSPHRASE:-}" ]] || { echo "未设置加密口令，跳过自动加密（可稍后手动 SSH 运行 fscrypt encrypt）" >&2; return 0; }
  [[ -n "${PUBLIC_IP:-}" ]] || { echo "无公网 IP，跳过自动加密" >&2; unset FSCRYPT_PASSPHRASE; return 0; }

  if ! command -v expect >/dev/null 2>&1; then
    echo "本地未安装 expect，跳过自动加密。请手动 SSH 后运行：fscrypt encrypt '${ENCRYPTED_DIR}'（口令请妥善保管）" >&2
    unset FSCRYPT_PASSPHRASE; return 0
  fi
  if ! command -v sshpass >/dev/null 2>&1; then
    echo "本地未安装 sshpass，跳过自动加密。请手动 SSH 后运行：fscrypt encrypt '${ENCRYPTED_DIR}'" >&2
    unset FSCRYPT_PASSPHRASE; return 0
  fi

  echo "==> 等待机器完成环境初始化（含 fscrypt 就绪），之后加密保护目录..."
  local max_wait=900 start now elapsed ready=no
  start="$(date +%s)"
  while :; do
    now="$(date +%s)"; elapsed=$((now - start))
    if (( elapsed > max_wait )); then
      echo "等待 cloud-init 完成超时（${elapsed}s）。请稍后手动 SSH 运行：fscrypt encrypt '${ENCRYPTED_DIR}'" >&2
      unset FSCRYPT_PASSPHRASE; return 1
    fi
    if SSHPASS="${DEV_PASSWORD}" sshpass -e ssh \
        -o StrictHostKeyChecking=accept-new \
        -o PreferredAuthentications=password \
        -o PubkeyAuthentication=no \
        -o ConnectTimeout=8 \
        -o NumberOfPasswordPrompts=1 \
        "${DEV_USER}@${PUBLIC_IP}" \
        'test -f /opt/quant-platform/READY && command -v fscrypt >/dev/null 2>&1'; then
      ready=yes; break
    fi
    sleep 10
  done
  echo "==> 机器就绪（${elapsed}s），加密保护目录 ${ENCRYPTED_DIR}（口令经 SSH 通道下发，不落盘/UserData）..."

  # 确保保护目录存在：fscrypt encrypt 要求目标目录已存在且为空，否则报错。
  # cloud-init 已尝试创建，这里作为兜底（处理相对路径修正、自定义路径等情形）。
  if ! SSHPASS="${DEV_PASSWORD}" sshpass -e ssh \
      -o StrictHostKeyChecking=accept-new \
      -o PreferredAuthentications=password -o PubkeyAuthentication=no \
      -o ConnectTimeout=8 -o NumberOfPasswordPrompts=1 \
      "${DEV_USER}@${PUBLIC_IP}" \
      "mkdir -p '${ENCRYPTED_DIR}' && chmod 0700 '${ENCRYPTED_DIR}' && test -d '${ENCRYPTED_DIR}'"; then
    echo "无法创建保护目录 ${ENCRYPTED_DIR}（路径无权限或非法）" >&2
    unset FSCRYPT_PASSPHRASE; return 1
  fi

  # expect 在本地跑，spawn sshpass+ssh 连远端 fscrypt encrypt；
  # 口令通过环境变量 FSCRYPT_PP 传入 expect，再 send 到远端 fscrypt 的 pty（经 SSH 加密通道）。
  # 因此口令不进本地 ps（环境变量传递）、不落远端磁盘、不进 UserData。
  local enc_script rc
  enc_script="$(mktemp)"
  cat > "${enc_script}" <<'EXPECT'
#!/usr/bin/env expect -f
set timeout 300
set dir    [lindex $argv 0]
set pp     $env(FSCRYPT_PP)
log_user 1
spawn sshpass -e ssh -o StrictHostKeyChecking=accept-new -o PreferredAuthentications=password -o PubkeyAuthentication=no $env(SSH_USER)@$env(SSH_HOST) fscrypt encrypt $dir
expect {
  -re {source number for the new protector} { send "2\r"; exp_continue }
  -re {Enter a name for the new protector} { send "[file tail $dir]\r"; exp_continue }
  -re {Enter custom passphrase} { send "$pp\r"; exp_continue }
  -re {Confirm passphrase} { send "$pp\r"; exp_continue }
  -re {Should we destroy} { send "n\r"; exp_continue }
  -re {refusing to encrypt|cannot be encrypted|Error:|Failed} { exit 1 }
  -re {now encrypted|already encrypted|encrypted, unlocked} { }
  timeout { exit 2 }
  eof
}
catch wait result
exit [lindex $result 3]
EXPECT
  chmod 600 "${enc_script}"
  SSHPASS="${DEV_PASSWORD}" SSH_USER="${DEV_USER}" SSH_HOST="${PUBLIC_IP}" \
    FSCRYPT_PP="${FSCRYPT_PASSPHRASE}" expect "${enc_script}" "${ENCRYPTED_DIR}"
  rc=$?
  rm -f "${enc_script}"
  unset FSCRYPT_PASSPHRASE
  if (( rc == 0 )); then
    echo "==> 保护目录已加密：${ENCRYPTED_DIR}"
    echo "    重启/重登后该目录锁定，需 fscrypt unlock '${ENCRYPTED_DIR}' + 口令解锁"
    echo "    口令已从内存清除；请确认你本地已备份（丢失无法恢复）"
  else
    echo "==> 自动加密失败（rc=${rc}）。请手动 SSH 后运行：fscrypt encrypt '${ENCRYPTED_DIR}'" >&2
  fi
  return $rc
}

create_instance() {
  validate_create_config
  resolve_create_defaults

  if [[ -z "${DEV_PASSWORD}" && "${GENERATE_DEV_PASSWORD}" == "true" ]]; then
    DEV_PASSWORD="$(python3 -c 'import secrets,string; alphabet=string.ascii_letters+string.digits+"@#%+="; print("Qp-"+ "".join(secrets.choice(alphabet) for _ in range(18)))')"
  fi

  local mode="${BILLING_MODE}"
  if [[ "${1:-}" == "--spot" ]]; then
    mode="spot"
  elif [[ "${1:-}" == "--postpaid" ]]; then
    mode="postpaid"
  elif [[ -n "${1:-}" ]]; then
    die "unknown create option: $1"
  fi

  local user_data
  user_data="$(mktemp "${TMPDIR:-/tmp}/quant-research-cloud-init.XXXXXX")"
  make_cloud_init "${user_data}"
  local user_data_b64
  user_data_b64="$(base64 < "${user_data}" | tr -d '\n')"

  if [[ "${mode}" != "spot" && "${mode}" != "postpaid" ]]; then
    die "BILLING_MODE must be spot or postpaid"
  fi

  echo "正在创建 ECS 实例：计费方式=${mode}，地域=${REGION_ID}..."
  local vswitch_candidates security_group_candidates type_candidates run_json last_error candidate_vswitch candidate_sg candidate_type
  vswitch_candidates="${VSWITCH_ID}"
  while IFS= read -r candidate_vswitch; do
    [[ -n "${candidate_vswitch}" ]] || continue
    [[ " ${vswitch_candidates} " == *" ${candidate_vswitch} "* ]] || vswitch_candidates="${vswitch_candidates} ${candidate_vswitch}"
  done < <(list_vswitch_ids || true)
  security_group_candidates="${SECURITY_GROUP_ID}"
  while IFS= read -r candidate_sg; do
    [[ -n "${candidate_sg}" ]] || continue
    [[ " ${security_group_candidates} " == *" ${candidate_sg} "* ]] || security_group_candidates="${security_group_candidates} ${candidate_sg}"
  done < <(list_security_group_ids || true)
  type_candidates="${INSTANCE_TYPE} ${FALLBACK_INSTANCE_TYPES}"

  for candidate_type in ${type_candidates}; do
    for candidate_vswitch in ${vswitch_candidates}; do
      for candidate_sg in ${security_group_candidates}; do
      [[ -n "${candidate_vswitch}" && -n "${candidate_sg}" && -n "${candidate_type}" ]] || continue
      local args=(
        ecs RunInstances
        --RegionId "${REGION_ID}"
        --ImageId "${IMAGE_ID}"
        --InstanceType "${candidate_type}"
        --SecurityGroupId "${candidate_sg}"
        --VSwitchId "${candidate_vswitch}"
        --InstanceName "${INSTANCE_NAME}"
        --HostName "${HOST_NAME}"
        --Amount 1
        --SystemDisk.Category "${SYSTEM_DISK_CATEGORY}"
        --SystemDisk.Size "${SYSTEM_DISK_SIZE}"
        --InternetMaxBandwidthOut "${INTERNET_MAX_BANDWIDTH_OUT}"
        --InstanceChargeType PostPaid
        --PasswordInherit "${PASSWORD_INHERIT}"
        --UserData "${user_data_b64}"
      )

      [[ -n "${ZONE_ID}" && "${candidate_vswitch}" == "${VSWITCH_ID}" ]] && args+=(--ZoneId "${ZONE_ID}")
      [[ -n "${KEY_PAIR_NAME}" ]] && args+=(--KeyPairName "${KEY_PAIR_NAME}")
      [[ -n "${AUTO_RELEASE_TIME}" ]] && args+=(--AutoReleaseTime "${AUTO_RELEASE_TIME}")
      if [[ "${mode}" == "spot" ]]; then
        args+=(--SpotStrategy "${SPOT_STRATEGY}")
        [[ -n "${SPOT_PRICE_LIMIT}" ]] && args+=(--SpotPriceLimit "${SPOT_PRICE_LIMIT}")
      fi

      echo "正在尝试：规格=${candidate_type}，交换机=${candidate_vswitch}..." >&2
      if run_json="$(aliyun "${args[@]}" 2>&1)"; then
        VSWITCH_ID="${candidate_vswitch}"
        SECURITY_GROUP_ID="${candidate_sg}"
        INSTANCE_TYPE="${candidate_type}"
        break 3
      fi

      last_error="${run_json}"
      if [[ "${run_json}" == *"InvalidSystemDiskSize.LessThanImageSize"* ]]; then
        SYSTEM_DISK_SIZE=$((SYSTEM_DISK_SIZE + 200))
        echo "系统盘偏小，已调整为 ${SYSTEM_DISK_SIZE}G，稍后重试..." >&2
      elif [[ "${run_json}" == *"OperationDenied.NoStock"* || "${run_json}" == *"NoStock"* || "${run_json}" == *"out of stock"* ]]; then
        echo "当前规格/可用区暂无库存，继续尝试下一个选项..." >&2
      elif [[ "${run_json}" == *"InvalidOperation.ResourceManagedByCloudProduct"* || "${run_json}" == *"managed by serviceID"* ]]; then
        echo "安全组 ${candidate_sg} 是云产品托管安全组，不能用于 ECS，继续尝试下一个安全组..." >&2
      else
        die "${run_json}"
      fi
      done
    done
  done

  if [[ -z "${run_json:-}" || "${run_json}" != *"InstanceIdSets"* ]]; then
    if [[ "${mode}" == "spot" ]]; then
      echo "抢占式库存全部不可用，自动改用按量计费继续尝试..." >&2
      mode="postpaid"
      for candidate_type in ${type_candidates}; do
        for candidate_vswitch in ${vswitch_candidates}; do
          for candidate_sg in ${security_group_candidates}; do
          local args=(
            ecs RunInstances
            --RegionId "${REGION_ID}"
            --ImageId "${IMAGE_ID}"
            --InstanceType "${candidate_type}"
            --SecurityGroupId "${candidate_sg}"
            --VSwitchId "${candidate_vswitch}"
            --InstanceName "${INSTANCE_NAME}"
            --HostName "${HOST_NAME}"
            --Amount 1
            --SystemDisk.Category "${SYSTEM_DISK_CATEGORY}"
            --SystemDisk.Size "${SYSTEM_DISK_SIZE}"
            --InternetMaxBandwidthOut "${INTERNET_MAX_BANDWIDTH_OUT}"
            --InstanceChargeType PostPaid
            --PasswordInherit "${PASSWORD_INHERIT}"
            --UserData "${user_data_b64}"
          )
          [[ -n "${ZONE_ID}" && "${candidate_vswitch}" == "${VSWITCH_ID}" ]] && args+=(--ZoneId "${ZONE_ID}")
          [[ -n "${KEY_PAIR_NAME}" ]] && args+=(--KeyPairName "${KEY_PAIR_NAME}")
          [[ -n "${AUTO_RELEASE_TIME}" ]] && args+=(--AutoReleaseTime "${AUTO_RELEASE_TIME}")
          echo "正在尝试按量计费：规格=${candidate_type}，交换机=${candidate_vswitch}..." >&2
          if run_json="$(aliyun "${args[@]}" 2>&1)"; then
            VSWITCH_ID="${candidate_vswitch}"
            SECURITY_GROUP_ID="${candidate_sg}"
            INSTANCE_TYPE="${candidate_type}"
            break 3
          fi
          last_error="${run_json}"
          done
        done
      done
    fi
  fi
  [[ -n "${run_json:-}" && "${run_json}" == *"InstanceIdSets"* ]] || die "${last_error:-failed to create ECS instance}"
  INSTANCE_ID="$(printf '%s' "${run_json}" | json_get 'InstanceIdSets.InstanceIdSet[0]')"
  [[ -n "${INSTANCE_ID}" ]] || die "failed to parse InstanceId from aliyun response: ${run_json}"

  echo "实例 ID：${INSTANCE_ID}"
  echo "正在等待实例启动..."
  aliyun ecs DescribeInstanceStatus --RegionId "${REGION_ID}" --InstanceId.1 "${INSTANCE_ID}" --output cols=InstanceId,Status rows=InstanceStatuses.InstanceStatus >/dev/null || true

  local public_ip="" private_ip=""
  for _ in $(seq 1 60); do
    local desc
    desc="$(aliyun ecs DescribeInstances --RegionId "${REGION_ID}" --InstanceIds "[\"${INSTANCE_ID}\"]")"
    public_ip="$(printf '%s' "${desc}" | python3 -c 'import json,sys; d=json.load(sys.stdin)["Instances"]["Instance"][0]; ips=d.get("PublicIpAddress",{}).get("IpAddress",[]) or d.get("EipAddress",{}).get("IpAddress",""); print(ips[0] if isinstance(ips,list) and ips else ips)')"
    private_ip="$(printf '%s' "${desc}" | python3 -c 'import json,sys; d=json.load(sys.stdin)["Instances"]["Instance"][0]; ips=d.get("VpcAttributes",{}).get("PrivateIpAddress",{}).get("IpAddress",[]); print(ips[0] if ips else "")')"
    [[ -n "${public_ip}${private_ip}" ]] && break
    sleep 5
  done

  PUBLIC_IP="${public_ip}"
  PRIVATE_IP="${private_ip}"
  save_state
  save_config

  echo "实例状态已保存：${STATE_FILE}"
  echo "公网 IP：${PUBLIC_IP:-N/A}"
  echo "内网 IP：${PRIVATE_IP:-N/A}"
  [[ -n "${DEV_PASSWORD}" ]] && echo "登录账号：${DEV_USER} / ${DEV_PASSWORD}"
  echo "机器还在自动安装环境，可用下面命令查看进度："
  echo "  ssh ${DEV_USER}@${PUBLIC_IP:-<public-ip>} 'sudo tail -f /var/log/cloud-init-output.log'"
  rm -f "${user_data}"

  # 等待 cloud-init 完成，并加密保护目录（口令经 SSH 通道下发，不落盘/UserData）
  if [[ "${ENCRYPT_HOME_SUBDIR}" == "true" ]]; then
    encrypt_protected_dir || echo "（加密步骤未完成，不影响机器使用，可稍后手动加密）" >&2
  fi
}

release_instance() {
  ensure_aliyun_cli
  load_state
  [[ -n "${INSTANCE_ID:-}" ]] || die "INSTANCE_ID missing in ${STATE_FILE}"
  echo "正在释放实例：${INSTANCE_ID}，地域=${REGION_ID}..."
  aliyun ecs DeleteInstance --RegionId "${REGION_ID}" --InstanceId "${INSTANCE_ID}" --Force true
  rm -f "${STATE_FILE}"
  echo "已释放实例：${INSTANCE_ID}"
}

status_instance() {
  ensure_aliyun_cli
  load_state
  aliyun ecs DescribeInstances --RegionId "${REGION_ID}" --InstanceIds "[\"${INSTANCE_ID}\"]" --output cols=InstanceId,InstanceName,Status,InstanceType,PublicIpAddress,VpcAttributes rows=Instances.Instance
}

reset_password() {
  ensure_aliyun_cli
  load_state
  [[ -n "${INSTANCE_ID:-}" ]] || die "INSTANCE_ID missing in ${STATE_FILE}"
  [[ -n "${DEV_PASSWORD:-}" ]] || die "DEV_PASSWORD missing in ${STATE_FILE}"
  echo "正在通过阿里云云助手重置登录密码并开启 SSH 密码登录..."
  aliyun ecs RunCommand \
    --RegionId "${REGION_ID}" \
    --Type RunShellScript \
    --InstanceId.1 "${INSTANCE_ID}" \
    --CommandContent "$(printf 'id -u %s >/dev/null 2>&1 || useradd -m -s /bin/bash %s\nusermod -aG sudo,docker %s 2>/dev/null || true\necho %s:%s | chpasswd\nsed -i \"s/^#\\?PasswordAuthentication .*/PasswordAuthentication yes/\" /etc/ssh/sshd_config\nsed -i \"s/^#\\?PubkeyAuthentication .*/PubkeyAuthentication yes/\" /etc/ssh/sshd_config\nsystemctl restart ssh || systemctl restart sshd\n' "${DEV_USER}" "${DEV_USER}" "${DEV_USER}" "${DEV_USER}" "${DEV_PASSWORD}" | base64 | tr -d '\n')" \
    --ContentEncoding Base64
  echo "已提交密码重置命令。等 10-20 秒后再执行：scripts/aliyun_research_ecs.sh ssh"
  echo "登录账号：${DEV_USER} / ${DEV_PASSWORD}"
}

ssh_instance() {
  load_state
  [[ -n "${PUBLIC_IP:-}" ]] || die "PUBLIC_IP missing in ${STATE_FILE}"
  local ssh_opts=(
    -o StrictHostKeyChecking=accept-new
    -o PreferredAuthentications=password
    -o PubkeyAuthentication=no
    -o NumberOfPasswordPrompts=3
    -o ConnectTimeout=10
  )
  if [[ -n "${DEV_PASSWORD:-}" ]] && command -v sshpass >/dev/null 2>&1; then
    exec sshpass -p "${DEV_PASSWORD}" ssh "${ssh_opts[@]}" "${DEV_USER}@${PUBLIC_IP}"
  fi
  if [[ -n "${DEV_PASSWORD:-}" ]]; then
    echo "登录密码：${DEV_PASSWORD}"
  fi
  exec ssh "${ssh_opts[@]}" "${DEV_USER}@${PUBLIC_IP}"
}

init_config() {
  mkdir -p "${STATE_DIR}"
  if [[ -f "${CONFIG_FILE}" ]]; then
    die "config already exists: ${CONFIG_FILE}"
  fi
  cat > "${CONFIG_FILE}" <<'EOF'
# Optional one-time defaults for scripts/aliyun_research_ecs.sh.
# Leave IMAGE_ID/SECURITY_GROUP_ID/VSWITCH_ID empty to auto-select them.
REGION_ID=cn-hangzhou
ZONE_ID=
VPC_ID=vpc-bp1wkj2li6xquauq5ccc5
IMAGE_ID=
SECURITY_GROUP_ID=
VSWITCH_ID=
KEY_PAIR_NAME=

# Daily defaults.
BILLING_MODE=spot
INSTANCE_TYPE=ecs.c9i.8xlarge
DEV_USER=research
DEV_PASSWORD=
GENERATE_DEV_PASSWORD=true
SSH_PASSWORD_AUTH=true
AUTO_RELEASE_TIME=
SYSTEM_DISK_SIZE=200
ENCRYPT_HOME_SUBDIR=true
ENCRYPTED_DIR=/home/research/private
EOF
  chmod 600 "${CONFIG_FILE}"
  echo "已创建配置文件：${CONFIG_FILE}"
  echo "如需固定交换机、安全组、密钥对或密码，可编辑这个文件。"
}

main() {
  local cmd="${1:-}"
  shift || true
  case "${cmd}" in
    create) create_instance "${1:-}" ;;
    release|destroy|delete) release_instance ;;
    status) status_instance ;;
    ssh) ssh_instance ;;
    reset-password) reset_password ;;
    init-config) init_config ;;
    -h|--help|help|"") usage ;;
    *) die "unknown command: ${cmd}" ;;
  esac
}

main "$@"
