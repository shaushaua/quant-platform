#!/bin/bash
# 集群一键重建脚本
# 使用前确保 kubectl 已配置好目标集群的 kubeconfig
#
# 用法:
#   ./scripts/restore-cluster.sh           # 全量重建
#   ./scripts/restore-cluster.sh --dry-run  # 只打印要执行的命令

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
K8S_DIR="$PROJECT_DIR/k8s"
OPERATOR_DIR="$PROJECT_DIR/../backtest-operator"
QUANT_NS="quant"

# 颜色
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

info()  { echo -e "${GREEN}[INFO]${NC} $1"; }
warn()  { echo -e "${YELLOW}[WARN]${NC} $1"; }
error() { echo -e "${RED}[ERROR]${NC} $1"; }

# dry-run 模式
KUBECTL="kubectl"
if [[ "${1:-}" == "--dry-run" ]]; then
  KUBECTL="echo kubectl"
  info "DRY-RUN 模式，只打印命令不执行"
fi

# ─── 1. 检查连接 ───────────────────────────────────────
info "[1/7] 检查 kubectl 连接..."
if ! kubectl cluster-info > /dev/null 2>&1; then
  error "kubectl 无法连接集群，请检查 kubeconfig"
  exit 1
fi
info "集群连接正常"

# ─── 2. Namespace ──────────────────────────────────────
info "[2/7] 创建 namespace..."
$KUBECTL create namespace "$QUANT_NS" --dry-run=client -o yaml | $KUBECTL apply -f -

# ─── 3. Secrets ────────────────────────────────────────
info "[3/7] 创建 quant-secrets..."
if $KUBECTL get secret quant-secrets -n "$QUANT_NS" > /dev/null 2>&1; then
  warn "quant-secrets 已存在，跳过（如需更新请先 delete）"
else
  $KUBECTL apply -f "$K8S_DIR/base/secret-template.yaml"
  warn "已创建模板 secrets，请立即替换为真实值："
  warn "  kubectl edit secret quant-secrets -n $QUANT_NS"
fi

# ─── 4. 实盘组件（collector + live-engine） ─────────────
info "[4/7] 部署实盘组件（collector + live-engine）..."
$KUBECTL apply -f "$K8S_DIR/base/collector.yaml"
$KUBECTL apply -f "$K8S_DIR/base/live-engine.yaml"

# ─── 5. API 服务 ───────────────────────────────────────
info "[5/7] 部署 API 服务..."
# 临时将 LoadBalancer 替换为 ClusterIP（避免产生额外 SLB 费用）
if grep -q "type: LoadBalancer" "$K8S_DIR/api/deployment.yaml"; then
  warn "检测到 api-svc 仍为 LoadBalancer 类型"
  warn "建议将 k8s/api/deployment.yaml 中 type: LoadBalancer 改为 type: ClusterIP"
fi
$KUBECTL apply -f "$K8S_DIR/api/deployment.yaml"

# 确保 api-svc 使用 ClusterIP（不产生 SLB）
$KUBECTL patch svc api-svc -n "$QUANT_NS" -p '{"spec":{"type":"ClusterIP"}}' 2>/dev/null || true

# ─── 6. Ingress ────────────────────────────────────────
info "[6/7] 部署 Ingress..."
$KUBECTL apply -f "$K8S_DIR/base/ingress.yaml"

# ─── 7. Backtest Operator ──────────────────────────────
info "[7/7] 部署 backtest-operator..."
if [ -d "$OPERATOR_DIR" ]; then
  $KUBECTL apply -f "$K8S_DIR/rbac.yaml"
  $KUBECTL apply -f "$OPERATOR_DIR/config/crd/"
  $KUBECTL apply -f "$OPERATOR_DIR/config/rbac/"
  $KUBECTL apply -f "$OPERATOR_DIR/config/manager/"
else
  warn "backtest-operator 目录不存在: $OPERATOR_DIR"
  warn "跳过 operator 部署。如需安装："
  warn "  git clone <repo> $OPERATOR_DIR"
  warn "  然后重新运行本脚本"
fi

# ─── 可选：Monitoring ──────────────────────────────────
if [ -f "$K8S_DIR/monitoring/prometheus-servicemonitor.yaml" ]; then
  echo ""
  read -p "部署 Prometheus ServiceMonitor？(y/N) " -n 1 -r
  echo
  if [[ $REPLY =~ ^[Yy]$ ]]; then
    info "部署 ServiceMonitor..."
    $KUBECTL apply -f "$K8S_DIR/monitoring/prometheus-servicemonitor.yaml"
  fi
fi

# ─── 等待就绪 ──────────────────────────────────────────
echo ""
info "等待所有 Deployment 就绪..."
$KUBECTL rollout status deployment/collector -n "$QUANT_NS" --timeout=120s 2>/dev/null || true
$KUBECTL rollout status deployment/api -n "$QUANT_NS" --timeout=120s 2>/dev/null || true

echo ""
info "========================================="
info "集群重建完成！"
info "========================================="
echo ""
info "当前 Pod 状态："
kubectl get pods -n "$QUANT_NS"
echo ""
info "Service 状态："
kubectl get svc -n "$QUANT_NS"
echo ""
info "CronJob 状态："
kubectl get cronjob -n "$QUANT_NS"
echo ""
info "注意事项："
warn "  1. 确认 secrets 中的值已替换为真实值"
warn "  2. 确认 live-trading 节点已打标签: kubectl label node <name> role=live-trading"
warn "  3. 确认 system 节点已打标签: kubectl label node <name> node-role=system"
