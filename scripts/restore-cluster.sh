#!/bin/bash
# 集群恢复脚本 - 一键重建 backtest-operator 环境
# 使用前确保 kubectl 已配置好新集群的 kubeconfig

set -e

OPERATOR_DIR="$(cd "$(dirname "$0")/../.." && pwd)/backtest-operator"
QUANT_NS="quant"

echo "[1/5] 检查 kubectl 连接..."
kubectl cluster-info

echo "[2/5] 创建 namespace..."
kubectl create namespace "$QUANT_NS" --dry-run=client -o yaml | kubectl apply -f -

echo "[3/5] 创建 quant-secrets..."
kubectl apply -f - <<'EOF'
apiVersion: v1
kind: Secret
metadata:
  name: quant-secrets
  namespace: quant
type: Opaque
data:
  OSS_ACCESS_KEY_ID: TFRBSTV0TFZSUWRjc3NBVktSV3NxSkhL
  OSS_ACCESS_KEY_SECRET: YzNPUUl5VW9kVm5XQm9RcUJYWFowRmxjYkJjZGdM
  OSS_BUCKET_NAME: cXVhbnQtbWRsLWRhdGE=
  OSS_DATA_BUCKET: cXVhbnQtaGlzdG9yaWNhbC1kYXRh
  OSS_ENDPOINT: aHR0cHM6Ly9vc3MtY24taGFuZ3pob3UtaW50ZXJuYWwuYWxpeXVuY3MuY29t
  OSS_RESULT_BUCKET: c3RvY2stbWRsLWRhdGEtcmVzdWx0
EOF

echo "[4/5] 安装 CRD 并部署 operator..."
if [ -d "$OPERATOR_DIR" ]; then
  kubectl apply -f "$OPERATOR_DIR/config/crd/bases/"
  kubectl apply -f "$OPERATOR_DIR/config/deploy/"
else
  echo "backtest-operator 目录不存在: $OPERATOR_DIR"
  echo "请先 git clone gitee.com/zhangyang555/backtest-operator 到 $OPERATOR_DIR"
  exit 1
fi

echo "[5/5] 等待 operator 就绪..."
kubectl rollout status deployment/backtest-operator -n "$QUANT_NS" --timeout=120s

echo ""
echo "恢复完成！验证："
kubectl get pods -n "$QUANT_NS"
echo ""
echo "提交测试任务："
echo "  TS=\$(date +%s)"
echo "  kubectl apply -f - <<EOF"
echo "  apiVersion: quant.lianghua.io/v1"
echo "  kind: BacktestTask"
echo "  metadata:"
echo "    name: test-momentum-v2-\$TS"
echo "    namespace: quant"
echo "  spec:"
echo "    dataPath: /2025"
echo "    startDate: \"2025-01-06\""
echo "    endDate: \"2025-01-08\""
echo "    imageTag: 172.24.99.176:5000/quant-platform/test-momentum-v2:latest"
echo "    initialCapital: 10000000"
echo "    instances: 1"
echo "    strategyName: test-momentum-v2"
echo "  EOF"
