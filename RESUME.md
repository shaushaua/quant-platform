# 集群恢复指南

> 记录停集群前的状态，下次恢复时按此文档操作

生成时间：2026-03-19

---

## 一、停集群前的确认清单

- [x] 代码全部推送到 Gitee
  - `backtest-operator`: gitee.com/zhangyang555/backtest-operator
  - `quant-platform`: gitee.com/zhangyang555/quant-platform
- [x] quant-secrets 已备份（见下方）
- [x] 镜像仓库 `172.24.99.176:5000` 是**独立 ECS**，不在 ACK 集群内，停集群不影响镜像
- [x] OSS 数据和回测结果持久化，不受集群影响

---

## 二、quant-secrets 备份

```yaml
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
```

> **注意**：以上是 base64 编码，不是加密。妥善保管，不要提交到公开仓库。

---

## 三、恢复步骤

### 3.1 创建新 ACK 集群（或复用已有集群）

推荐配置：
- 1-3 个按需节点（system-pool），用于运行 operator
- 回测时临时添加 Spot 节点

### 3.2 安装 CRD

```bash
cd backtest-operator
kubectl apply -f config/crd/bases/
```

### 3.3 创建 namespace 和 secret

```bash
kubectl create namespace quant

# 恢复 secret（将上方备份 YAML 保存为 quant-secrets.yaml）
kubectl apply -f quant-secrets.yaml
```

### 3.4 部署 backtest-operator

```bash
kubectl apply -f config/deploy/
# 或者直接 apply deployment yaml
```

关键 deployment 参数：
- Image: `172.24.99.176:5000/quant-platform/backtest-operator:latest`
- Namespace: `quant`

### 3.5 验证 operator 正常

```bash
kubectl get pods -n quant
kubectl logs -n quant deployment/backtest-operator
```

### 3.6 提交测试任务

```bash
TS=$(date +%s)
cat <<EOF | kubectl apply -f -
apiVersion: quant.lianghua.io/v1
kind: BacktestTask
metadata:
  name: test-momentum-v2-${TS}
  namespace: quant
  labels:
    strategy-name: test-momentum-v2
spec:
  dataPath: /2025
  endDate: "2025-01-08"
  imageTag: 172.24.99.176:5000/quant-platform/test-momentum-v2:latest
  initialCapital: 10000000
  instances: 1
  startDate: "2025-01-06"
  strategyName: test-momentum-v2
EOF
```

---

## 四、关键配置参考

| 配置项 | 值 |
|--------|----|
| 内部镜像仓库 | `172.24.99.176:5000` |
| OSS Endpoint | `oss-cn-hangzhou-internal.aliyuncs.com` |
| OSS 数据 Bucket | `quant-historical-data` |
| OSS 结果 Bucket | `stock-mdl-data-result` |
| CRD Group | `quant.lianghua.io/v1` |
| Worker 镜像 | `172.24.99.176:5000/quant-platform/test-momentum-v2:latest` |
| Base 镜像 | `172.24.99.176:5000/quant-platform/quant-platform-base:latest` |

---

## 五、下次继续的工作

- [ ] 多 shard 并行回测验证（`instances: 3` 以上）
- [ ] BacktestStrategy CRD（自动 kaniko build + 提交 task）
- [ ] 结果查询 API
- [ ] live-engine + collector 迁移
