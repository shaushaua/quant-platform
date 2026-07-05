# Dev 测试环境部署指南

## 架构

```
ACK 集群（同一集群）
├── namespace: quant (prod)
│   ├── deployment: combined-engine
│   ├── 节点池 A: quant.lianghua.io/combined-engine=true, taint: dedicated=quant:NoSchedule
│   ├── 资源: 88Gi / 32 CPU
│   └── 定时: 9:00 扩容 / 15:30 缩容
│
├── namespace: quant-dev (dev)
│   ├── deployment: combined-engine
│   ├── 节点池 B: quant.lianghua.io/dev=true, taint: dedicated=dev:NoSchedule
│   ├── 资源: 14Gi / 4 CPU
│   └── 手动: scale 1 测试 / scale 0 释放
```

## 配置文件

| 文件 | 用途 |
|------|------|
| `k8s/namespace-dev.yaml` | quant-dev namespace |
| `k8s/overlays/dev/kustomization.yaml` | dev overlay（资源、nodeSelector、taint） |
| `k8s/base/combined-engine.yaml` | 基础 deployment（prod + dev 共享） |

## 首次部署（只需一次）

```bash
# 1. 创建 namespace
kubectl apply -f k8s/namespace-dev.yaml

# 2. 复制 secrets（MDL_TOKEN / DATAYES_TOKEN / ORDER_GATEWAY_TOKEN 等）
kubectl get secret quant-secrets -n quant -o yaml \
  | sed 's/namespace: quant/namespace: quant-dev/' \
  | kubectl apply -f -

# 3. 部署 dev deployment（replicas=0，不自动启动）
kubectl apply -k k8s/overlays/dev/
```

## 日常测试流程

```bash
# 构建并推送最新镜像
make build push

# 拉起 dev pod（autoscaler 自动扩 dev 节点）
kubectl scale -n quant-dev deploy/combined-engine --replicas=1

# 看日志
kubectl logs -n quant-dev deploy/combined-engine -f

# 测完释放（dev 节点自动缩到 0）
kubectl scale -n quant-dev deploy/combined-engine --replicas=0
```

## Dev 节点池配置（ACK 控制台）

ACK 控制台 → 节点池 → 创建：

| 配置项 | 值 |
|--------|---|
| 名称 | dev-node-pool |
| 实例规格 | 按需（如 ecs.g7.xlarge 4C16G） |
| 最小节点数 | 0 |
| 最大节点数 | 1 |

**Label**（不是 taint）：

```
Key:   quant.lianghua.io/dev
Value: true
```

**Taint**（3 个独立字段，不要填反）：

```
Key:     dedicated
Value:   dev
Effect:  NoSchedule
```

> 注意：Taint 的 Key 是 `dedicated`，不是 `key`/`value`/`effect`。ACK 控制台的表单字段名和 Taint 的 key/value 是两回事。

## 隔离对比

| | prod (quant) | dev (quant-dev) |
|---|---|---|
| namespace | quant | quant-dev |
| 镜像 | same (:latest) | same (:latest) |
| 节点 | 节点池 A（大机器） | 节点池 B（小机器） |
| 资源 | 88Gi / 32 CPU | 14Gi / 4 CPU |
| 启停 | CronJob 自动 | 手动 scale |
| broker | 连实盘 | 连实盘（测试时注意下单） |
| secrets | 独立 | 从 prod 复制 |

## 注意事项

1. **dev 连的是同一个 broker**（`ORDER_GATEWAY_URL` 继承自 base）。测试代码如果会下单，设 `MINUTE_RUN_INFERENCE=false` 防止意外下单。
2. **改 dev 资源不影响 prod**：`kubectl set resources -n quant-dev deploy/combined-engine --limits=memory=6Gi`
3. **prod 资源不要手动改**：用 `kubectl apply -k k8s/overlays/prod/` 统一管理。
