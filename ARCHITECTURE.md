# 量化平台整体架构方案

> 生成时间：2026-03-19

---

## 一、核心约束

1. **通联行情 License** 绑定 MAC 地址，要求固定 ECS，不能换机器
2. **实盘内存共享**：行情接收进程与交易引擎必须在同一台机器上（`/dev/shm` 或 mmap）
3. **回测计算**：计算量大，需要弹性扩缩容，用完释放节省成本

---

## 二、整体架构

```
┌──────────────────────────────────────────────────────┐
│                   ACK 托管集群                        │
│                                                      │
│  ┌─────────────────────────┐                         │
│  │  节点池 A：live-trading  │                         │
│  │  固定 ECS（绑定 License）│                         │
│  │  固定私网 IP + MAC       │                         │
│  │                         │                         │
│  │  ├── collector Pod      │                         │
│  │  │   （通联行情接收）    │                         │
│  │  └── live-engine Pod    │                         │
│  │      （实盘引擎）        │                         │
│  │      共享内存通信        │                         │
│  └─────────────────────────┘                         │
│                                                      │
│  ┌─────────────────────────┐                         │
│  │  节点池 B：backtest      │                         │
│  │  弹性按量 ECS            │                         │
│  │  可缩到 0 节点           │                         │
│  │                         │                         │
│  │  ├── backtest-operator  │                         │
│  │  ├── Worker Job         │                         │
│  │  └── Aggregator Job     │                         │
│  └─────────────────────────┘                         │
│                                                      │
└──────────────────────────────────────────────────────┘

外部依赖（VPC 内网访问）：
├── OSS：历史数据 + 回测结果存储
├── 镜像仓库：172.24.99.176:5000（独立 ECS）
└── Redis/MemoryStore（可选）
```

---

## 三、节点池配置

### 节点池 A：live-trading（固定节点）

| 属性 | 值 |
|------|----|
| 节点数 | 1（固定，不扩缩） |
| 计费 | 包年包月（不能停机） |
| 标签 | `role=live-trading` |
| 用途 | 通联行情接收、实盘引擎 |

### 节点池 B：backtest（弹性节点）

| 属性 | 值 |
|------|----|
| 节点数 | 0 ~ N（按需扩缩） |
| 计费 | 按量计费（用完停机）|
| 标签 | `role=backtest` |
| 用途 | 回测 Worker、Operator |

---

## 四、Pod 调度策略

### 实盘 Pod 固定到 live-trading 节点

```yaml
affinity:
  nodeAffinity:
    requiredDuringSchedulingIgnoredDuringExecution:
      nodeSelectorTerms:
      - matchExpressions:
        - key: role
          operator: In
          values: [live-trading]
```

### 回测 Job 固定到 backtest 节点

```yaml
affinity:
  nodeAffinity:
    requiredDuringSchedulingIgnoredDuringExecution:
      nodeSelectorTerms:
      - matchExpressions:
        - key: role
          operator: In
          values: [backtest]
```

---

## 五、成本结构

| 组件 | 费用类型 | 说明 |
|------|---------|------|
| ACK 管控面（Pro）| ¥0.64/小时 | 上实盘后变固定成本 |
| live-trading ECS | 包年包月固定费 | 通联 License 绑定，不能停 |
| backtest ECS | 按量计费 | 不跑回测时停机，费用归零 |
| 镜像仓库 ECS | 按量计费（可停机） | 不构建镜像时停机 |
| OSS | 按存储量 + 流量 | 极低 |

---

## 六、阶段规划

### 测试阶段（当前）
- 用完删集群，节省管控面 ¥0.64/小时
- 用 `scripts/restore-cluster.sh` 随时重建，约 5-10 分钟恢复
- 镜像仓库 ECS 停机保留（云盘保留镜像）

### 上实盘后
- 固定 live-trading 节点接入集群，打 `role=live-trading` 标签
- backtest 节点池按需弹性，不用时缩到 0
- 管控面费用纳入运营成本（¥460/月）

---

## 七、各项目职责分工

| 项目 | 运行位置 | 职责 |
|------|---------|------|
| `backtest-operator` | backtest 节点 | CRD 调度、Job 编排、镜像构建 API |
| `quant-platform` worker | backtest 节点 | 因子计算、回测执行 |
| `quant-platform` aggregator | backtest 节点 | 结果聚合、写入 OSS |
| `quant-platform` collector | live-trading 节点 | 通联行情接收 |
| `quant-platform` live-engine | live-trading 节点 | 实盘引擎，共享内存 |
