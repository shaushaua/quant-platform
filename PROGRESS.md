# 项目进度总结

> 生成时间：2026-03-19

---

## 一、项目概览

将原 `qlib_factor_platform` 单体项目拆分为两个独立仓库，实现控制面与计算面分离：

| 仓库 | 定位 | 语言 | 状态 |
|------|------|------|------|
| `backtest-operator` | K8s 控制面：CRD 调度、镜像构建、任务编排 | Go (kubebuilder) | **已上线** |
| `quant-platform` | 计算面：因子计算 Worker、回测 Aggregator | Python | **已上线** |

---

## 二、已完成工作

### 2.1 backtest-operator（Go）

**CRD & Controller**
- `BacktestTask` CRD 定义完成，支持字段：`strategyName`、`imageTag`、`startDate`/`endDate`、`initialCapital`、`instances`、`dataPath`
- Controller 状态机：`Pending → Building → Running → Aggregating → Done / Failed`
- Worker Job 按 `instances` 分片，注入 `START_DATE`/`END_DATE`/`SHARD_INDEX` 环境变量
- Aggregator Job 汇总各 shard 结果并写入 OSS

**关键 bug 修复**

| commit | 修复内容 |
|--------|----------|
| `6d5d1d2` | kaniko job 名称查找错误（handleBuilding 阶段）|
| `316ba34` | init container 改用内部 registry busybox（无公网环境）|
| `57862d6` | `Status().Update()` 路径修复，PhaseBuilding 状态正确写入 |
| `aaec207` | worker 直接执行 strategy.py，通过环境变量注入日期参数 |
| `e358b78` | Dockerfile ENTRYPOINT 改为 exec form |
| `6784913` | agg job 创建忽略 AlreadyExists，避免 reconcile 竞争误判 Failed |

**部署信息**
- Namespace: `quant`
- 镜像仓库: `172.24.99.176:5000`
- Deployment: `backtest-operator`（1 replica，Running）

### 2.2 quant-platform（Python）

**因子计算 Worker**
- `quant_platform` Python 包，包含因子引擎核心逻辑
- Worker 入口：`strategy.py`，从环境变量读取 `START_DATE`/`END_DATE`/`SHARD_INDEX`
- 基础镜像：内部 `quant-platform-base`（预装依赖，163 镜像源）
- 使用 `PYTHONPATH` 避免 hatchling editable install 问题

**回测 Aggregator**
- `aggregator.py` 汇总各 shard 指标（sharpe、max_drawdown、total_return）
- 结果写入 `oss://<result_bucket>/<task_name>/result.json`

**OSS 配置**

| 用途 | Bucket |
|------|--------|
| 行情数据读取 | `quant-mdl-data` |
| 回测结果写入 | `stock-mdl-data-result` |

凭证通过 K8s Secret `quant-secrets` 注入（`OSS_ACCESS_KEY_ID`、`OSS_ACCESS_KEY_SECRET`、`OSS_ENDPOINT`、`OSS_BUCKET_NAME`、`OSS_RESULT_BUCKET`）。

---

## 三、端到端验证

最新成功任务：`test-momentum-v2-1773857074`（2026-03-19）

```
Phase:   Done
Shards:  1/1
Worker:  Complete
Agg:     Complete
```

Aggregator 输出（写入 OSS）：

```
sharpe:        1.5
max_drawdown:  -8%
total_return:  15%
回测区间:       2025-01-06 ~ 2025-01-08
```

---

## 四、待完成工作

### 高优先级
- [ ] 回测数据覆盖更长时间区间（当前仅 2 天）
- [ ] 多 shard 并行回测验证（`instances > 1`）
- [ ] BacktestStrategy CRD 实现（自动触发 kaniko build + task 提交）

### 中优先级
- [ ] live-engine + collector 迁移到 quant-platform
- [ ] MemoryStore（Redis）集成
- [ ] 结果查询 API（读取 OSS result.json）

### 低优先级
- [ ] 回测结果可视化
- [ ] 多策略并发调度压测
- [ ] CI/CD 流水线（自动 build + deploy）

---

## 五、关键配置参考

### BacktestTask 示例

```yaml
apiVersion: quant.lianghua.io/v1
kind: BacktestTask
metadata:
  name: my-strategy-20250106
  namespace: quant
spec:
  strategyName: my-strategy
  imageTag: 172.24.99.176:5000/quant-platform/my-strategy:latest
  dataPath: /2025
  startDate: "2025-01-06"
  endDate: "2025-01-08"
  initialCapital: 10000000
  instances: 1
```

### quant-secrets 必要字段

```
OSS_ACCESS_KEY_ID
OSS_ACCESS_KEY_SECRET
OSS_ENDPOINT
OSS_BUCKET_NAME        # quant-mdl-data
OSS_RESULT_BUCKET      # stock-mdl-data-result
```
